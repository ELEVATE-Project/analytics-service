import asyncio
import json
import logging
import os
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    get_submission_type_and_payload,
    insert_llm_log,
    insert_ranking_result,
)

logger = logging.getLogger("analytics_service.temporal.activities")

BASE_DIR = Path(__file__).resolve().parents[2]
DOWNLOADS_DIR = BASE_DIR / "downloads"

REQUIRED_RATING_FIELDS = [
    "document_language", "impact_and_outcome_score", "impact_justification",
    "issue_and_challenge_score", "issue_justification", "action_steps_score",
    "action_justification", "composite_score", "tier", "overall_summary",
]
SCORE_FIELDS = [
    "impact_and_outcome_score", "issue_and_challenge_score",
    "action_steps_score", "composite_score",
]


def _resolve_url(url: str) -> str:
    url_str = str(url).strip()
    if url_str.startswith("http://") or url_str.startswith("https://"):
        return url_str
    base_url = settings.MEDIA_BASE_URL
    if not base_url:
        raise ValueError("Relative PDF URL encountered but MEDIA_BASE_URL is not configured.")
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", url_str.lstrip("/"))


def _download_file(url: str, local_path: Path) -> None:
    logger.info(f"Downloading story PDF {url} to {local_path}")
    with urllib.request.urlopen(url, timeout=60) as response:
        with open(local_path, "wb") as f:
            f.write(response.read())


def _extract_text_from_pdf(local_path: Path) -> str:
    from pypdf import PdfReader
    reader = PdfReader(str(local_path))
    pages_text = [page.extract_text() or "" for page in reader.pages]
    return "\n".join(pages_text).strip()


def _truncate_text(text: str, max_chars: int = settings.MAX_PDF_TEXT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n[... content truncated ...]"


def _build_fallback_text(challenge: Optional[str], action_steps: Optional[str], impact: Optional[str]) -> str:
    parts = []
    if challenge and str(challenge).strip():
        parts.append(f"Challenges and Issues:\n{str(challenge).strip()}\n")
    if action_steps and str(action_steps).strip():
        parts.append(f"Action Steps Taken:\n{str(action_steps).strip()}\n")
    if impact and str(impact).strip():
        parts.append(f"Impact and Outcomes:\n{str(impact).strip()}\n")
    return "\n".join(parts)


def _fetch_story_content(
    pdf_url: Optional[str],
    challenge: Optional[str],
    action_steps: Optional[str],
    impact: Optional[str],
    submission_id: str,
    tenant_code: str,
    log_prefix: str,
) -> tuple:
    """
    Blocking helper (run via asyncio.to_thread): downloads and extracts PDF text,
    falling back to challenge/action_steps/impact fields if the PDF is missing,
    fails to download, or yields no extractable text.
    Returns (content, source, total_chars) where source is 'pdf' or 'fields' and
    total_chars is the length of the extracted/fallback text before truncation.
    """
    if pdf_url:
        local_path = None
        try:
            resolved_url = _resolve_url(pdf_url)
            parsed_path = urllib.parse.urlparse(resolved_url).path
            ext = os.path.splitext(parsed_path)[1] or ".pdf"
            DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
            local_path = DOWNLOADS_DIR / f"story_rating_{submission_id}_{tenant_code}{ext}"

            _download_file(resolved_url, local_path)
            pdf_text = _extract_text_from_pdf(local_path)

            if pdf_text.strip():
                logger.info(f"{log_prefix} Using PDF content for analysis ({len(pdf_text)} chars)")
                return _truncate_text(pdf_text), "pdf", len(pdf_text)

            logger.warning(f"{log_prefix} PDF extracted no text. Falling back to submission fields.")
        except Exception as e:
            logger.warning(f"{log_prefix} PDF download/extraction failed: {e}. Falling back to submission fields.")
        finally:
            if local_path and local_path.exists():
                try:
                    local_path.unlink()
                except Exception as clean_err:
                    logger.warning(f"{log_prefix} Failed to delete temp file {local_path}: {clean_err}")
    else:
        logger.info(f"{log_prefix} No PDF link provided. Using submission fields.")

    fallback_text = _build_fallback_text(challenge, action_steps, impact)
    return fallback_text, "fields", len(fallback_text)


async def _get_story_rating_prompt(conn) -> Dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.analysis_type = $1 AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC
        LIMIT 1
        """,
        "story_rating"
    )
    if not row:
        raise RuntimeError("No active story_rating prompt version found in the database.")
    return dict(row)


def _is_valid_score(score: Any) -> bool:
    try:
        return 0.0 <= float(score) <= 1.0
    except (TypeError, ValueError):
        return False


@activity.defn
async def story_rating_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that rates a story submission's PDF content (falling back to its
    challenge/action_steps/impact fields), producing a composite score, tier, and
    per-criterion justifications stored in the `ranking` table.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    log_prefix = f"[StoryRating:{submission_id}]"
    resolved_model = params.get("llm_model") or settings.OPENROUTER_MODEL
    resolved_max_tokens = params.get("max_tokens") or settings.LLM_MAX_TOKENS
    resolved_timeout = params.get("llm_timeout_seconds") or settings.LLM_TIMEOUT_SECONDS

    # --- Phase 1: read submission payload/type. Own connection scope — released
    # before the timeout-bound PDF download and LLM call below.
    async with db.pool.acquire() as conn:
        sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)

    if "story" not in sub_type:
        return {"status": "skipped", "reason": f"story_rating only applies to story submissions, got '{sub_type}'"}

    pdf_urls = payload.get("pdf_urls") or []
    pdf_url = next((u for u in pdf_urls if u and str(u).strip()), None)

    # PDF download/extraction — also timeout-bound external I/O (up to 60s), so no
    # DB connection is held during this either.
    content, source, total_pdf_text_chars = await asyncio.to_thread(
        _fetch_story_content,
        pdf_url,
        payload.get("challenge"),
        payload.get("action_steps"),
        payload.get("impact"),
        submission_id,
        tenant_code,
        log_prefix,
    )

    if not content.strip():
        logger.info(f"{log_prefix} No PDF content and no fallback fields available. Skipping story rating.")
        return {"status": "skipped", "reason": "no PDF content or fallback fields available"}

    # --- Phase 2: read the rating prompt. Own connection scope.
    async with db.pool.acquire() as conn:
        prompt_data = await _get_story_rating_prompt(conn)
    prompt_version_id = str(prompt_data["id"])
    system_prompt = prompt_data["system_prompt"]
    user_prompt = prompt_data["user_prompt"].replace("{{story_content}}", content)
    full_prompt = f"{system_prompt}\n\n{user_prompt}"

    response_text = ""
    usage = {}
    try:
        # --- Phase 3: call the LLM — no DB connection held during this timeout-bound call.
        from app.services.llm import openrouter_chat_completion, split_llm_usage
        response_text, usage = await asyncio.to_thread(
            openrouter_chat_completion, full_prompt, model=resolved_model, max_tokens=resolved_max_tokens, timeout=resolved_timeout,
        )

        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            if lines[0].startswith("```json") or lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()

        result = json.loads(cleaned)

        missing = [f for f in REQUIRED_RATING_FIELDS if f not in result]
        if missing:
            raise ValueError(f"LLM response missing required fields: {missing}")

        if not all(_is_valid_score(result[f]) for f in SCORE_FIELDS):
            raise ValueError("One or more scores are outside the valid 0.0-1.0 range.")

        criteria_data = {
            "document_language": result["document_language"],
            "impact_and_outcome_score": float(result["impact_and_outcome_score"]),
            "impact_justification": result["impact_justification"],
            "issue_and_challenge_score": float(result["issue_and_challenge_score"]),
            "issue_justification": result["issue_justification"],
            "action_steps_score": float(result["action_steps_score"]),
            "action_justification": result["action_justification"],
        }
        composite_score = float(result["composite_score"])
        tier = result["tier"]
        overall_summary = result["overall_summary"]

        # --- Phase 4: persist results. Fresh connection scope, acquired only now
        # that the LLM call has returned. The delete+insert+log sequence is wrapped
        # in one transaction so a crash mid-sequence can't leave the submission with
        # its old ranking deleted but no new one written (or a ranking with no
        # matching llm_logs entry) — it's all-or-nothing.
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                # Clear any previous rating for this submission before writing the new one
                await conn.execute(
                    "DELETE FROM ranking WHERE submission_id = $1 AND tenant_code = $2",
                    submission_id, tenant_code
                )
                await insert_ranking_result(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    criteria_data=criteria_data,
                    composite_score=composite_score,
                    tier=tier,
                    overall_summary=overall_summary,
                    meta_data={
                        "content_source": source,
                        "pdf_url": pdf_url,
                        "llm_response": result,
                        "total_pdf_text_chars": total_pdf_text_chars,
                        "max_pdf_text_chars": settings.MAX_PDF_TEXT_CHARS,
                    },
                )

                prompt_tokens, completion_tokens, usage_meta = split_llm_usage(usage)
                await insert_llm_log(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    model_name=resolved_model,
                    analysis_type="story_rating",
                    prompt_version_id=prompt_version_id,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    status="success",
                    meta_data=usage_meta or None,
                )

        logger.info(f"{log_prefix} Story rating success - Tier: {tier}, Composite Score: {composite_score} (source: {source})")
        return {"status": "success", "tier": tier, "composite_score": composite_score, "content_source": source}

    except Exception as e:
        logger.error(f"{log_prefix} Story rating failed: {e}")
        try:
            # Real usage is only available if the API call itself succeeded — fall
            # back to a word-count estimate only when we never got a response to
            # report actual billed tokens for.
            if usage:
                from app.services.llm import split_llm_usage
                prompt_tokens, completion_tokens, usage_meta = split_llm_usage(usage)
            else:
                prompt_tokens = len(full_prompt.split()) if full_prompt else 0
                completion_tokens = len(response_text.split()) if response_text else 0
                usage_meta = None

            async with db.pool.acquire() as conn:
                await insert_llm_log(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    model_name=resolved_model,
                    analysis_type="story_rating",
                    prompt_version_id=prompt_version_id,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    status="failed",
                    error_message=str(e),
                    meta_data=usage_meta,
                )
        except Exception as log_err:
            logger.error(f"{log_prefix} Failed to log error to llm_logs: {log_err}")
        raise
