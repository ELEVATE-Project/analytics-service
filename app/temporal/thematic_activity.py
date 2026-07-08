import json
import logging
import re
from typing import Dict, Any, List, Optional
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    insert_llm_log,
    insert_analysis_result,
    get_submission_type_and_payload,
)
from app.services.classifier import build_theme_embeddings, classify_statement

logger = logging.getLogger("analytics_service.temporal.activities")


def _is_garbage_or_spam(text: str) -> bool:
    """
    Detects spam/garbage text patterns beyond the word-count gate.
    Examples: "test test", "aaa aaa aaa", "123 456", repeated single tokens.
    """
    cleaned_text = text.strip()
    if not cleaned_text:
        return True

    # 1. No alphabetic characters at all (English or Indic scripts)
    if not re.search(r'[a-zA-Z\u0900-\u097F\u0980-\u09FF]', cleaned_text):
        return True

    # 2. Consecutive repeated characters: 4 or more (e.g., "aaaa", "....", "----")
    if re.search(r'(.)\1{3,}', cleaned_text):
        return True

    words = [w.strip().lower() for w in cleaned_text.split() if w.strip()]
    if not words:
        return True

    # 3. Common placeholder words/mashing (case-insensitive)
    placeholders = {
        "test", "testing", "demo", "dummy", "asdf", "ghjk", "qwerty", 
        "placeholder", "abc", "xyz", "nothing", "none", "nil", "n/a", "na"
    }
    # If the text matches a placeholder or all words are placeholders
    if (len(words) == 1 and words[0] in placeholders) or all(w in placeholders for w in words):
        return True

    # 4. Keyboard mashes (words of length >= 6 with zero vowels)
    vowels = set("aeiouy")
    for w in words:
        if len(w) >= 6 and re.match(r'^[a-z]+$', w):
            if not any(char in vowels for char in w):
                return True

    # 5. Repetitive text spam: if unique words make up less than 30% of total words (for statements with 3+ words)
    if len(words) >= 3:
        unique_ratio = len(set(words)) / len(words)
        if unique_ratio < 0.3:
            return True

    # 6. All words are the same
    if len(set(words)) == 1 and len(words) > 1:
        return True

    return False


async def _fetch_approved_themes(conn) -> list:
    """Fetches all approved themes from the themes table."""
    rows = await conn.fetch(
        "SELECT id, name, definitions, keywords FROM themes WHERE status ILIKE 'approved'"
    )
    return [dict(row) for row in rows]


async def _get_theme_classification_prompt(conn, analysis_type: str) -> dict:
    """
    Fetches the latest active theme_classification prompt version.
    Returns dict with 'id', 'system_prompt', 'user_prompt'.
    """
    row = await conn.fetchrow(
        """
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.analysis_type = $1 AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC
        LIMIT 1
        """,
        analysis_type
    )
    if not row:
        raise RuntimeError(f"No active {analysis_type} prompt version found in the database.")
    return dict(row)


def _build_themes_text(approved_themes: list) -> str:
    """
    Formats approved themes into a text block for prompt substitution.
    Each theme includes its id, name, and definition.
    """
    lines = []
    for theme in approved_themes:
        theme_id = theme["id"]
        name = theme.get("name", "")
        definition = theme.get("definitions", "") or ""
        keywords = theme.get("keywords", "") or ""
        lines.append(
            f"Theme ID: {theme_id}\n"
            f"Theme Name: {name}\n"
            f"Definition: {definition}\n"
            f"Keywords: {keywords}\n"
        )
    return "\n---\n".join(lines)


async def _classify_single_statement(
    conn,
    statement: str,
    submission_id: str,
    tenant_code: str,
    statement_type: str,
    approved_themes: list,
    theme_vectors: dict,
    theme_id_to_info: dict,
    pii_masked_at: list = None,
    abusive_masked_at: list = None,
    analysis_type: str = "thematic_classification",
) -> Dict[str, Any]:
    """
    Runs the full classification pipeline (Steps 2-9) for a single statement.
    Writes one row to analysis_results and returns the result metadata.
    """
    diagnostics = {
        "word_count_check": {
            "passed": False,
            "word_count": 0,
            "threshold": settings.MINIMUM_THEME_WORD_COUNT,
        },
        "safety_check": {
            "passed": False,
            "is_flagged": False,
        },
        "local_embedding_compare": {
            "similarity_score": 0.0,
            "threshold": settings.SIMILARITY_SCORE_THRESHOLD,
            "passed": False,
        },
        "llm_fallback": {
            "executed": False,
            "confidence_score": None,
            "threshold": settings.LLM_CONFIDENCE_SCORE_THRESHOLD,
            "passed": False,
        }
    }
    result = {
        "statement": statement,
        "category_type": None,
        "theme_id": None,
        "similarity_score": None,
        "confidence_score": None,
        "diagnostics": diagnostics,
    }

    logger.info(f"\n[Thematic Pipeline] =================== Evaluating Statement: '{statement}' ===================")

    # --- Step 2: Word-count / garbage gate ---
    word_count = len(statement.strip().split())
    diagnostics["word_count_check"]["word_count"] = word_count
    logger.info(f"[Thematic Pipeline] Step 2: Checking word-count threshold (words={word_count}, minimum={settings.MINIMUM_THEME_WORD_COUNT})")
    if word_count < settings.MINIMUM_THEME_WORD_COUNT or _is_garbage_or_spam(statement):
        result["category_type"] = "Unknown/Unclear"
        await insert_analysis_result(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            theme_id=None,
            analysis_type="theme",
            statements=statement,
            statement_type=statement_type,
            category_type="Unknown/Unclear",
            meta_data=diagnostics,
        )
        logger.info(f"[Thematic Pipeline] -> FAILED word-count/garbage gate. Marked Unknown/Unclear.")
        return result

    diagnostics["word_count_check"]["passed"] = True
    logger.info(f"[Thematic Pipeline] -> PASSED word-count/garbage gate.")

    # --- Step 3: Safety check ---
    logger.info(f"[Thematic Pipeline] Step 3: Checking statement safety (Checking if column {statement_type} was flagged in PII/Abuse activity)")
    pii_cols = pii_masked_at or []
    abusive_cols = abusive_masked_at or []
    flagged = (statement_type in pii_cols) or (statement_type in abusive_cols)
    diagnostics["safety_check"]["is_flagged"] = flagged
    if flagged:
        result["category_type"] = "Flagged"
        await insert_analysis_result(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            theme_id=None,
            analysis_type="theme",
            statements=statement,
            statement_type=statement_type,
            category_type="Flagged",
            meta_data=diagnostics,
        )
        logger.info(f"[Thematic Pipeline] -> FAILED safety check. Column {statement_type} contains PII or abusive content.")
        return result

    diagnostics["safety_check"]["passed"] = True
    logger.info(f"[Thematic Pipeline] -> PASSED safety check.")

    # --- Step 5 (themes already fetched) ---
    # --- Step 6: Local embedding classification ---
    logger.info(f"[Thematic Pipeline] Step 6: Comparing against approved themes using local SentenceTransformer embeddings")
    best_theme_id, best_similarity = classify_statement(
        statement, theme_vectors, theme_id_to_info
    )
    result["similarity_score"] = best_similarity
    diagnostics["local_embedding_compare"]["similarity_score"] = best_similarity

    if best_similarity >= settings.SIMILARITY_SCORE_THRESHOLD and best_theme_id:
        diagnostics["local_embedding_compare"]["passed"] = True
        # Local match is strong enough — Standard
        result["category_type"] = "Standard"
        result["theme_id"] = best_theme_id
        await insert_analysis_result(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            theme_id=best_theme_id,
            analysis_type="theme",
            statements=statement,
            statement_type=statement_type,
            category_type="Standard",
            similarity_score=best_similarity,
            meta_data=diagnostics,
        )
        theme_name = theme_id_to_info.get(best_theme_id, {}).get("name", "?")
        logger.info(f"[Thematic Pipeline] -> SUCCESSFUL local embedding match: '{statement[:50]}...' → {theme_name} (sim={best_similarity:.3f} >= threshold={settings.SIMILARITY_SCORE_THRESHOLD:.3f})")
        return result

    logger.info(f"[Thematic Pipeline] -> LOCAL MATCH similarity {best_similarity:.3f} was below threshold={settings.SIMILARITY_SCORE_THRESHOLD:.3f}. Proceeding to fallback.")

    # --- Step 7-8: LLM fallback ---
    logger.info(f"[Thematic Pipeline] Steps 7-8: Executing LLM fallback via OpenRouter")
    diagnostics["llm_fallback"]["executed"] = True
    try:
        prompt_data = await _get_theme_classification_prompt(conn, analysis_type)
        prompt_version_id = str(prompt_data["id"])
        system_prompt = prompt_data["system_prompt"]
        user_prompt = prompt_data["user_prompt"]

        # Substitute placeholders
        themes_text = _build_themes_text(approved_themes)
        user_prompt = user_prompt.replace("{{approved_themes}}", themes_text)
        user_prompt = user_prompt.replace("{{statements}}", statement)
        user_prompt = user_prompt.replace("{{statement}}", statement)

        # Build combined prompt for the LLM call
        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        from app.services.llm import openrouter_chat_completion
        response_text = openrouter_chat_completion(full_prompt)

        # Clean markdown wrappers
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            if lines[0].startswith("```json") or lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()

        # Fix unquoted UUIDs in the JSON if the LLM outputted them without quotes
        cleaned = re.sub(
            r'"theme_id"\s*:\s*([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
            r'"theme_id": "\1"',
            cleaned
        )

        try:
            llm_result = json.loads(cleaned)
        except json.JSONDecodeError as jde:
            logger.error(f"JSON parsing failed for LLM response. Error: {jde}. Cleaned response: {cleaned!r}. Raw response: {response_text!r}")
            raise

        # Parse result — handle both single-object and classified_data array formats
        classified_items = []
        if "classified_data" in llm_result and isinstance(llm_result["classified_data"], list):
            classified_items = llm_result["classified_data"]
        else:
            classified_items = [llm_result]

        llm_confidence = None
        llm_theme_id = None
        llm_theme_name = None
        llm_justification = None

        if classified_items:
            best_item = max(classified_items, key=lambda x: float(x.get("confidence_score", 0)))
            llm_confidence = float(best_item.get("confidence_score", 0))
            llm_theme_name = best_item.get("theme_name")
            llm_justification = best_item.get("justification")
            llm_theme_id_raw = best_item.get("theme_id")

            # Resolve theme_id — the LLM may return an integer id or a name
            if llm_theme_id_raw is not None:
                # Try to find by matching theme name in our approved set
                for tid, tinfo in theme_id_to_info.items():
                    if tinfo.get("name", "").lower().strip() == str(llm_theme_name or "").lower().strip():
                        llm_theme_id = tid
                        break

        result["confidence_score"] = llm_confidence
        diagnostics["llm_fallback"]["confidence_score"] = llm_confidence

        # Log LLM call
        await insert_llm_log(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            model_name=settings.OPENROUTER_MODEL,
            analysis_type=analysis_type,
            prompt_version_id=prompt_version_id,
            prompt_tokens=len(full_prompt.split()),
            completion_tokens=len(response_text.split()),
            status="success",
        )

        # --- Step 9: Finalize category_type ---
        if llm_confidence is not None and llm_confidence >= settings.LLM_CONFIDENCE_SCORE_THRESHOLD and llm_theme_id:
            diagnostics["llm_fallback"]["passed"] = True
            result["category_type"] = "Standard"
            result["theme_id"] = llm_theme_id
            await insert_analysis_result(
                conn,
                submission_id=submission_id,
                tenant_code=tenant_code,
                theme_id=llm_theme_id,
                analysis_type="theme",
                statements=statement,
                statement_type=statement_type,
                category_type="Standard",
                confidence_score=llm_confidence,
                similarity_score=best_similarity,
                justification=llm_justification,
                meta_data=diagnostics,
            )
            logger.info(f"LLM match: '{statement[:60]}...' → {llm_theme_name} (conf={llm_confidence:.2f})")
        else:
            # Others — vague, multi-theme, off-taxonomy, low confidence
            result["category_type"] = "Others"
            await insert_analysis_result(
                conn,
                submission_id=submission_id,
                tenant_code=tenant_code,
                theme_id=None,
                analysis_type="theme",
                statements=statement,
                statement_type=statement_type,
                category_type="Others",
                confidence_score=llm_confidence,
                similarity_score=best_similarity,
                justification=llm_justification,
                meta_data=diagnostics,
            )
            logger.info(f"Statement marked Others (low confidence): {statement[:80]}...")

    except Exception as e:
        logger.error(f"LLM fallback failed for statement: {e}")
        diagnostics["llm_fallback"]["error"] = str(e)
        # Fall through to Others on LLM failure
        result["category_type"] = "Others"
        await insert_analysis_result(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            theme_id=None,
            analysis_type="theme",
            statements=statement,
            statement_type=statement_type,
            category_type="Others",
            similarity_score=best_similarity,
            meta_data=diagnostics,
        )

    return result


@activity.defn
async def thematic_classification_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that performs category-type-gated thematic classification.

    Pipeline:
      Step 1:  Read column config, extract text
      Step 1b: Split discussion-type statements by delimiter
      Step 2:  Word-count / garbage gate → Unknown/Unclear
      Step 3:  Safety check (no LLM) → Flagged
      Step 4:  STOP point for Unknown/Flagged
      Step 5:  Fetch approved themes
      Step 6:  Local embedding classification
      Step 7:  Build LLM prompt (if similarity below threshold)
      Step 8:  Call LLM, parse confidence
      Step 9:  Finalize category_type (Standard / Others)
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    target_columns = params["target_columns"]
    analysis_type = params.get("analysis_type", "thematic_classification")

    if not target_columns:
        return {"status": "skipped", "reason": "no columns specified"}

    async with db.pool.acquire() as conn:
        sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)

        # --- Step 5: Fetch approved themes (once per activity invocation) ---
        warnings = []
        approved_themes = await _fetch_approved_themes(conn)
        if not approved_themes:
            warn_msg = "No approved themes found in database. All statements will go to LLM fallback."
            logger.warning(warn_msg)
            warnings.append(warn_msg)

        # Build embedding vectors for approved themes (once)
        theme_id_to_info = {str(t["id"]): t for t in approved_themes}
        theme_vectors = build_theme_embeddings(approved_themes) if approved_themes else {}

        pii_masked_at = payload.get("pii_masked_at") or []
        abusive_masked_at = payload.get("abusive_masked_at") or []

        # Clear existing analysis results for this submission's theme analysis
        await conn.execute(
            "DELETE FROM analysis_results WHERE submission_id = $1 AND tenant_code = $2 AND analysis_type = 'theme'",
            submission_id, tenant_code
        )

        all_results = []

        for col in target_columns:
            # Map column name to DB column
            db_col = "challenge" if col == "challenges" and sub_type == "story" else col
            raw_value = payload.get(db_col)
            if not raw_value:
                continue

            raw_text = str(raw_value).strip()
            if not raw_text:
                continue

            # --- Step 1b: Discussion splitting logic ---
            is_discussion = "discussion" in sub_type
            if is_discussion:
                # Split discussion-type cells by delimiter into separate statements
                delimiter = settings.THEMATIC_STATEMENT_DELIMITER
                statements = [s.strip() for s in raw_text.split(delimiter) if s.strip()]

                # TODO: Discussion-specific cleanup rules (Section 3a from plan)
                # - de-duplication of near-identical split fragments
                # - minimum fragment length re-check
                # - re-merging fragments that are clearly continuations
                # - tracking original unsplit statement in meta_data for audit
            else:
                # Story / single-statement types — process as one unit
                statements = [raw_text]

            for statement in statements:
                res = await _classify_single_statement(
                    conn=conn,
                    statement=statement,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    statement_type=col,
                    approved_themes=approved_themes,
                    theme_vectors=theme_vectors,
                    theme_id_to_info=theme_id_to_info,
                    pii_masked_at=pii_masked_at,
                    abusive_masked_at=abusive_masked_at,
                    analysis_type=analysis_type,
                )
                all_results.append(res)

        # Summary
        quality_counts = {}
        for r in all_results:
            q = r.get("category_type", "unknown")
            quality_counts[q] = quality_counts.get(q, 0) + 1

        return {
            "status": "success",
            "total_statements": len(all_results),
            "quality_breakdown": quality_counts,
            "warnings": warnings,
            "results": all_results,
        }
