import asyncio
import json
import logging
from typing import Any, Dict

from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    fetch_statements_for_submission,
    insert_analysis_result,
    update_submission_status,
)

from app.services.classifier import load_setfit_model, predict_setfit_batch

logger = logging.getLogger("analytics_service.temporal.statement_category")


async def _get_statement_category_prompt(conn, analysis_type: str = "statement_category") -> Dict[str, Any]:
    """Fetch the active statement_category prompt version from the database."""
    row = await conn.fetchrow(
        """
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.analysis_type = $1 AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC
        LIMIT 1
        """,
        analysis_type,
    )
    if not row:
        raise RuntimeError(f"No active {analysis_type} prompt version found in database.")
    return dict(row)


def _llm_classify(text: str, system_prompt: str, user_prompt: str):
    """Call the LLM to classify a single statement using prompts from database (blocking)."""
    from app.services.llm import openrouter_chat_completion

    formatted_user_prompt = user_prompt.replace("{{text}}", text)
    full_prompt = f"{system_prompt}\n\n{formatted_user_prompt}"

    response_text, usage = openrouter_chat_completion(full_prompt)

    # Clean markdown wrappers if present
    cleaned = response_text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines[0].startswith("```json") or lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM response: %s (raw: %s)", e, response_text[:200])
        result = {"category": "Other", "confidence": 0.0, "justification": "LLM parse error"}

    return result, usage


# -------------------------------------------------------------------------
# Temporal activity definition
# -------------------------------------------------------------------------
@activity.defn
async def statement_category_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity: classifies each statement for a submission using a
    SetFit model, falling back to LLM when confidence is below threshold.
    Results are stored in the analysis_results table.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    threshold = settings.SETFIT_CONFIDENCE_THRESHOLD

    logger.info(
        "Starting statement categorization for submission=%s tenant=%s (threshold=%.2f)",
        submission_id, tenant_code, threshold,
    )

    # 1. Fetch original statements and active prompt from DB
    async with db.pool.acquire() as conn:
        statements = await fetch_statements_for_submission(conn, submission_id, tenant_code)
        prompt_data = await _get_statement_category_prompt(conn)

    if not statements:
        logger.info("No statements found for submission=%s — skipping.", submission_id)
        return {"status": "skipped", "reason": "no statements found"}

    logger.info("Found %d statements to classify for submission=%s", len(statements), submission_id)

    # 2. Load the SetFit model (blocking — run in thread)
    model = await asyncio.to_thread(
        load_setfit_model,
        settings.SETFIT_MODEL_ID,
        settings.SETFIT_MODEL_VERSION,
    )

    # 3. Batch predict with SetFit (blocking — run in thread)
    texts = [s["raw_statement"] for s in statements]
    predictions, confidence_scores = await asyncio.to_thread(predict_setfit_batch, model, texts)

    # 4. Process each result — LLM fallback if below threshold
    results = []
    for i, stmt in enumerate(statements):
        model_pred = str(predictions[i])
        model_conf = float(confidence_scores[i])

        llm_pred = None
        llm_conf = None
        justification = None

        if model_conf < threshold:
            # LLM fallback
            logger.info(
                "Statement [%s] confidence %.2f < threshold %.2f — calling LLM fallback.",
                stmt["id"], model_conf, threshold,
            )
            try:
                llm_result, usage = await asyncio.to_thread(
                    _llm_classify,
                    stmt["raw_statement"],
                    prompt_data["system_prompt"],
                    prompt_data["user_prompt"],
                )

                if not isinstance(llm_result, dict):
                    raise ValueError(f"LLM response is not a dict: {type(llm_result)}")

                raw_cat = llm_result.get("category")
                raw_conf = llm_result.get("confidence")
                raw_just = llm_result.get("justification")

                if not isinstance(raw_cat, str) or not raw_cat.strip():
                    raise ValueError(f"Invalid LLM category: {raw_cat!r}")

                import math
                conf_float = float(raw_conf)
                if not math.isfinite(conf_float) or not (0.0 <= conf_float <= 1.0):
                    raise ValueError(f"LLM confidence score out of range [0.0, 1.0]: {raw_conf!r}")

                if raw_just is not None and not isinstance(raw_just, str):
                    raw_just = str(raw_just)

                llm_pred = raw_cat.strip()
                llm_conf = conf_float
                justification = raw_just
            except Exception as e:
                logger.error("LLM fallback failed for statement [%s]: %s", stmt["id"], e)
                llm_pred = "Other"
                llm_conf = 0.0
                justification = f"LLM error: {e}"


        final_category = llm_pred if llm_pred else model_pred

        results.append({
            "statement_id": stmt["id"],
            "statement_type": stmt["statement_type"],
            "model_pred": model_pred,
            "model_conf": model_conf,
            "llm_pred": llm_pred,
            "llm_conf": llm_conf,
            "justification": justification,
            "final_category": final_category,
        })

    # 5. Bulk insert into analysis_results
    async with db.pool.acquire() as conn:
        for r in results:
            await insert_analysis_result(
                conn,
                submission_id=submission_id,
                tenant_code=tenant_code,
                statement_id=r["statement_id"],
                analysis_type="statement_category",
                analysis_column=[r["statement_type"]],
                ml_model_name=settings.SETFIT_MODEL_ID,
                ml_model_version=settings.SETFIT_MODEL_VERSION,
                model_confidence_score=r["model_conf"],
                model_prediction=r["model_pred"],
                llm_confidence_score=r["llm_conf"],
                llm_prediction=r["llm_pred"],
                threshold=threshold,
                justification=r["justification"],
            )

    model_only_count = sum(1 for r in results if not r["llm_pred"])
    llm_fallback_count = sum(1 for r in results if r["llm_pred"])

    logger.info(
        "Statement categorization complete for submission=%s: %d total, %d model-only, %d LLM-fallback",
        submission_id, len(results), model_only_count, llm_fallback_count,
    )

    return {
        "status": "success",
        "total": len(results),
        "model_only": model_only_count,
        "llm_fallback": llm_fallback_count,
    }
