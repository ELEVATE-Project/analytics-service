import asyncio
import json
import logging
from typing import Any, Dict

from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import (
    fetch_child_statements,
    fetch_statements_for_submission,
    insert_analysis_result,
    update_submission_status,
)

from app.services.classifier import load_setfit_model, predict_setfit_batch

logger = logging.getLogger("analytics_service.temporal.statement_category")


async def _get_statement_category_prompt(conn) -> Dict[str, Any]:
    row = await conn.fetchrow("""
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.name = 'Statement Category' AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC LIMIT 1
    """)
    if not row:
        raise RuntimeError("No active statement category prompt version found in the database.")
    return dict(row)


def _llm_classify(text: str, system_prompt: str, user_prompt: str):
    """Call the LLM to classify a single statement (blocking)."""
    from app.services.llm import openrouter_chat_completion

    u_prompt = user_prompt.replace("{{text}}", text)
    prompt = f"{system_prompt}\n\n{u_prompt}"
    response_text, usage = openrouter_chat_completion(prompt)

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
    thresholds = settings.SETFIT_CONFIDENCE_THRESHOLD

    logger.info(
        "Starting statement categorization for submission=%s tenant=%s (thresholds=%s)",
        submission_id, tenant_code, thresholds,
    )

    # 1. Fetch original statements (skip duplicates with parent_id set)
    async with db.pool.acquire() as conn:
        statements = await fetch_statements_for_submission(conn, submission_id, tenant_code)

    if not statements:
        logger.info("No statements found for submission=%s — skipping.", submission_id)
        return {"status": "skipped", "reason": "no statements found"}

    logger.info("Found %d statements to classify for submission=%s", len(statements), submission_id)

    # 1.5. Fetch the LLM prompt from database
    async with db.pool.acquire() as conn:
        prompt_data = await _get_statement_category_prompt(conn)
    system_prompt = prompt_data["system_prompt"]
    user_prompt = prompt_data["user_prompt"]

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

        category_key = model_pred.lower()
        current_threshold = thresholds.get(category_key, 0.80)

        llm_pred = None
        llm_conf = None
        justification = None

        if model_conf < current_threshold:
            # LLM fallback
            logger.info(
                "Statement [%s] confidence %.2f < threshold %.2f — calling LLM fallback.",
                stmt["id"], model_conf, current_threshold,
            )
            try:
                llm_result, usage = await asyncio.to_thread(_llm_classify, stmt["raw_statement"], system_prompt, user_prompt)
                llm_pred = llm_result.get("category")
                llm_conf = llm_result.get("confidence")
                justification = llm_result.get("justification")
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
            "current_threshold": current_threshold,
        })

    # 5. Bulk insert into analysis_results (parent statements only)
    child_copy_count = 0
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
                threshold=r.get("current_threshold", 0.80),
                justification=r["justification"],
            )

            # 5a. Propagate the same result to any duplicate (child) statements so
            #     every statement_id has its own analysis_results row.  Consumers
            #     never need to walk parent_id chains to read classification output.
            children = await fetch_child_statements(
                conn,
                parent_statement_id=r["statement_id"],
                submission_id=submission_id,
                tenant_code=tenant_code,
            )
            for child in children:
                await insert_analysis_result(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    statement_id=child["id"],
                    analysis_type="statement_category",
                    analysis_column=[child["statement_type"]],
                    ml_model_name=settings.SETFIT_MODEL_ID,
                    ml_model_version=settings.SETFIT_MODEL_VERSION,
                    model_confidence_score=r["model_conf"],
                    model_prediction=r["model_pred"],
                    llm_confidence_score=r["llm_conf"],
                    llm_prediction=r["llm_pred"],
                    threshold=r.get("current_threshold", 0.80),
                    justification=r["justification"],
                    meta_data={"deduped_from": str(r["statement_id"])},
                )
                child_copy_count += 1
                logger.debug(
                    "Copied statement_category result from parent [%s] to child [%s]",
                    r["statement_id"], child["id"],
                )

    model_only_count = sum(1 for r in results if not r["llm_pred"])
    llm_fallback_count = sum(1 for r in results if r["llm_pred"])

    logger.info(
        "Statement categorization complete for submission=%s: %d total, %d model-only, %d LLM-fallback, %d child copies",
        submission_id, len(results), model_only_count, llm_fallback_count, child_copy_count,
    )

    return {
        "status": "success",
        "total": len(results),
        "model_only": model_only_count,
        "llm_fallback": llm_fallback_count,
        "child_copies": child_copy_count,
    }
