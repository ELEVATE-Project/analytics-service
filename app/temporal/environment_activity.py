import asyncio
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
    update_submission_status
)

logger = logging.getLogger("analytics_service.temporal.activities")

ENVIRONMENT_PROMPT_NAME = "Environment Detection"


async def _get_environment_prompt(conn) -> Dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.name = $1 AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC
        LIMIT 1
        """,
        ENVIRONMENT_PROMPT_NAME
    )
    if not row:
        raise RuntimeError(f"No active {ENVIRONMENT_PROMPT_NAME} prompt version found in the database.")
    return dict(row)



@activity.defn
async def environment_detection_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    analysis_type = params.get("analysis_type", "environment_detection")
    resolved_model = params.get("llm_model") or settings.OPENROUTER_MODEL
    resolved_max_tokens = params.get("max_tokens") or settings.LLM_MAX_TOKENS
    resolved_timeout = params.get("llm_timeout_seconds") or settings.LLM_TIMEOUT_SECONDS
    config_columns = params.get("target_columns") or []

    if not config_columns:
        return {"status": "skipped", "reason": "no columns specified"}

    prompt_version_id = None
    full_prompt = ""
    response_text = ""
    usage = {}

    try:
        async with db.pool.acquire() as conn:
            sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)
            
            if "story" not in sub_type:
                logger.info(f"Skipping environment detection for non-story submission {submission_id}")
                return {"status": "skipped", "reason": "not a story submission"}

            # Extract relevant fields dynamically based on the configured columns.
            input_text_dict = {"id": submission_id}
            statements_parts = []

            for col in config_columns:
                val = payload.get(col)
                if isinstance(val, list):
                    val_str = "\n".join(str(v) for v in val if v)
                    db_str = "\n".join(str(v) for v in val if v)
                else:
                    val_str = str(val or "")
                    db_str = val_str

                input_text_dict[col] = val_str
                if db_str.strip():
                    statements_parts.append(f"{col}:\n{db_str}")

            if not statements_parts:
                return {"status": "skipped", "reason": "no source text available"}

            prompt_data = await _get_environment_prompt(conn)

        prompt_version_id = str(prompt_data["id"])
        system_prompt = prompt_data["system_prompt"]
        user_prompt_tmpl = prompt_data["user_prompt"]

        json_data = json.dumps(input_text_dict, ensure_ascii=False)
        statements_str = "\n\n".join(statements_parts)
        
        user_prompt = user_prompt_tmpl.replace("{{text}}",json_data)

        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        # Call the LLM
        from app.services.llm import openrouter_chat_completion, split_llm_usage

        # Enforce that the LLM timeout is strictly less than the Temporal activity deadline (leave 15s for DB writes/parsing)
        # This prevents orphaned threads blocking forever if Temporal times out the activity.
        info = activity.info()
        if info.start_to_close_timeout:
            max_safe_timeout = int(info.start_to_close_timeout.total_seconds()) - 15
            resolved_timeout = min(resolved_timeout, max_safe_timeout)

        response_text, usage = await asyncio.to_thread(
            openrouter_chat_completion,
            full_prompt, model=resolved_model, max_tokens=resolved_max_tokens, timeout=resolved_timeout,
        )

        # Clean/parse LLM response as JSON
        cleaned_response = response_text.strip()
        if cleaned_response.startswith("```"):
            lines = cleaned_response.splitlines()
            if lines[0].startswith("```json") or lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned_response = "\n".join(lines).strip()

        try:
            parsed_data = json.loads(cleaned_response)
        except Exception as parse_err:
            logger.error(f"Failed to parse LLM response JSON: {parse_err} (response length={len(response_text)})")
            raise parse_err

        if not isinstance(parsed_data, dict):
            raise ValueError("LLM response must be a JSON object")

        required_fields = ["environment_classification", "rationale", "keywords_considered", "confidence_score"]
        for field in required_fields:
            if field not in parsed_data:
                raise ValueError(f"Missing required field in LLM response: {field}")

        new_env = parsed_data.get("environment_classification")
        if not new_env or not isinstance(new_env, str):
            raise ValueError(f"Invalid or missing environment_classification: {new_env}")

        allowed_envs = {"Classroom", "School", "Community"}
        if new_env != "Requires Review":
            envs = [e.strip() for e in new_env.split(",")]
            for e in envs:
                if e not in allowed_envs:
                    raise ValueError(f"Invalid environment value '{e}'. Allowed values are: {', '.join(allowed_envs)} or 'Requires Review'.")

        rationale = str(parsed_data.get("rationale", ""))
        keywords = str(parsed_data.get("keywords_considered", ""))
        
        raw_score = parsed_data.get("confidence_score")
        try:
            confidence_score = float(raw_score)
            if not (0.0 <= confidence_score <= 1.0):
                raise ValueError("confidence_score must be a finite float between 0.0 and 1.0")
        except (TypeError, ValueError):
            raise ValueError(f"Invalid confidence_score: {raw_score}. Must be a float between 0.0 and 1.0.")

        meta_data = {
            "keywords_considered": keywords,
            "raw_llm_response": response_text
        }

        async with db.pool.acquire() as conn:
            async with conn.transaction():
                # Make idempotent: remove any existing results for this specific analysis type on this submission
                await conn.execute(
                    "DELETE FROM analysis_results WHERE submission_id = $1 AND tenant_code = $2 AND analysis_type = $3",
                    submission_id, tenant_code, analysis_type
                )
                
                await insert_analysis_result(
                    conn,
                    submission_id=submission_id,
                    tenant_code=tenant_code,
                    theme_id=None,
                    analysis_type=analysis_type,
                    statements=statements_str,
                    analysis_column=config_columns,
                    llm_confidence_score=confidence_score,
                    justification=rationale,
                    category_type=None,
                    improvement_environment=new_env,
                    meta_data=meta_data
                )

                prompt_tokens, completion_tokens, usage_meta = split_llm_usage(usage)
                await insert_llm_log(
                    conn,
                    submission_id,
                    tenant_code,
                    resolved_model,
                    analysis_type,
                    prompt_version_id,
                    prompt_tokens,
                    completion_tokens,
                    "success",
                    meta_data=usage_meta or None,
                )

        return {
            "status": "success",
            "improvement_environment": new_env
        }

    except Exception as e:
        logger.error(f"Environment detection failed: {e}")
        try:
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
                    submission_id,
                    tenant_code,
                    resolved_model,
                    analysis_type,
                    prompt_version_id,
                    prompt_tokens,
                    completion_tokens,
                    "failed",
                    error_message=str(e),
                    meta_data=usage_meta
                )
        except Exception as log_err:
            logger.error(f"Failed to log error to llm_logs: {log_err}")

        raise e
