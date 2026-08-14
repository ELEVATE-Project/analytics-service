import asyncio
import json
import logging
import csv
import io
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

async def _get_environment_prompt(conn, analysis_type: str) -> Dict[str, Any]:
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

def _parse_llm_table_output(response_text: str) -> Dict[str, Any]:
    # Look for a markdown table
    lines = response_text.strip().split('\n')
    table_lines = [line.strip() for line in lines if line.strip().startswith('|') and line.strip().endswith('|')]
    
    if not table_lines:
        raise ValueError("Could not find a valid markdown table in the LLM response.")
    
    # We expect headers, a separator line (e.g. |---|---|), and then data rows
    if len(table_lines) < 3:
        raise ValueError("Markdown table does not have enough rows (header, separator, data).")
    
    headers = [h.strip() for h in table_lines[0].split('|')[1:-1]]
    
    # Take the first data row (we only pass one row in)
    data_row_parts = table_lines[2].split('|')[1:-1]
    
    if len(headers) != len(data_row_parts):
        # Handle cases where pipes inside content break the split. 
        # For a single row, a robust approach is to look for the last 4 columns assuming the first ones might be merged
        # But this is a basic implementation
        logger.warning(f"Header length ({len(headers)}) does not match data length ({len(data_row_parts)})")
    
    row_data = {headers[i].strip(): data_row_parts[i].strip() if i < len(data_row_parts) else "" for i in range(len(headers))}
    return row_data

@activity.defn
async def environment_detection_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    analysis_type = params.get("analysis_type", "environment_detection")
    resolved_model = params.get("llm_model") or settings.OPENROUTER_MODEL
    resolved_max_tokens = params.get("max_tokens") or settings.LLM_MAX_TOKENS
    resolved_timeout = params.get("llm_timeout_seconds") or settings.LLM_TIMEOUT_SECONDS

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

            prompt_data = await _get_environment_prompt(conn, analysis_type)

        prompt_version_id = str(prompt_data["id"])
        system_prompt = prompt_data["system_prompt"]
        user_prompt_tmpl = prompt_data["user_prompt"]

        # Extract relevant fields
        action_steps_val = payload.get("action_steps") or payload.get("actionSteps")
        if isinstance(action_steps_val, list):
            action_steps = "\\n".join(action_steps_val)
        else:
            action_steps = str(action_steps_val or "")
            
        content = str(payload.get("content") or payload.get("objective") or "")

        # We construct a CSV format for the single row
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        # Headers the prompt expects: `id`, `action_steps`, and `content`
        writer.writerow(["id", "action_steps", "content"])
        writer.writerow([submission_id, action_steps, content])
        
        csv_data = output.getvalue()
        user_prompt = user_prompt_tmpl.replace("{{csv_data}}", csv_data).replace("{csv_data}", csv_data)

        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        # Call the LLM
        from app.services.llm import openrouter_chat_completion, split_llm_usage
        response_text, usage = await asyncio.to_thread(
            openrouter_chat_completion,
            full_prompt, model=resolved_model, max_tokens=resolved_max_tokens, timeout=resolved_timeout,
        )

        try:
            parsed_data = _parse_llm_table_output(response_text)
        except Exception as parse_err:
            logger.error(f"Failed to parse LLM table response: {parse_err}")
            raise parse_err

        new_env = parsed_data.get("new_environment_classification", "Unknown")
        rationale = parsed_data.get("rationale", "")
        keywords = parsed_data.get("keywords_considered", "")
        
        confidence_score = None

        meta_data = {
            "keywords_considered": keywords,
            "raw_llm_response": response_text
        }

        async with db.pool.acquire() as conn:
            # According to user request: store rationale in justification, keywords_considered in meta_data, statement_type = "action_steps"
            await insert_analysis_result(
                conn,
                submission_id=submission_id,
                tenant_code=tenant_code,
                theme_id=None,
                analysis_type=analysis_type,
                statements=f"action_steps:\n{action_steps}\n\ncontent:\n{content}",
                statement_type="action_steps",
                confidence_score=confidence_score,
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
