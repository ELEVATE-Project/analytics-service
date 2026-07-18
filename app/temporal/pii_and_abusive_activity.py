import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import insert_llm_log, get_submission_type_and_payload

logger = logging.getLogger("analytics_service.temporal.activities")


def map_column_to_db_col(col: str, sub_type: str) -> str:
    col_lower = col.lower().strip()
    if col_lower == "challenges" and "story" in sub_type:
        return "challenge"
    if col_lower == "actionsteps":
        return "action_steps"
    return col


def _get_case_insensitive_key(d: dict, key: str) -> Any:
    if key in d:
        return d[key]
    key_lower = key.lower()
    for k, v in d.items():
        if k.lower() == key_lower:
            return v
    return None


def _parse_statement_list(raw_value: Any) -> Optional[List[str]]:
    """
    Returns raw_value if it's already a list — discussion columns like
    challenges/solutions are stored as TEXT[] (per operations.py's
    _normalize_statement_list), which asyncpg auto-decodes to a native Python list.
    Returns None for scalar columns (a story's objective/challenge are plain text).
    """
    return raw_value if isinstance(raw_value, list) else None


async def _get_pii_and_abusive_prompt(conn, analysis_type: str) -> Dict[str, Any]:
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


@activity.defn
async def pii_and_abusive_language_detection_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity to detect PII and abusive language, mask inputs, and update tables.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    target_columns = params["target_columns"]
    analysis_type = params.get("analysis_type", "pii_and_abusive_language_detection")
    resolved_model = params.get("llm_model") or settings.OPENROUTER_MODEL
    resolved_max_tokens = params.get("max_tokens") or settings.LLM_MAX_TOKENS
    resolved_timeout = params.get("llm_timeout_seconds") or settings.LLM_TIMEOUT_SECONDS

    if not target_columns:
        return {"status": "skipped", "reason": "no columns specified"}

    from app.database.operations import update_submission_status

    prompt_version_id = None
    full_prompt = ""
    response_text = ""
    usage = {}

    try:
        # --- Phase 1: read submission payload + prompt, mark processing. Own
        # connection scope — released before the timeout-bound LLM call below, so a
        # pool connection isn't held idle for the whole duration of that call.
        async with db.pool.acquire() as conn:
            # Step 1. Update the status in submissions to processing
            await update_submission_status(conn, submission_id, tenant_code, "processing")

            # Get the submission type and payload
            sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)

            # Step 2. Get the prompt
            prompt_data = await _get_pii_and_abusive_prompt(conn, analysis_type)

        prompt_version_id = str(prompt_data["id"])
        system_prompt_tmpl = prompt_data["system_prompt"]
        user_prompt_tmpl = prompt_data["user_prompt"]

        # Replace {columns} in system prompt
        system_prompt = system_prompt_tmpl.replace("{columns}", json.dumps(target_columns))

        # Prepare the user prompt input (dictionary mapping column -> raw value).
        # Discussion columns (challenges/solutions) are stored as TEXT[] and come
        # back from asyncpg as a native list — embed it directly so the model sees
        # a proper nested array (and is asked to return one masked entry per
        # statement, per the prompt's list-input contract) rather than a flattened
        # string.
        input_text_dict = {}
        column_statements: Dict[str, List[str]] = {}
        for col in target_columns:
            db_col = map_column_to_db_col(col, sub_type)
            raw_value = payload.get(db_col) or ""
            parsed_list = _parse_statement_list(raw_value)
            if parsed_list is not None:
                column_statements[col] = parsed_list
                input_text_dict[col] = parsed_list
            else:
                input_text_dict[col] = raw_value

        user_prompt = user_prompt_tmpl.replace("{{text}}", json.dumps(input_text_dict, ensure_ascii=False))

        # --- Phase 2: call the LLM — no DB connection held during this timeout-bound call.
        from app.services.llm import openrouter_chat_completion, split_llm_usage
        full_prompt = f"{system_prompt}\n\n{user_prompt}"

        response_text, usage = await asyncio.to_thread(
            openrouter_chat_completion,
            full_prompt, model=resolved_model, max_tokens=resolved_max_tokens, timeout=resolved_timeout,
        )

        # Clean/parse LLM response
        cleaned_response = response_text.strip()
        if cleaned_response.startswith("```"):
            lines = cleaned_response.splitlines()
            if lines[0].startswith("```json") or lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned_response = "\n".join(lines).strip()

        try:
            llm_response_dict = json.loads(cleaned_response)
        except Exception as parse_err:
            logger.error(f"Failed to parse LLM response JSON: {parse_err}\nRaw response:\n{response_text}\nCleaned response:\n{cleaned_response}")
            raise parse_err

        # Step 3 & 4. Store response in respective tables and update pii_masked, pii_masked_at, abusive_masked_at, meta_data
        updated_fields = {}
        pii_masked_at_list = []
        abusive_masked_at_list = []

        for col in target_columns:
            db_col = map_column_to_db_col(col, sub_type)
            col_res = _get_case_insensitive_key(llm_response_dict, col)

            if col in column_statements:
                # List-valued column — expect one masked entry per input statement.
                original_statements = column_statements[col]
                if not isinstance(col_res, list):
                    logger.warning(
                        f"Expected a list response for column {col} (has {len(original_statements)} "
                        f"statements) but got {type(col_res).__name__}; leaving column unmasked."
                    )
                    continue

                masked_by_index: Dict[int, str] = {}
                col_pii_found = False
                col_abusive = False
                for entry in col_res:
                    if not isinstance(entry, dict):
                        continue
                    idx = entry.get("statement_index")
                    if not isinstance(idx, int) or not (0 <= idx < len(original_statements)):
                        continue
                    masked_text = entry.get("masked_text")
                    masked_by_index[idx] = masked_text if masked_text is not None else original_statements[idx]
                    if entry.get("pii_found"):
                        col_pii_found = True
                    if entry.get("abusive_language"):
                        col_abusive = True

                if len(masked_by_index) != len(original_statements):
                    logger.warning(
                        f"PII masking for column {col} returned {len(masked_by_index)} of "
                        f"{len(original_statements)} statements; missing entries kept unmasked."
                    )

                # Never drop a statement: default any missing index back to its
                # original (unmasked) text so the array length — and thematic
                # classification's per-statement iteration downstream — stays intact.
                # Assigned as a plain list (not json.dumps'd) — db_col is TEXT[],
                # and asyncpg binds a native Python list directly, no encoding needed.
                masked_list = [masked_by_index.get(i, original_statements[i]) for i in range(len(original_statements))]
                updated_fields[db_col] = masked_list
                if col_pii_found:
                    pii_masked_at_list.append(col)
                if col_abusive:
                    abusive_masked_at_list.append(col)

            elif isinstance(col_res, dict):
                masked_text = col_res.get("masked_text")
                if masked_text is not None:
                    updated_fields[db_col] = masked_text

                pii_found = col_res.get("pii_found", [])
                abusive_language = col_res.get("abusive_language", False)
                if pii_found:
                    pii_masked_at_list.append(col)
                if abusive_language:
                    abusive_masked_at_list.append(col)
            else:
                logger.warning(f"Unexpected response structure for column {col}: {col_res}")

        # --- Phase 3: persist results. Fresh connection scope, acquired only now
        # that the LLM call has returned.
        async with db.pool.acquire() as conn:
            table_name = "story_submissions" if sub_type == "story" else "discussion_submissions"

            # Construct dynamic SET clauses for update query
            n = len(updated_fields)
            set_clauses = ["pii_masked = TRUE", f"pii_masked_at = ${n + 3}", f"abusive_masked_at = ${n + 4}", f"meta_data = ${n + 5}", "updated_at = now()"]
            for i, col in enumerate(updated_fields.keys()):
                set_clauses.append(f"{col} = ${i+3}")

            query = f"""
                UPDATE {table_name}
                SET {", ".join(set_clauses)}
                WHERE submission_id = $1 AND tenant_code = $2
            """

            values = list(updated_fields.values())
            await conn.execute(
                query,
                submission_id,
                tenant_code,
                *values,
                pii_masked_at_list,
                abusive_masked_at_list,
                json.dumps(llm_response_dict)
            )

            # Step 5. Update the llm_logs table
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

            # Step 6. Update the status in submissions to success
            await update_submission_status(conn, submission_id, tenant_code, "success")

        return {
            "status": "success",
            "updated_columns": list(updated_fields.keys()),
            "pii_masked_at": pii_masked_at_list,
            "abusive_masked_at": abusive_masked_at_list
        }

    except Exception as e:
        logger.error(f"PII and Abusive language detection failed: {e}")
        try:
            # Real usage is only available if the API call itself succeeded (e.g. a
            # later JSON-parse failure) — fall back to a word-count estimate only
            # when we never got a response to report actual billed tokens for.
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

                # Update the status in submissions to failed
                await update_submission_status(conn, submission_id, tenant_code, "failed")
        except Exception as log_err:
            logger.error(f"Failed to log error to llm_logs: {log_err}")

        raise e
