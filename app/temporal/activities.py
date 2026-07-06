import json
import logging
import os
import re
import urllib.request
from pathlib import Path
from typing import Dict, Any, List, Optional
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import insert_llm_log, get_submission_type_and_payload
from app.services.image_blur import anonymize_face

logger = logging.getLogger("analytics_service.temporal.activities")

BASE_DIR = Path(__file__).resolve().parents[2]
DOWNLOADS_DIR = BASE_DIR / "downloads"
OUTPUTS_DIR = BASE_DIR / "outputs"

def _download_file(url: str, filename: str) -> Path:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    local_path = DOWNLOADS_DIR / filename
    logger.info(f"Downloading {url} to {local_path}")
    
    with urllib.request.urlopen(url, timeout=60) as response:
        with open(local_path, "wb") as f:
            f.write(response.read())
    return local_path


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


async def _get_pii_and_abusive_prompt(conn) -> Dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT pv.id, pv.system_prompt, pv.user_prompt
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.analysis_type IN ('pii_and_abusive_language_detection', 'pii') AND pv.is_active = TRUE
        ORDER BY CASE WHEN p.analysis_type = 'pii_and_abusive_language_detection' THEN 1 ELSE 2 END, pv.created_at DESC
        LIMIT 1
        """
    )
    if not row:
        raise RuntimeError("No active pii_and_abusive_language_detection or pii prompt version found in the database.")
    return dict(row)


@activity.defn
async def pii_and_abusive_language_detection_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity to detect PII and abusive language, mask inputs, and update tables.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    target_columns = params["target_columns"]

    if not target_columns:
        return {"status": "skipped", "reason": "no columns specified"}

    async with db.pool.acquire() as conn:
        # Step 1. Update the status in submissions to processing
        from app.database.operations import update_submission_status
        await update_submission_status(conn, submission_id, tenant_code, "processing")

        try:
            # Get the submission type and payload
            sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)
            
            # Step 2. Get the prompt and columns, make the llm api call
            prompt_data = await _get_pii_and_abusive_prompt(conn)
            prompt_version_id = str(prompt_data["id"])
            system_prompt_tmpl = prompt_data["system_prompt"]
            user_prompt_tmpl = prompt_data["user_prompt"]

            # Replace {columns} in system prompt
            system_prompt = system_prompt_tmpl.replace("{columns}", json.dumps(target_columns))

            # Prepare the user prompt input (dictionary mapping column -> raw value)
            input_text_dict = {}
            for col in target_columns:
                db_col = map_column_to_db_col(col, sub_type)
                input_text_dict[col] = payload.get(db_col) or ""
            
            user_prompt = user_prompt_tmpl.replace("{{text}}", json.dumps(input_text_dict, ensure_ascii=False))

            # Make the LLM API call
            from app.services.llm import openrouter_chat_completion
            full_prompt = f"{system_prompt}\n\n{user_prompt}"
            
            response_text = openrouter_chat_completion(full_prompt)

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
                
                if isinstance(col_res, dict):
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
            prompt_tokens = len(full_prompt.split())
            completion_tokens = len(response_text.split())
            await insert_llm_log(
                conn,
                submission_id,
                tenant_code,
                settings.OPENROUTER_MODEL,
                "pii",
                prompt_version_id,
                prompt_tokens,
                completion_tokens,
                "success",
                meta_data=llm_response_dict
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
                # Retrieve prompt_version_id if not loaded
                if 'prompt_version_id' not in locals():
                    prompt_version_id = await _get_prompt_version_id(conn, "pii")
                
                prompt_tokens = len(full_prompt.split()) if 'full_prompt' in locals() else 0
                completion_tokens = len(response_text.split()) if 'response_text' in locals() else 0
                
                await insert_llm_log(
                    conn,
                    submission_id,
                    tenant_code,
                    settings.OPENROUTER_MODEL,
                    "pii",
                    prompt_version_id,
                    prompt_tokens,
                    completion_tokens,
                    "failed",
                    error_message=str(e)
                )
            except Exception as log_err:
                logger.error(f"Failed to log error to llm_logs: {log_err}")

            # Update the status in submissions to failed
            await update_submission_status(conn, submission_id, tenant_code, "failed")
            raise e


@activity.defn

async def deface_blur_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that downloads and runs local OpenCV/ONNX face blurring on ingestion images.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]

    async with db.pool.acquire() as conn:
        sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)
        
        image_urls = payload.get("image_urls")
        if not image_urls:
            return {"status": "skipped", "reason": "no image urls available"}

        blurred_local_paths = []
        for i, url in enumerate(image_urls):
            filename = f"{submission_id}_{tenant_code}_{i}.jpg"
            try:
                # 1. Download file locally
                local_path = _download_file(url, filename)
                
                # 2. Deface image
                OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
                output_path = OUTPUTS_DIR / f"blurred_{filename}"
                
                anonymize_face(
                    input_path=str(local_path),
                    output_path=str(output_path)
                )
                
                blurred_local_paths.append(str(output_path))
            except Exception as e:
                logger.error(f"Failed face blurring for {url}: {e}")
                raise

        # Save output paths back to DB
        # Note: In production this would upload to S3/GCS and save public URLs
        if blurred_local_paths:
            if sub_type == "story":
                await conn.execute(
                    "UPDATE story_submissions SET blur_image_urls = $3, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2",
                    submission_id, tenant_code, blurred_local_paths
                )
            else:
                await conn.execute(
                    "UPDATE discussion_submissions SET blur_image_urls = $3, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2",
                    submission_id, tenant_code, blurred_local_paths
                )

        return {"status": "success", "blur_paths": blurred_local_paths}


@activity.defn
async def update_status_activity(params: Dict[str, Any]) -> None:
    """
    Temporal activity to update the overall processing status of a submission in PostgreSQL.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    status = params["status"]
    process_status = params.get("process_status")

    async with db.pool.acquire() as conn:
        from app.database.operations import update_submission_status
        await update_submission_status(conn, submission_id, tenant_code, status, process_status)


@activity.defn
async def fetch_pending_submissions_activity() -> List[Dict[str, Any]]:
    """
    Retrieves all submissions currently in a 'pending' state and attaches their config-driven process steps.
    """
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT submission_id, tenant_code, submission_type FROM submissions WHERE status = 'pending'"
        )
        results = []
        for row in rows:
            sub_id = row["submission_id"]
            tenant = row["tenant_code"]
            sub_type = row["submission_type"]
            # Load process steps dynamically from settings based on type
            process_steps = settings.get_process_config(sub_type)
            results.append({
                "submission_id": sub_id,
                "tenant_code": tenant,
                "submission_type": sub_type,
                "process_steps": process_steps
            })
        return results



