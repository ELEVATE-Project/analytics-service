import json
import logging
import os
import urllib.request
from pathlib import Path
from typing import Dict, Any, List
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import insert_llm_log, insert_analysis_result
from app.services.pii import mask_pii_text
from app.services.thematic import extract_thematic_analysis
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

async def _get_prompt_version_id(conn, analysis_type: str) -> str:
    """
    Fetch the active prompt template from the database.
    The seed script populates these rows at startup; the workflow uses them directly.
    """
    row = await conn.fetchrow(
        """
        SELECT pv.id
        FROM prompt_version pv
        JOIN prompts p ON p.id = pv.prompt_id
        WHERE p.analysis_type = $1 AND pv.is_active = TRUE
        ORDER BY pv.created_at DESC
        LIMIT 1
        """,
        analysis_type,
    )
    if not row:
        raise RuntimeError(f"No active {analysis_type} prompt version found in the database.")
    return str(row["id"])


async def _get_submission_type_and_payload(conn, submission_id: str, tenant_code: str) -> tuple:
    sub_row = await conn.fetchrow(
        "SELECT submission_type FROM submissions WHERE submission_id = $1 AND tenant_code = $2",
        submission_id, tenant_code
    )
    if not sub_row:
        raise ValueError(f"Submission {submission_id} not found in database.")
    
    sub_type = sub_row["submission_type"].lower().strip()
    
    if "story" in sub_type:
        payload_row = await conn.fetchrow(
            """
            SELECT title, objective, challenge, action_steps, impact, duration, blurb, content, image_urls 
            FROM story_submissions 
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code
        )
    elif "discussion" in sub_type:
        payload_row = await conn.fetchrow(
            """
            SELECT title, challenges, solutions, author, language, image_urls 
            FROM discussion_submissions 
            WHERE submission_id = $1 AND tenant_code = $2
            """,
            submission_id, tenant_code
        )
    else:
        raise ValueError(f"Unsupported submission type: {sub_type}")

    if not payload_row:
        raise ValueError(f"Payload details not found for {submission_id} under type {sub_type}.")

    return sub_type, dict(payload_row)


@activity.defn
async def pii_detection_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that masks PII for specified columns in database and updates state.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    target_columns = params["target_columns"]

    if not target_columns:
        return {"status": "skipped", "reason": "no columns to mask"}

    async with db.pool.acquire() as conn:
        sub_type, payload = await _get_submission_type_and_payload(conn, submission_id, tenant_code)
        prompt_version_id = await _get_prompt_version_id(conn, "pii")

        updated_fields = {}
        # Mock token metrics since OpenRouter free tier doesn't always return usage reliably
        prompt_tokens_estimated = 0
        completion_tokens_estimated = 0

        for col in target_columns:
            # Map challenges/solutions or specific DB column singular name
            db_col = "challenge" if col == "challenges" and sub_type == "story" else col
            
            raw_text = payload.get(db_col)
            if not raw_text:
                continue

            try:
                masked_text = mask_pii_text(raw_text)
                updated_fields[db_col] = masked_text
                
                # Update estimations
                prompt_tokens_estimated += len(raw_text.split())
                completion_tokens_estimated += len(masked_text.split())

            except Exception as e:
                logger.error(f"PII masking failed on column {col}: {e}")
                # Log failed attempt
                await insert_llm_log(
                    conn, submission_id, tenant_code, settings.OPENROUTER_MODEL,
                    "pii", prompt_version_id,
                    prompt_tokens_estimated, completion_tokens_estimated,
                    "failed", error_message=str(e)
                )
                raise

        # Save updates to DB
        if updated_fields:
            if sub_type == "story":
                set_clauses = ", ".join(f"{col} = ${i+3}" for i, col in enumerate(updated_fields.keys()))
                values = list(updated_fields.values())
                query = f"UPDATE story_submissions SET {set_clauses}, content_masked = TRUE, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2"
                await conn.execute(query, submission_id, tenant_code, *values)
            else:
                set_clauses = ", ".join(f"{col} = ${i+3}" for i, col in enumerate(updated_fields.keys()))
                values = list(updated_fields.values())
                query = f"UPDATE discussion_submissions SET {set_clauses}, content_masked = TRUE, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2"
                await conn.execute(query, submission_id, tenant_code, *values)

            await insert_llm_log(
                conn, submission_id, tenant_code, settings.OPENROUTER_MODEL,
                "pii", prompt_version_id,
                prompt_tokens_estimated, completion_tokens_estimated,
                "success"
            )

        return {"status": "success", "updated_columns": list(updated_fields.keys())}


@activity.defn
async def thematic_analysis_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that performs thematic analysis on textual columns, upserts findings to analysis_results.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]
    target_columns = params["target_columns"]

    if not target_columns:
        return {"status": "skipped", "reason": "no columns specified"}

    async with db.pool.acquire() as conn:
        sub_type, payload = await _get_submission_type_and_payload(conn, submission_id, tenant_code)

        content_parts = []
        for col in target_columns:
            db_col = "challenge" if col == "challenges" and sub_type == "story" else col
            val = payload.get(db_col)
            if val:
                content_parts.append(str(val))

        combined_text = "\n\n".join(content_parts)
        if not combined_text.strip():
            return {"status": "skipped", "reason": "content columns are empty"}

        # Perform thematic extraction
        try:
            theme_data = extract_thematic_analysis(combined_text)
        except Exception as e:
            logger.error(f"Thematic extraction failed: {e}")
            raise

        theme_name = theme_data["theme_name"]
        theme_def = theme_data["theme_definition"]
        keywords_list = theme_data["keywords"]
        confidence = theme_data["confidence_score"]

        # Ensure theme metadata exists in 'themes' table
        theme_id = await conn.fetchval(
            """
            INSERT INTO themes (name, definitions, keywords, status)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name) DO UPDATE SET 
                definitions = COALESCE(EXCLUDED.definitions, themes.definitions),
                keywords = COALESCE(EXCLUDED.keywords, themes.keywords)
            RETURNING id;
            """,
            theme_name,
            theme_def,
            ",".join(keywords_list),
            "Draft"
        )
        if not theme_id:
            theme_id = await conn.fetchval("SELECT id FROM themes WHERE name = $1", theme_name)

        # Clear existing analysis results for this submission
        await conn.execute(
            "DELETE FROM analysis_results WHERE submission_id = $1 AND tenant_code = $2 AND analysis_type = 'theme'",
            submission_id, tenant_code
        )

        # Save new analysis result
        await insert_analysis_result(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            theme_id=theme_id,
            analysis_type="theme",
            statements=combined_text[:500] + "...", # representative excerpt
            statement_type=",".join(target_columns),
            confidence_score=confidence,
            justification=theme_def
        )

        # Update LLM logs table using the DB-managed prompt template.
        prompt_version_id = await _get_prompt_version_id(conn, "theme")

        await insert_llm_log(
            conn,
            submission_id=submission_id,
            tenant_code=tenant_code,
            model_name=settings.OPENROUTER_MODEL,
            analysis_type="theme",
            prompt_version_id=prompt_version_id,
            prompt_tokens=len(combined_text.split()),
            completion_tokens=50,
            status="success"
        )

        return theme_data


@activity.defn
async def deface_blur_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that downloads and runs local OpenCV/ONNX face blurring on ingestion images.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]

    async with db.pool.acquire() as conn:
        sub_type, payload = await _get_submission_type_and_payload(conn, submission_id, tenant_code)
        
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



