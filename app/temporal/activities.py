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
            import urllib.parse
            import os
            parsed_path = urllib.parse.urlparse(url).path
            ext = os.path.splitext(parsed_path)[1]
            if not ext:
                ext = ".jpg"
            filename = f"{submission_id}_{tenant_code}_{i}{ext}"
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
                
                # 3. Upload to GCP
                from app.services.gcp_storage import upload_to_gcp
                from app.config import settings
                blob_name = f"{settings.GCP_BLOB_PREFIX}/{filename}"
                public_url = upload_to_gcp(str(output_path), blob_name)
                
                blurred_local_paths.append(public_url)
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



