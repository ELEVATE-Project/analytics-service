import logging
import os
import urllib.request
import urllib.parse
import asyncio
from pathlib import Path
from typing import Dict, Any, List
from temporalio import activity

from app.config import settings
from app.database.db import db
from app.database.operations import get_submission_type_and_payload
from app.services.image_blur import anonymize_face
from app.services.gcp_storage import upload_to_gcp

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


@activity.defn
async def deface_blur_activity(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Temporal activity that downloads and runs local OpenCV/ONNX face blurring on ingestion images,
    then uploads the result to GCP Storage.
    """
    submission_id = params["submission_id"]
    tenant_code = params["tenant_code"]

    async with db.pool.acquire() as conn:
        sub_type, payload = await get_submission_type_and_payload(conn, submission_id, tenant_code)
        
        image_urls = payload.get("image_urls")
        if not image_urls:
            return {"status": "skipped", "reason": "no image urls available"}

        blurred_local_paths = []
        relative_original_urls = []

        for i, url in enumerate(image_urls):
            # Parse URL to reconstruct if it is relative (which is common in batch-mode/retries)
            url_str = str(url).strip()
            if not (url_str.startswith("http://") or url_str.startswith("https://")):
                base_url = settings.MEDIA_BASE_URL
                if not base_url:
                    raise ValueError("Relative image URL encountered but MEDIA_BASE_URL is not configured.")
                resolved_url = urllib.parse.urljoin(base_url.rstrip("/") + "/", url_str.lstrip("/"))
                logger.info(f"Reconstructed absolute URL for download: {resolved_url} (from relative path: {url_str})")
            else:
                resolved_url = url_str

            parsed_path = urllib.parse.urlparse(resolved_url).path
            parts = [p for p in parsed_path.split("/") if p]
            if len(parts) >= 2:
                actual_name = f"{parts[-2]}/{parts[-1]}"
            else:
                actual_name = parts[-1] if parts else f"{submission_id}_{i}.jpg"
                
            relative_original_urls.append(parsed_path)

            ext = os.path.splitext(parsed_path)[1]
            if not ext:
                ext = ".jpg"
            filename = f"{submission_id}_{tenant_code}_{i}{ext}"

            local_path = DOWNLOADS_DIR / filename
            output_path = OUTPUTS_DIR / f"blurred_{filename}"

            try:
                # 1. Download file locally (non-blocking thread pool execution)
                await asyncio.to_thread(_download_file, resolved_url, filename)
                
                # 2. Deface/Blur image
                OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
                await asyncio.to_thread(
                    anonymize_face,
                    input_path=str(local_path),
                    output_path=str(output_path)
                )
                
                # 3. Upload to GCP Storage (non-blocking thread pool execution)
                if "story" in sub_type:
                    blob_prefix = settings.STORY_BLOB or "story_blurred_image"
                else:
                    blob_prefix = settings.DISCUSSION_BLOB or "dicussion_blurred_image"
                
                blob_name = f"{blob_prefix}/{actual_name}"
                
                public_url = await asyncio.to_thread(upload_to_gcp, str(output_path), blob_name)
                blurred_local_paths.append(public_url)

            except Exception as e:
                logger.error(f"Failed face blurring for {resolved_url}: {e}")
                raise
            finally:
                # Clean up local temporary files under all conditions (prevent disk leakage)
                if local_path.exists():
                    try:
                        local_path.unlink()
                    except Exception as clean_err:
                        logger.warning(f"Failed to delete temp file {local_path}: {clean_err}")
                if output_path.exists():
                    try:
                        output_path.unlink()
                    except Exception as clean_err:
                        logger.warning(f"Failed to delete temp file {output_path}: {clean_err}")

        # Save output paths back to DB
        if blurred_local_paths or relative_original_urls:
            if sub_type == "story":
                await conn.execute(
                    "UPDATE story_submissions SET blur_image_urls = $3, image_urls = $4, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2",
                    submission_id, tenant_code, blurred_local_paths, relative_original_urls
                )
            else:
                await conn.execute(
                    "UPDATE discussion_submissions SET blur_image_urls = $3, image_urls = $4, updated_at = now() WHERE submission_id = $1 AND tenant_code = $2",
                    submission_id, tenant_code, blurred_local_paths, relative_original_urls
                )

        return {"status": "success", "blur_paths": blurred_local_paths}
