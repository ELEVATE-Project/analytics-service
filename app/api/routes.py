import io
import logging

import pandas as pd
from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile, File
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from temporalio.client import Client

from app.config import settings
from app.csv_pipeline import csv_upload_repo
from app.csv_pipeline.csv_upload_repo import check_duplicate_file
from app.csv_pipeline.validators import validate_columns
from app.storage.gcs import upload_csv
from app.temporal.workflows import ConfigDrivenProcessingWorkflow

logger = logging.getLogger("analytics_service.api.routes")

# ---------------------------------------------------------------------------
# Shared auth
# ---------------------------------------------------------------------------
security = HTTPBearer()


async def verify_auth_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verifies that the incoming Bearer token matches AUTH_TOKEN in settings."""
    if credentials.credentials != settings.AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid token")


# ---------------------------------------------------------------------------
# Submissions router  →  /api/submissions
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/api/submissions", tags=["Submissions"])


class ManualTriggerRequest(BaseModel):
    submission_id: str
    tenant_code: str
    submission_type: str


@router.post("/trigger")
async def trigger_submission_manually(request: ManualTriggerRequest):
    """
    Manually triggers real-time processing workflow for a submission.
    """
    logger.info(f"API: Received manual trigger request for submission {request.submission_id}")

    process_steps = settings.get_process_config(request.submission_type)
    if not process_steps:
        raise HTTPException(
            status_code=400,
            detail=f"No process configuration found for submission type: {request.submission_type}",
        )

    try:
        client = await Client.connect(settings.TEMPORAL_HOST)
        workflow_id = f"manual-{request.submission_id}-{request.tenant_code}"
        handle = await client.start_workflow(
            ConfigDrivenProcessingWorkflow.run,
            {
                "submission_id": request.submission_id,
                "tenant_code": request.tenant_code,
                "process_steps": process_steps,
            },
            id=workflow_id,
            task_queue=settings.TEMPORAL_QUEUE,
        )
        return {
            "status": "success",
            "message": "Workflow started successfully",
            "workflow_id": handle.id,
            "run_id": handle.first_execution_run_id,
        }
    except Exception as e:
        logger.error(f"API: Manual trigger failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {str(e)}")


# ---------------------------------------------------------------------------
# CSV upload router  →  /v1
# ---------------------------------------------------------------------------
csv_router = APIRouter(prefix="/v1", tags=["CSV Pipeline"])


@csv_router.post("/upload/")
async def upload_report(
    report_type: str = Form(...),
    program_name: str = Form(...),
    leader_category: str = Form(...),
    file: UploadFile = File(...),
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Upload a CSV report file.

    The file is stored in GCS and a tracking record is created with
    status='pending' (validation passed) or status='on_hold' (validation failed).
    """
    normalized_type = report_type.lower().strip()
    if normalized_type not in ("story", "discussion"):
        raise HTTPException(
            status_code=400,
            detail="Only 'story' or 'discussion' report types are accepted.",
        )

    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted")

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    file_size = len(file_bytes)
    file_name = file.filename

    # Duplicate check
    is_duplicate = await check_duplicate_file(
        program_name=program_name,
        leader_category=leader_category,
        report_type=normalized_type,
        file_name=file_name,
        file_size=file_size,
    )
    if is_duplicate:
        raise HTTPException(status_code=400, detail="FILE ALREADY EXISTS")

    # Validate columns
    is_valid = True
    errors = []
    try:
        df = pd.read_csv(io.BytesIO(file_bytes))
        is_valid, errors = validate_columns(df, normalized_type)
    except Exception as exc:
        is_valid = False
        errors = [f"Failed to parse CSV: {exc}"]

    # Upload to GCS
    try:
        cloud_storage_path = upload_csv(file_bytes, normalized_type, file_name)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"GCS Upload failed: {exc}. Please verify GCS settings.",
        )

    meta_data = {
        "original_filename": file_name,
        "program_name": program_name,
        "leader_category": leader_category,
        "report_type": normalized_type,
    }

    if not is_valid:
        meta_data["validation_errors"] = errors
        status = "on_hold"
    else:
        status = "pending"

    record_id = await csv_upload_repo.insert_upload_record(
        report_type=normalized_type,
        program_name=program_name,
        leader_category=leader_category,
        cloud_storage_path=cloud_storage_path,
        file_name=file_name,
        file_size=file_size,
        meta_data=meta_data,
    )

    logger.info(
        "CSV uploaded: id=%s, report_type=%s, status=%s, cloud_storage_path=%s",
        record_id, normalized_type, status, cloud_storage_path,
    )

    # Trigger Temporal workflow in real-time mode
    if status == "pending" and settings.PROCESSING_MODE.lower().strip() == "real-time":
        try:
            temporal_client = await Client.connect(settings.TEMPORAL_HOST)
            await temporal_client.start_workflow(
                "CsvProcessingWorkflow",
                record_id,
                id=f"csv-upload-{record_id}",
                task_queue=settings.TEMPORAL_QUEUE,
            )
            logger.info("Triggered real-time CsvProcessingWorkflow for upload ID %s", record_id)
        except Exception as e:
            logger.error("Failed to trigger real-time CsvProcessingWorkflow: %s", e)

    response = {
        "message": "Successfully uploaded to cloud",
        "id": record_id,
        "status": status,
        "cloud_storage_path": cloud_storage_path,
    }
    if not is_valid:
        response["errors"] = errors

    return response


@csv_router.post("/push/{record_id}")
async def push_record(record_id: int):
    """
    Manually trigger processing for a specific csv_upload record.
    """
    record = await csv_upload_repo.get_record(record_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Record not found")

    if record["status"] == "in_progress":
        raise HTTPException(status_code=409, detail="Record is already being processed")

    await csv_upload_repo.update_status(record_id, "in_progress")

    try:
        temporal_client = await Client.connect(settings.TEMPORAL_HOST)
        await temporal_client.start_workflow(
            "CsvProcessingWorkflow",
            record_id,
            id=f"csv-upload-{record_id}",
            task_queue=settings.TEMPORAL_QUEUE,
        )
        return {"status": "success", "message": "CSV processing workflow started"}
    except Exception as e:
        await csv_upload_repo.update_status(record_id, "on_hold", {"error": str(e)})
        raise HTTPException(
            status_code=500, detail=f"Failed to start CSV processing workflow: {e}"
        )


