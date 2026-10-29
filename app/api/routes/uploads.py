import asyncio
import json
import uuid
import logging
from fastapi import APIRouter, Depends, Form, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app.api.deps import verify_auth_token
from app.api.validators.uploads import (
    validate_report_type,
    validate_extension,
    validate_file_bytes,
)
from app.api.models.uploads import UploadResponse
from app.api.models.submissions import (
    StepRetryRequest,
    StepRetryResponse,
    BulkRetryRequest,
    BulkRetryResponse,
)
from app.api.services import uploads as upload_service
from app.config import settings
from app.database.db import db
from app.database.operations import fetch_retryable_failed_submissions
from app.temporal.client import get_temporal_client
from app.temporal.workflows import ConfigDrivenProcessingWorkflow

logger = logging.getLogger("analytics_service.api.routes.uploads")

uploads_router = APIRouter(prefix="/v1")


# ==========================================
# CSV Pipeline Endpoints
# ==========================================

@uploads_router.post("/upload/", response_model=UploadResponse, tags=["CSV Pipeline"])
async def upload_report(
    background_tasks: BackgroundTasks,
    report_type: str = Form(...),
    program_name: str = Form(...),
    leader_category: str = Form(...),
    tenant_code: str = Form(default="mitra"),
    file: UploadFile = File(...),
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Upload a CSV report file.

    The file is stored in GCS and a tracking record is created with
    status='pending'. Processing (column validation, Kafka publishing)
    runs in the background.
    """
    # Pure request-shape checks
    report_type = validate_report_type(report_type)
    validate_extension(file.filename)

    file_bytes = await file.read(settings.MAX_CSV_UPLOAD_BYTES + 1)
    validate_file_bytes(file_bytes)

    # Delegate to domain service
    return await upload_service.handle_upload(
        report_type=report_type,
        program_name=program_name,
        leader_category=leader_category,
        tenant_code=tenant_code,
        file_name=file.filename,
        file_bytes=file_bytes,
        background_tasks=background_tasks,
    )


@uploads_router.post("/process/csv/{record_id}", tags=["CSV Pipeline"])
async def push_record(
    record_id: int,
    background_tasks: BackgroundTasks,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Manually trigger processing for a specific csv_upload record in pending status.
    """
    return await upload_service.handle_push(record_id, background_tasks)


# ==========================================
# Step Retry Endpoints
# ==========================================

@uploads_router.post("/retry-step", response_model=StepRetryResponse, tags=["Step Retry"])
async def retry_step(
    body: StepRetryRequest,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Retry a single failed submission from the failed step (or a specified step).
    """
    async with db.pool.acquire() as conn:
        # 1. Look up the submission (both submission_id and session_id mandatory)
        row = await conn.fetchrow(
            "SELECT submission_id, session_id, tenant_code, submission_type, status, process_status "
            "FROM submissions WHERE submission_id = $1 AND session_id = $2 AND tenant_code = $3",
            body.submission_id, body.session_id, body.tenant_code
        )

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Submission not found with submission_id='{body.submission_id}', session_id='{body.session_id}', and tenant_code='{body.tenant_code}'."
            )

        # 3. Status check
        if row["status"] != "failed":
            raise HTTPException(
                status_code=400,
                detail=f"Submission status is '{row['status']}'. Only failed submissions can be retried."
            )

        # 4. Parse process_status
        process_status = row.get("process_status") or {}
        if isinstance(process_status, str):
            try:
                process_status = json.loads(process_status)
            except Exception:
                process_status = {}

        sub_type = row["submission_type"]
        process_steps = settings.get_process_config(sub_type)
        existing_steps = (process_status or {}).get("steps", [])

        submission_id = row["submission_id"]
        workflow_id = f"step-retry-{submission_id}-{body.tenant_code}-{uuid.uuid4().hex[:8]}"

        payload = {
            "submission_id": submission_id,
            "session_id": row["session_id"],
            "tenant_code": body.tenant_code,
            "submission_type": sub_type,
            "process_steps": process_steps,
            "existing_steps": existing_steps,
        }

        # 5. Dispatch to Temporal using shared singleton client
        client = await get_temporal_client()
        await client.start_workflow(
            ConfigDrivenProcessingWorkflow.run,
            payload,
            id=workflow_id,
            task_queue=settings.TEMPORAL_QUEUE,
        )

        return StepRetryResponse(
            workflow_id=workflow_id,
            submission_id=submission_id,
            session_id=row["session_id"],
            message="Retry workflow started.",
        )


@uploads_router.post("/retry-step/all", response_model=BulkRetryResponse, tags=["Step Retry"])
async def retry_all_failed(
    body: BulkRetryRequest,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Start individual retry workflows for all retryable failed submissions directly.
    No parent bulk workflow is created in Temporal.
    """
    client = await get_temporal_client()

    async with db.pool.acquire() as conn:
        rows = await fetch_retryable_failed_submissions(conn, body.tenant_code, settings.BATCH_SIZE)

    if not rows:
        return BulkRetryResponse(
            workflow_id="none",
            message=f"No retryable failed submissions found (tenant_code={body.tenant_code or 'all'}).",
        )

    tasks = []
    started_count = 0
    for row in rows:
        process_status = row.get("process_status") or {}
        if isinstance(process_status, str):
            try:
                process_status = json.loads(process_status)
            except Exception:
                process_status = {}

        sub_type = row["submission_type"]
        process_steps = settings.get_process_config(sub_type)
        existing_steps = process_status.get("steps", [])
        workflow_id = f"step-retry-{row['submission_id']}-{row['tenant_code']}-{uuid.uuid4().hex[:8]}"

        tasks.append(
            client.start_workflow(
                ConfigDrivenProcessingWorkflow.run,
                {
                    "submission_id": row["submission_id"],
                    "session_id": row.get("session_id"),
                    "tenant_code": row["tenant_code"],
                    "submission_type": sub_type,
                    "process_steps": process_steps,
                    "existing_steps": existing_steps,
                },
                id=workflow_id,
                task_queue=settings.TEMPORAL_QUEUE,
            )
        )
        started_count += 1

    if tasks:
        await asyncio.gather(*tasks)

    logger.info(f"Triggered {started_count} direct retry workflows for tenant_code={body.tenant_code or 'all'}.")

    return BulkRetryResponse(
        workflow_id=f"dispatched-{started_count}",
        message=f"Dispatched {started_count} retry workflows directly (tenant_code={body.tenant_code or 'all'}).",
    )
