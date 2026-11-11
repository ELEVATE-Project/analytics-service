import asyncio
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional
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
from app.database.operations import (
    fetch_retryable_submission_for_retry,
    fetch_retryable_submissions,
    claim_submission_for_retry,
    reconcile_completed_submission,
)
from app.temporal.client import get_temporal_client
from app.temporal.liveness import (
    check_submission_temporal_state,
    TemporalLivenessStatus,
)
from app.temporal.workflows import ConfigDrivenProcessingWorkflow
from temporalio.exceptions import WorkflowAlreadyStartedError

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
    Retry a single failed or stuck submission. Automatically resumes from the failed
    step using the persisted process status in the database.
    """
    async with db.pool.acquire() as conn:
        row = await fetch_retryable_submission_for_retry(
            conn, body.submission_id, body.session_id, body.tenant_code
        )

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Submission not found with submission_id='{body.submission_id}', session_id='{body.session_id}', and tenant_code='{body.tenant_code}'."
            )

        # Status check: Allow 'failed' and stuck 'processing' submissions to be retried
        if row["status"] not in ("failed", "processing"):
            raise HTTPException(
                status_code=400,
                detail=f"Submission status is '{row['status']}'. Only failed or stuck processing submissions can be retried."
            )

        # Parse process_status
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
        session_id = row["session_id"]

    # DB connection released — check Temporal status before dispatching

    if not process_steps:
        raise HTTPException(
            status_code=400,
            detail=f"No process steps configured for submission type '{sub_type}'. Cannot retry."
        )

    client = await get_temporal_client()

    # Liveness check: If row is currently in 'processing', verify whether any workflow is actually running
    if row["status"] == "processing":
        liveness, active_wf = await check_submission_temporal_state(client, submission_id, body.tenant_code)
        if liveness == TemporalLivenessStatus.RUNNING:
            raise HTTPException(
                status_code=409,
                detail=f"Submission '{submission_id}' is currently actively executing in Temporal (workflow: '{active_wf}')."
            )
        elif liveness == TemporalLivenessStatus.COMPLETED:
            async with db.pool.acquire() as conn:
                await reconcile_completed_submission(conn, submission_id, body.tenant_code)
            return StepRetryResponse(
                workflow_id=active_wf or f"step-retry-{submission_id}-{body.tenant_code}",
                submission_id=submission_id,
                session_id=session_id,
                message=f"Submission '{submission_id}' was already completed in Temporal; status reconciled to 'success'.",
            )

    # Atomically claim the retry attempt in PostgreSQL before dispatching
    async with db.pool.acquire() as conn:
        claim_info = await claim_submission_for_retry(conn, submission_id, body.tenant_code)
        process_status["retry_attempt"] = claim_info["retry_attempt"]

    # Stable workflow ID prevents concurrent duplicate retries for the same submission
    workflow_id = f"step-retry-{submission_id}-{body.tenant_code}"

    payload = {
        "submission_id": submission_id,
        "session_id": session_id,
        "tenant_code": body.tenant_code,
        "submission_type": sub_type,
        "process_steps": process_steps,
        "existing_steps": existing_steps,
        "process_status": process_status,
    }

    try:
        await client.start_workflow(
            ConfigDrivenProcessingWorkflow.run,
            payload,
            id=workflow_id,
            task_queue=settings.TEMPORAL_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"A retry workflow for submission '{submission_id}' is already running.",
        )

    return StepRetryResponse(
        workflow_id=workflow_id,
        submission_id=submission_id,
        session_id=session_id,
        message="Retry workflow started.",
    )


async def _dispatch_all_retry_workflows(
    tenant_code: Optional[str],
    cutoff: str,
    sweep_id: str,
) -> None:
    """
    Background worker: pages through ALL retryable submissions (failed, or stuck processing
    older than STUCK_PROCESSING_MIN_AGE_MINUTES) and fires an individual
    ConfigDrivenProcessingWorkflow for each one.
    Only submissions that failed BEFORE this sweep started are picked up,
    so failures that occur during the sweep are not re-fetched mid-run.
    """
    client = await get_temporal_client()
    total_success = 0
    total_reconciled = 0
    total_skipped = 0
    total_failed = 0
    page = 0
    cursor_updated_at = None
    cursor_submission_id = None
    sem = asyncio.Semaphore(10)

    while True:
        async with db.pool.acquire() as conn:
            rows = await fetch_retryable_submissions(
                conn,
                tenant_code,
                settings.BATCH_SIZE,
                failed_before=cutoff,
                cursor_updated_at=cursor_updated_at,
                cursor_submission_id=cursor_submission_id,
                stuck_processing_min_age_minutes=settings.STUCK_PROCESSING_MIN_AGE_MINUTES,
            )

        if not rows:
            break

        # Advance keyset cursor to the last row of the current page
        cursor_updated_at = rows[-1]["updated_at"]
        cursor_submission_id = rows[-1]["submission_id"]

        page += 1

        async def _process_row(row):
            sub_id = row["submission_id"]
            t_code = row["tenant_code"]
            status = row["status"]

            # If row is in 'processing', check Temporal liveness with bounded concurrency
            if status == "processing":
                try:
                    async with sem:
                        liveness, active_wf = await check_submission_temporal_state(client, sub_id, t_code)
                    if liveness == TemporalLivenessStatus.RUNNING:
                        logger.info(f"[{sweep_id}] Skipping submission '{sub_id}' — actively running in Temporal ({active_wf})")
                        return "skipped"
                    if liveness == TemporalLivenessStatus.COMPLETED:
                        logger.info(f"[{sweep_id}] Reconciling submission '{sub_id}' — already COMPLETED in Temporal ({active_wf})")
                        async with db.pool.acquire() as conn:
                            await reconcile_completed_submission(conn, sub_id, t_code)
                        return "reconciled"
                except Exception as e:
                    logger.warning(f"[{sweep_id}] Failed liveness check for '{sub_id}': {e}. Skipping row.")
                    return "failed"

            sub_type = row["submission_type"]
            process_steps = settings.get_process_config(sub_type)
            if not process_steps:
                logger.warning(f"[{sweep_id}] Skipping submission '{sub_id}' — no steps configured for type '{sub_type}'")
                return "skipped"

            process_status = row.get("process_status") or {}
            if isinstance(process_status, str):
                try:
                    process_status = json.loads(process_status)
                except Exception:
                    process_status = {}

            # Atomically claim attempt
            async with db.pool.acquire() as conn:
                claim_info = await claim_submission_for_retry(conn, sub_id, t_code)
                process_status["retry_attempt"] = claim_info["retry_attempt"]

            # Start workflow
            try:
                await client.start_workflow(
                    ConfigDrivenProcessingWorkflow.run,
                    {
                        "submission_id": sub_id,
                        "session_id": row.get("session_id"),
                        "tenant_code": t_code,
                        "submission_type": sub_type,
                        "process_steps": process_steps,
                        "existing_steps": (process_status or {}).get("steps", []),
                        "process_status": process_status,
                    },
                    id=f"step-retry-{sub_id}-{t_code}",
                    task_queue=settings.TEMPORAL_QUEUE,
                )
                return "started"
            except WorkflowAlreadyStartedError:
                logger.info(f"[{sweep_id}] Retry workflow already started for '{sub_id}'")
                return "skipped"
            except Exception as e:
                logger.warning(f"[{sweep_id}] Failed to dispatch retry workflow for '{sub_id}': {e}")
                return "failed"

        results = await asyncio.gather(*[_process_row(r) for r in rows], return_exceptions=True)
        page_success = sum(1 for r in results if r == "started")
        page_reconciled = sum(1 for r in results if r == "reconciled")
        page_skipped = sum(1 for r in results if r == "skipped")
        page_failed = sum(1 for r in results if r == "failed" or isinstance(r, Exception))

        total_success += page_success
        total_reconciled += page_reconciled
        total_skipped += page_skipped
        total_failed += page_failed

        logger.info(
            f"[{sweep_id}] Page {page}: processed {len(rows)} "
            f"({page_success} started, {page_reconciled} reconciled, {page_skipped} skipped, {page_failed} failed) — "
            f"running total {total_success} started."
        )

        if len(rows) < settings.BATCH_SIZE:
            # Last page — no more rows to fetch
            break

    logger.info(
        f"[{sweep_id}] Bulk retry sweep complete for tenant_code={tenant_code or 'all'}: "
        f"{total_success} started, {total_reconciled} reconciled, {total_skipped} skipped, {total_failed} failed across {page} page(s)."
    )



@uploads_router.post("/retry-step/all", response_model=BulkRetryResponse, tags=["Step Retry"])
async def retry_all_failed(
    body: BulkRetryRequest,
    background_tasks: BackgroundTasks,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Enqueue a background sweep that pages through ALL retryable failed submissions
    (BATCH_SIZE at a time) and fires an individual ConfigDrivenProcessingWorkflow
    for each one. Only individual step-retry-* workflows appear in Temporal UI.
    Returns immediately with a sweep_id you can use to correlate logs.

    Any holder of AUTH_TOKEN can trigger this endpoint including across all tenants
    (tenant_code=None). This is intentional — the endpoint is internal-only, protected
    by the shared AUTH_TOKEN.
    """
    cutoff = datetime.now(timezone.utc).isoformat()
    tenant_label = body.tenant_code if body.tenant_code is not None else "all"
    sweep_id = f"bulk-retry-sweep-{tenant_label}-{uuid.uuid4().hex[:8]}"

    background_tasks.add_task(
        _dispatch_all_retry_workflows,
        body.tenant_code,
        cutoff,
        sweep_id,
    )

    logger.info(
        f"Bulk retry sweep '{sweep_id}' enqueued for tenant_code={tenant_label} "
        f"(cutoff={cutoff}, batch_size={settings.BATCH_SIZE})."
    )

    return BulkRetryResponse(
        workflow_id=sweep_id,
        message=(
            f"Bulk retry sweep started (tenant_code={tenant_label}). "
            f"Individual step-retry-* workflows will appear in Temporal as they are dispatched. "
            f"Track progress in logs with sweep_id='{sweep_id}'."
        ),
    )
