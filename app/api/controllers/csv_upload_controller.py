import logging
from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile, File
from fastapi.security import HTTPAuthorizationCredentials
from app.api.deps import verify_auth_token
from app.api.validators.csv_upload import (
    validate_report_type,
    validate_extension,
    validate_file_bytes,
)
from app.api.services import csv_upload_service
from app.api.exceptions import (
    InvalidReportType,
    InvalidFileType,
    FileTooLarge,
    EmptyFile,
    DuplicateFile,
    RecordNotFound,
    RecordAlreadyProcessing,
    RecordNotPending,
)
from app.api.schemas.csv_upload import UploadResponse
from app.config import settings

logger = logging.getLogger("analytics_service.api.controllers.csv_upload_controller")

csv_router = APIRouter(prefix="/v1", tags=["CSV Pipeline"])


@csv_router.post("/upload/", response_model=UploadResponse)
async def upload_report(
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
    status='pending' (validation passed) or status='on_hold' (validation failed).
    """
    # 1. Pure request-shape checks
    try:
        report_type = validate_report_type(report_type)
    except InvalidReportType as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    try:
        validate_extension(file.filename)
    except InvalidFileType as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Read file bytes up to limit
    file_bytes = await file.read(settings.MAX_CSV_UPLOAD_BYTES + 1)
    try:
        validate_file_bytes(file_bytes)
    except FileTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    except EmptyFile as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # 2. Delegate to service layer
    try:
        return await csv_upload_service.handle_upload(
            report_type=report_type,
            program_name=program_name,
            leader_category=leader_category,
            tenant_code=tenant_code,
            file_name=file.filename,
            file_bytes=file_bytes,
        )
    except DuplicateFile as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("CSV upload failed")
        raise HTTPException(status_code=500, detail="Internal server error") from exc


@csv_router.post("/process/csv/{record_id}")
async def push_record(
    record_id: int,
    _token: HTTPAuthorizationCredentials = Depends(verify_auth_token),
):
    """
    Manually trigger processing for a specific csv_upload record in pending status.
    """
    try:
        return await csv_upload_service.handle_push(record_id)
    except RecordNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RecordAlreadyProcessing as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RecordNotPending as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Failed to start CSV processing workflow")
        raise HTTPException(
            status_code=500, detail="Failed to start CSV processing workflow"
        ) from exc
