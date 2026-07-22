import logging
from typing import List
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("analytics_service.api.exceptions")


# ---------------------------------------------------------------------------
# Domain Exceptions
# ---------------------------------------------------------------------------

class InvalidReportType(Exception):
    """Raised when report type is not story or discussion."""
    pass


class InvalidFileType(Exception):
    """Raised when file extension is not csv."""
    pass


class FileTooLarge(Exception):
    """Raised when upload size exceeds configuration settings."""
    pass


class EmptyFile(Exception):
    """Raised when the uploaded file contains zero bytes."""
    pass


class DuplicateFile(Exception):
    """Raised when a CSV file with the same details is already uploaded."""
    pass


class InvalidCsvColumns(Exception):
    """
    Raised when the uploaded CSV's header row doesn't match the expected
    schema (missing and/or unexpected columns). Raised before the file is
    uploaded to GCS or a csv_uploads row is created — a column mismatch must
    never leave cloud-storage or tracking-table clutter behind.
    """
    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__("CSV column mismatch — please correct the file and re-upload.")


class RecordNotFound(Exception):
    """Raised when the CSV record cannot be found in database."""
    pass


class RecordAlreadyProcessing(Exception):
    """Raised when the record is already in_progress."""
    pass


class RecordNotPending(Exception):
    """Raised when trying to process a record that is not in pending status."""
    pass


# ---------------------------------------------------------------------------
# FastAPI Exception Handler Registration
# ---------------------------------------------------------------------------

def register_exception_handlers(app: FastAPI) -> None:
    """
    Registers global exception handlers on the FastAPI application instance
    to translate domain exceptions cleanly into HTTP JSON responses.
    """

    @app.exception_handler(InvalidReportType)
    async def invalid_report_type_handler(request: Request, exc: InvalidReportType):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(InvalidFileType)
    async def invalid_file_type_handler(request: Request, exc: InvalidFileType):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(EmptyFile)
    async def empty_file_handler(request: Request, exc: EmptyFile):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(DuplicateFile)
    async def duplicate_file_handler(request: Request, exc: DuplicateFile):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(InvalidCsvColumns)
    async def invalid_csv_columns_handler(request: Request, exc: InvalidCsvColumns):
        return JSONResponse(status_code=400, content={"detail": str(exc), "errors": exc.errors})

    @app.exception_handler(RecordNotPending)
    async def record_not_pending_handler(request: Request, exc: RecordNotPending):
        # Same status class as RecordAlreadyProcessing (409) — both are "request
        # conflicts with the resource's current status," not a client input error.
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(FileTooLarge)
    async def file_too_large_handler(request: Request, exc: FileTooLarge):
        return JSONResponse(status_code=413, content={"detail": str(exc)})

    @app.exception_handler(RecordNotFound)
    async def record_not_found_handler(request: Request, exc: RecordNotFound):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(RecordAlreadyProcessing)
    async def record_already_processing_handler(request: Request, exc: RecordAlreadyProcessing):
        return JSONResponse(status_code=409, content={"detail": str(exc)})
