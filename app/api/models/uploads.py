from typing import Optional
from pydantic import BaseModel


class UploadResponse(BaseModel):
    message: str
    id: int
    status: str


class CsvUploadStatus(BaseModel):
    id: int
    status: str
    report_type: str
    cloud_storage_path: str
    created_at: Optional[str] = None
