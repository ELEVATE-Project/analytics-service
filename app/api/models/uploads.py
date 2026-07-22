from typing import List, Optional
from pydantic import BaseModel


class UploadResponse(BaseModel):
    message: str
    id: int
    status: str
    errors: Optional[List[str]] = None


class CsvUploadStatus(BaseModel):
    id: int
    status: str
    report_type: str
    cloud_storage_path: str
    created_at: Optional[str] = None
