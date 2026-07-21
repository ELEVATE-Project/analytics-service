from pydantic import BaseModel
from typing import List, Optional


class UploadResponse(BaseModel):
    message: str
    id: int
    status: str
    cloud_storage_path: str
    errors: Optional[List[str]] = None
