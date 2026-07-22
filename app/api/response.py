from typing import Any, Optional
from fastapi.responses import JSONResponse


def response_builder(
    data: Optional[Any] = None,
    message: str = "Success",
    errors: Optional[Any] = None,
    status_code: int = 200,
) -> JSONResponse:
    """
    Standard envelope response builder utility for API endpoints.
    """
    payload = {
        "status_code": status_code,
        "message": message,
        "data": data,
        "errors": errors,
    }
    return JSONResponse(status_code=status_code, content=payload)
