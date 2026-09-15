import secrets
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from app.config import settings

security = HTTPBearer()


async def verify_auth_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verifies that the incoming Bearer token matches AUTH_TOKEN in settings."""
    if not secrets.compare_digest(credentials.credentials, settings.AUTH_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid token")
