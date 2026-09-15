from fastapi import APIRouter
from app.api.routes.uploads import uploads_router

api_router = APIRouter()
api_router.include_router(uploads_router)
