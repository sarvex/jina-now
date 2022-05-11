from app.v1.routers import image, text
from fastapi import APIRouter

v1_router = APIRouter()
v1_router.include_router(image.router, prefix='/image', tags=['Image'])
v1_router.include_router(text.router, prefix='/text', tags=['Text'])
