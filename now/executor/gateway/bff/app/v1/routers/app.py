from fastapi import APIRouter

from now.executor.gateway.bff.app.v1.routers.admin import router as admin_router
from now.executor.gateway.bff.app.v1.routers.info import router as info_router
from now.executor.gateway.bff.app.v1.routers.search import router as search_router

search_app_router = APIRouter()
search_app_router.include_router(search_router, tags=['Search App'])
search_app_router.include_router(info_router, tags=['Info'])


# rename admin_router to something more meaningful
extras_router = APIRouter()
extras_router.include_router(admin_router, tags=['Admin level operations'])
