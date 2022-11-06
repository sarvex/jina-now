import logging.config
import sys

import uvicorn
from fastapi import APIRouter, FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount

from deployment.bff.app.constants import (
    DEFAULT_HOST,
    DEFAULT_LOGGING_CONFIG,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_PORT,
    DESCRIPTION,
    TITLE,
)
from deployment.bff.app.endpoint.legacy import admin, cloud_temp_link
from deployment.bff.app.route_generation import create_endpoints
from now.common.options import construct_app
from now.constants import Apps

logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
logger = logging.getLogger('bff.app')
logger.setLevel(DEFAULT_LOGGING_LEVEL)


def get_fast_api_app(app_name):
    """Build FastAPI app."""
    app = FastAPI(
        title=TITLE,
        description=DESCRIPTION,
        swagger_ui_parameters={
            "defaultModelsExpandDepth": -1,
        },
        docs_url=None,
    )

    try:
        app.mount("/static", StaticFiles(directory="static"), name="static")
    except Exception as e:
        logger.error(f'Failed to mount static files: {e}')

    @app.get("/docs", include_in_schema=False)
    async def custom_swagger_ui_html():
        return get_swagger_ui_html(
            openapi_url=f'/api/v1/{app_name}/openapi.json',
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
            # swagger_js_url="/static/swagger-ui-bundle.js",
            swagger_css_url=f'/api/v1/{app_name}/static/swagger-ui.css',
        )

    extend_default_routes(app)
    return app


def extend_default_routes(app):
    @app.on_event('startup')
    def startup():
        logger.info(f'Jina NOW started! ' f'Listening to [::]:{DEFAULT_PORT}')

    @app.exception_handler(Exception)
    async def unicorn_exception_handler(request: Request, exc: Exception):
        import traceback

        error = traceback.format_exc()
        return JSONResponse(
            status_code=500,
            content={
                "message": f"Exception in BFF, but the root cause can still be in the flow: {error}"
            },
        )


def get_app_routes():
    routes = []
    for app in Apps():
        router = APIRouter()
        app_instance = construct_app(app)
        input_modality = app_instance.input_modality
        output_modality = app_instance.output_modality
        create_endpoints(router, input_modality, output_modality)

        # Image2Image router
        app_name = f'{input_modality}-to-{output_modality}'
        mount_path = f'/api/v1/{app_name}'
        fast_api_app = get_fast_api_app(app_name)

        fast_api_app.include_router(router, tags=[app_instance.app_name])

        route = Mount(mount_path, fast_api_app)
        routes.append(route)
    return routes


def get_additional_routes():
    routes = []
    for app_name, tag, router in [
        ('cloud-bucket-utils', 'Temporary-Link-Cloud', cloud_temp_link.router),
        ('admin', 'admin', admin.router),
    ]:
        partial_app = get_fast_api_app(app_name)
        partial_app.include_router(router, tags=[tag])
        routes.append(Mount(f'/api/v1/{app_name}', partial_app))
    return routes


def build_app():
    app = Starlette(routes=get_app_routes() + get_additional_routes())
    return app


def run_server():
    """Run server."""
    app = build_app()

    # start the server!
    uvicorn.run(
        app,
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        loop='uvloop',
        http='httptools',
    )


if __name__ == '__main__':
    try:
        run_server()
    except Exception as exc:
        logger.critical(str(exc))
        logger.exception(exc)
        sys.exit(1)
else:
    application = build_app()
