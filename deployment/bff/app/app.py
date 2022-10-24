# TODO bff_request_mapping_fn and bff_response_mapping_fn should be used to create all routes

import logging.config
import sys

import uvicorn
from fastapi import APIRouter, FastAPI, Request
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount

import deployment.bff.app.settings as api_settings
from deployment.bff.app.decorators import api_method, timed
from deployment.bff.app.v1.routers import admin, cloud_temp_link
from deployment.bff.app.v1.routers.data_routes import create_endpoints
from now.common.options import construct_app
from now.constants import Apps

logging.config.dictConfig(api_settings.DEFAULT_LOGGING_CONFIG)
logger = logging.getLogger('bff.app')
logger.setLevel(api_settings.DEFAULT_LOGGING_LEVEL)

TITLE = 'Jina NOW'
DESCRIPTION = 'The Jina NOW service API'
AUTHOR = 'Jina AI'
EMAIL = 'hello@jina.ai'
__version__ = 'latest'


def get_app_instance():
    """Build FastAPI app."""
    app = FastAPI(
        title=TITLE,
        description=DESCRIPTION,
        contact={
            'author': AUTHOR,
            'email': EMAIL,
        },
    )

    @app.get('/ping')
    @api_method
    @timed
    def check_liveness() -> str:
        """
        Sanity check - this will let the caller know that the service is operational.
        """
        return 'pong!'

    @app.get('/')
    @api_method
    @timed
    def read_root() -> str:
        """
        Root path welcome message.
        """
        return (
            f'{TITLE} v{__version__} 🚀 {DESCRIPTION} ✨ '
            f'author: {AUTHOR} email: {EMAIL} 📄  '
            'Check out /docs or /redoc for the API documentation!'
        )

    @app.on_event('startup')
    def startup():
        logger.info(
            f'Jina NOW started! ' f'Listening to [::]:{api_settings.DEFAULT_PORT}'
        )

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

    return app


def get_app_routes():
    routes = []
    for app in Apps():
        router = APIRouter()
        app_instance = construct_app(app)
        input_modality = app_instance.input_modality
        output_modality = app_instance.output_modality
        create_endpoints(router, input_modality, output_modality)

        # Image2Image router
        img2img_mount = f'/api/v1/{input_modality}-to-{output_modality}'
        img2img_app = get_app_instance()
        img2img_app.include_router(router, tags=[app_instance.app_name])
        route = Mount(img2img_mount, img2img_app)
        routes.append(route)
    return routes


def build_app():
    # cloud temporary link router
    cloud_temp_link_mount = '/api/v1/cloud-bucket-utils'
    cloud_temp_link_app = get_app_instance()
    cloud_temp_link_app.include_router(
        cloud_temp_link.router, tags=['Temporary-Link-Cloud']
    )

    # Admin router
    admin_mount = '/api/v1/admin'
    admin_app = get_app_instance()
    admin_app.include_router(admin.router, tags=['admin'])

    # Mount them - for other modalities just add an app instance
    app = Starlette(
        routes=[
            Mount(cloud_temp_link_mount, cloud_temp_link_app),
            Mount(admin_mount, admin_app),
        ]
        + get_app_routes()
    )
    return app


application = build_app()


def run_server():
    """Run server."""
    app = build_app()

    # start the server!
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=8080,
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
