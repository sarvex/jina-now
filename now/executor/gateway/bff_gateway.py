from jina.serve.runtimes.gateway.http.fastapi import FastAPIBaseGateway
from jina.serve.runtimes.gateway.http.models import JinaHealthModel


class BFFGateway(FastAPIBaseGateway):
    @property
    def app(self):
        from now.executor.gateway.bff.app.app import application

        # fix to use starlette instead of FastAPI app (throws warning that "/" is used for health checks
        application.add_route(
            path='/', route=lambda: JinaHealthModel(), methods=['GET']
        )

        return application
