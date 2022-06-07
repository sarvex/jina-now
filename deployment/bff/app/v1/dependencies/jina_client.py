from jina.clients import Client


def get_jina_client(host: str = 'localhost', port: int = 31080) -> Client:
    if 'wolf.jina.ai' in host:
        return Client(host=host)
    else:
        return Client(host=host, port=port)
