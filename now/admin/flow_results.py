from jina import Client, Document

flow_id = ''
api_key = ''
search_text = ''
limit = 60


client = Client(host=f'grpcs://nowapi-{flow_id}.wolf.jina.ai')


def call():
    result = client.post(
        '/search',
        inputs=Document(chunks=Document(text=search_text)),
        parameters={
            'api_key': api_key,
            'limit': limit,
        },
    )
    return result[0].matches[:, 'uri']


for uri in call():
    print(uri)
