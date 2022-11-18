from jina import Client

flow_id = '...'
api_key = ''
jwt = ''

client = Client(host=f'grpcs://nowapi-{flow_id}.wolf.jina.ai')

client.post(
    '/curate',
    parameters={
        'api_key': api_key,
        'jwt': jwt,
        'query_to_filter': {
            'query1': [{'uri': {'$eq': '3'}}],
            'query2': [
                {'uri': {'$eq': 'uri2'}},
                {'tags__color': {'$eq': 'red'}},
            ],
        },
    },
)
