from docarray import dataclass
from docarray.typing import Image, Text
from jina import Client, Document

flow_id = ''
api_key = ''

client = Client(host=f'grpcs://{flow_id}-grpc.wolf.jina.ai')


def call(text=None, image=None):
    @dataclass
    class Query:
        query_text: Text
        query_image: Image

    query_doc = Document(Query(query_text=text, query_image=image))
    result = client.post(
        '/search',
        inputs=query_doc,
        parameters={'api_key': api_key, 'limit': 10, 'access_paths': '@cc'},
    )
    return result[0].matches


print('text search:')
for m in call(text='loading'):
    print(m.tags['id'], m.tags['s3Path'], m.tags['assetUrl'])

print('image search:')
for m in call(
    image='https://upload.wikimedia.org/wikipedia/en/7/7d/Lenna_%28test_image%29.png'
):
    print(m.tags['id'], m.tags['s3Path'], m.tags['assetUrl'])
