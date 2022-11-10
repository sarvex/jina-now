from jina import Client, Document, DocumentArray

from now.constants import DatasetTypes
from now.now_dataclasses import UserInput

client = Client(host='grpc://0.0.0.0:8080')
user_input = UserInput()
user_input.dataset_type = DatasetTypes.DEMO
user_input.dataset_name = 'deepfashion'
user_input.cluster = 'kind-jina-now'
user_input.deployment_type = 'local'
params = {
    'user_input': user_input.__dict__,
    'traversal_paths': '@r',
    'access_paths': '@r',
}

# print('suggestions')
# suggestions_da = client.post('/suggestion', DocumentArray(Document(text='jacket mens')))
# suggestions_da.summary()
# print(suggestions_da['@m'])

print('search')
search_da = client.post('/search', DocumentArray(Document(text='jacket mens')))
search_da.summary()
print(search_da['@m'])
