from jina import Client, Document, DocumentArray

from now.constants import DatasetTypes
from now.now_dataclasses import UserInput

da = DocumentArray.load_binary(
    '/home/girishc13/.cache/jina-now/data/tmp/aHR0cHM6Ly9zdG9yYWdlLmdvb2dsZWFwaXMuY29tL2ppbmEtZmFzaGlvbi1kYXRhL2RhdGEvb25lLWxpbmUvZGF0YXNldHMvanBlZy9kZWVwZmFzaGlvbi5WaVQtQjMyLTAuMTMuMTcuYmlu.bin'
)

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

from now.executor.indexer.qdrant.executor import NOWQdrantIndexer15

client = Client(host='grpc://0.0.0.0:8080')

for docs in da[:200].batch(batch_size=50):
    encoded_da = client.post(on='/encode', inputs=docs, parameters=params)
    client.post(on='/index', inputs=encoded_da, parameters=params)
