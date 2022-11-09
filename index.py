from jina import DocumentArray, Document, Client

da = DocumentArray.load_binary(
    '/home/girishc13/.cache/jina-now/data/tmp/aHR0cHM6Ly9zdG9yYWdlLmdvb2dsZWFwaXMuY29tL2ppbmEtZmFzaGlvbi1kYXRhL2RhdGEvb25lLWxpbmUvZGF0YXNldHMvanBlZy9kZWVwZmFzaGlvbi5WaVQtQjMyLTAuMTMuMTcuYmlu.bin'
)

client = Client(host='grpc://0.0.0.0:8080')
params = {
    'user_input': user_input.__dict__,
    'traversal_paths': '@r',
    'access_paths': '@r',
}

for docs in da[:1000].batch(batch_size=50):
    client.post('/index', docs, parameters=params)
