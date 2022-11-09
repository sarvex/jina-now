from jina import DocumentArray, Document, Client

client = Client(host='grpc://0.0.0.0:8080')

print('suggestions')
suggestions_da = client.post('/suggestion', DocumentArray(Document(text='jacket mens')))
suggestions_da.summary()
print(suggestions_da['@m'])

print('search')
search_da = client.post('/search', DocumentArray(Document(text='jacket mens')))
search_da.summary()
print(search_da['@m'])
