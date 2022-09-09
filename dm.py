from docarray import Document
from jina import Client

client = Client(host='localhost', port=31080)
a = client.post('/encode', inputs=Document(text='hello world'), return_results=True)
print(a)
