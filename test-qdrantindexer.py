from jina import Flow, Client
from docarray import Document
import os
import numpy as np

os.environ['JINA_LOG_LEVEL'] = 'DEBUG'

# f = Flow().add(
#     uses='docker://qdrantindexer15:local',
#     uses_with={'collection_name': 'test', 'dim': 2},
# )
c = Client(host='grpc://0.0.0.0:55887')

# with f:
c.post(
    on='/index',
    inputs=[
        Document(id='a', embedding=np.zeros((1, 512))),
        Document(id='b', embedding=np.ones((1, 512))),
    ],
)

docs = c.post(
    on='/search',
    inputs=[Document(embedding=np.ones((1, 512)))],
)

# will print "The ID of the best match of [1,1] is: b"
print('The ID of the best match of [1,1] is: ', docs[0].matches[0].id)
