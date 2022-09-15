import numpy as np
from docarray import Document, DocumentArray
from jina import Flow
from ..elastic_indexer import ElasticIndexer
from elasticsearch import Elasticsearch

MAPPING = {
    "properties": {
        "id": {"type": "text", "analyzer": "standard"},
        "text": {"type": "text", "analyzer": "standard"},
        "text_embedding": {
            "type": "dense_vector",
            "dims": 7,
            "similarity": "cosine",
            "index": "true",
        },
        "image_embedding": {
            "type": "dense_vector",
            "dims": 5,
            "similarity": "cosine",
            "index": "true",
        },
    }
}

<<<<<<< HEAD

def test_indexing():
    index_name = "test-nest"
    hosts = "http://localhost:9200"
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={"hosts": hosts, "index_name": index_name, "es_mapping": MAPPING},
    ) as f:
=======
def test_indexing():
    index_name = "test-nest"
    hosts = "http://localhost:9200"
    with Flow().add(uses=ElasticIndexer, uses_with={"hosts": hosts, "index_name": index_name,"es_mapping": MAPPING}) as f:
>>>>>>> main
        f.index(
            DocumentArray(
                [
                    Document(
                        id='123',
                        chunks=[
                            Document(
                                id='x',
                                text='this is a flower',
                                embedding=np.ones(7),
                                modality='text',
                            ),
                            Document(
                                id='xx',
                                uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                                embedding=np.ones(5),
                                modality='image',
                            ),
                        ],
                    ),
                    Document(
                        id='456',
                        chunks=[
                            Document(
                                id='xxx',
                                text='this is a cat',
                                embedding=np.ones(7),
                                modality='text',
                            ),
                            Document(
                                id='xxxx',
                                uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                                embedding=np.ones(5),
                                modality='image',
                            ),
                        ],
                    ),
                ]
            )
        )
        es = Elasticsearch(hosts=hosts)
        # fetch all indexed documents
<<<<<<< HEAD
        res = es.search(
            index=index_name, body={'size': 100, 'query': {'match_all': {}}}
        )
        assert len(res['hits']['hits']) == 2


def test_search():
    index_name = "test-nest"
    hosts = "http://localhost:9200"
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={"hosts": hosts, "index_name": index_name, "es_mapping": MAPPING},
    ) as f:
=======
        res = es.search(index=index_name, body={'size':100, 'query': {'match_all': {} }})
        assert len(res['hits']['hits']) == 2

def test_search():
    index_name = "test-nest"
    hosts = "http://localhost:9200"
    with Flow().add(uses=ElasticIndexer, uses_with={"hosts": hosts, "index_name": index_name,"es_mapping": MAPPING}) as f:
>>>>>>> main
        f.index(
            DocumentArray(
                [
                    Document(
                        id='123',
                        chunks=[
                            Document(
                                id='x',
                                text='this is a flower',
                                embedding=np.ones(7),
                                modality='text',
                            ),
                            Document(
                                id='xx',
                                uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                                embedding=np.ones(5),
                                modality='image',
                            ),
                        ],
                    ),
                    Document(
                        id='456',
                        chunks=[
                            Document(
                                id='xxx',
                                text='this is a cat',
                                embedding=np.ones(7),
                                modality='text',
                            ),
                            Document(
                                id='xxxx',
                                uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                                embedding=np.ones(5),
                                modality='image',
                            ),
                        ],
                    ),
                ]
            )
        )
        x = f.search(
<<<<<<< HEAD
            DocumentArray(
                [
                    Document(
                        chunks=[
                            Document(
                                text='this is a flower',
                                embedding=np.random.rand(5),
                                modality='text',
                            )
                        ]
                    )
                ]
            )
        )
=======
                DocumentArray(
                    [
                        Document(
                            chunks=[
                                Document(
                                    text='this is a flower',
                                    embedding=np.random.rand(5),
                                    modality='text',
                                )
                            ]
                        )
                    ]
                )
            )
>>>>>>> main
        assert len(x) != 0
        assert len(x.matches) != 0
