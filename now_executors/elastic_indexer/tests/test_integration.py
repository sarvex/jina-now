import numpy as np
from docarray import Document, DocumentArray
from elasticsearch import Elasticsearch
from jina import Flow

from ..elastic_indexer import ElasticIndexer

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


def test_indexing():
    index_name = "test-nest"
    hosts = "http://localhost:9200"
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={"hosts": hosts, "index_name": index_name, "es_mapping": MAPPING},
    ) as f:
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
            DocumentArray(
                [
                    Document(
                        chunks=[
                            Document(
                                text='this is a flower',
                                embedding=np.random.rand(7),
                                modality='text',
                            )
                        ]
                    )
                ]
            )
        )
        assert len(x) != 0
        assert len(x[0].matches) != 0
