import numpy as np
import pytest
from docarray import Document, DocumentArray
from elasticsearch import Elasticsearch
from jina import Flow

from ..elastic_indexer import ElasticIndexer

MAPPING = {
    'properties': {
        'id': {'type': 'keyword'},
        'text': {'type': 'text', 'analyzer': 'standard'},
        'embedding_0': {
            'type': 'dense_vector',
            'dims': 7,
            'similarity': 'cosine',
            'index': 'true',
        },
        'embedding_1': {
            'type': 'dense_vector',
            'dims': 5,
            'similarity': 'cosine',
            'index': 'true',
        },
    }
}


@pytest.fixture
def multimodal_da():
    return DocumentArray(
        [
            Document(
                id='123',
                chunks=[
                    Document(
                        id='x',
                        text='this is a flower',
                        embedding=np.ones(7),
                    ),
                    Document(
                        id='xx',
                        uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                        embedding=np.ones(5),
                    ),
                ],
            ),
            Document(
                id='456',
                chunks=[
                    Document(
                        id='xxx',
                        text='this is a cat',
                        embedding=np.array([1, 2, 3, 4, 5, 6, 7]),
                    ),
                    Document(
                        id='xxxx',
                        uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                        embedding=np.array([1, 2, 3, 4, 5]),
                    ),
                ],
            ),
        ]
    )


def test_indexing(multimodal_da):
    da = multimodal_da
    index_name = 'test-nest1'
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={'hosts': hosts, 'index_name': index_name, 'dims': [7, 5]},
    ) as f:
        f.index(da)
        es = Elasticsearch(hosts=hosts)
        # fetch all indexed documents
        res = es.search(index=index_name, size=100, query={'match_all': {}})
        print(res['hits']['hits'])
        assert len(res['hits']['hits']) == 2


def test_search_with_bm25(multimodal_da):
    da = multimodal_da
    index_name = 'test-nest2'
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={'hosts': hosts, 'index_name': index_name, 'dims': [7, 5]},
    ) as f:
        f.index(da)
        x = f.search(
            DocumentArray(
                [
                    Document(
                        text='cat',
                        chunks=[
                            Document(
                                embedding=np.array([1, 2, 3, 4, 5, 6, 7]),
                            ),
                            Document(
                                embedding=np.array([1, 2, 3, 4, 5]),
                            ),
                        ],
                    )
                ]
            ),
            parameters={'apply_bm25': True},
        )
        assert len(x) != 0
        assert len(x[0].matches) != 0
