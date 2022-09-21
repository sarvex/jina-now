import numpy as np
import pytest
from docarray import Document, DocumentArray
from elasticsearch import Elasticsearch
from jina import Flow

from ..elastic_indexer import ElasticIndexer


@pytest.fixture
def multimodal_da():
    return DocumentArray(
        [
            Document(
                id='123',
                tags={'cost': 18.0},
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
                tags={'cost': 21.0},
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


@pytest.fixture
def multimodal_query():
    return DocumentArray(
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
    )


def test_indexing(multimodal_da):
    da = multimodal_da
    index_name = 'test-indexing'
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={'hosts': hosts, 'index_name': index_name, 'dims': [7, 5]},
    ) as f:
        f.index(da)
        es = Elasticsearch(hosts=hosts)
        # fetch all indexed documents
        res = es.search(index=index_name, size=100, query={'match_all': {}})
        assert len(res['hits']['hits']) == 2


def test_search_with_bm25(multimodal_da, multimodal_query):
    da = multimodal_da
    query_da = multimodal_query
    index_name = 'test-search-bm25'
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={'hosts': hosts, 'index_name': index_name, 'dims': [7, 5]},
    ) as f:
        f.index(da)
        x = f.search(
            query_da,
            parameters={'apply_bm25': True},
        )
        assert len(x) != 0
        assert len(x[0].matches) != 0


def test_search_with_filter(multimodal_da, multimodal_query):
    da = multimodal_da
    query_da = multimodal_query
    index_name = 'test-search-filter'
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={'hosts': hosts, 'index_name': index_name, 'dims': [7, 5]},
    ) as f:
        f.index(da)
        x = f.search(
            query_da,
            parameters={'apply_bm25': True, 'filter': {'cost': {'gte': 20.0}}},
        )
        assert len(x) != 0
        assert len(x[0].matches) == 1


def test_list(multimodal_da):
    da = multimodal_da
    index_name = 'test-list'
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={'hosts': hosts, 'index_name': index_name, 'dims': [7, 5]},
    ) as f:
        f.index(da)
        list_res = f.post(on='/list', parameters={'limit': 1, 'offset': 1})
        assert list_res[0].id == '456'
