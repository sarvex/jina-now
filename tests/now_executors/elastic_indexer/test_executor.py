from typing import Union, List

import numpy as np
import pytest
from docarray import Document, DocumentArray
from elasticsearch import Elasticsearch
from jina import Flow

from now_executors.elastic_indexer.elastic_indexer import ElasticIndexer


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


@pytest.fixture
def text_da():
    return DocumentArray(
        [
            Document(
                id='123',
                tags={'cost': 18.0},
                text='test text',
                embedding=np.ones(7),
            ),
            Document(
                id='456',
                tags={'cost': 21.0},
                text='another text',
                embedding=np.array([1, 2, 3, 4, 5, 6, 7]),
            ),
        ]
    )


@pytest.fixture
def text_query():
    return DocumentArray([Document(text='text', embedding=np.ones(7))])


@pytest.fixture
def pop_lyrics_dataset():
    da = DocumentArray.load_binary(
        '/Users/jinadev/Downloads/data_one-line_datasets_text_pop-lyrics.ViT-B16-0.13.17.bin'
    )
    return da[:300]


@pytest.mark.parametrize(
    'da, traversal_paths, dims',
    [('text_da', '@r', 7), ('multimodal_da', '@c', [7, 5])],
)
def test_indexing(
    da: DocumentArray, traversal_paths: str, dims: Union[str, List[int]], request
):
    da = request.getfixturevalue(da)
    index_name = 'test-indexing'
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={
            'hosts': hosts,
            'index_name': index_name,
            'dims': dims,
            'traversal_paths': traversal_paths,
        },
    ) as f:
        f.index(da)
        es = Elasticsearch(hosts=hosts)
        # fetch all indexed documents
        res = es.search(index=index_name, size=100, query={'match_all': {}})
        assert len(res['hits']['hits']) == 2


@pytest.mark.parametrize(
    'da, query_da, traversal_paths, dims, index_name',
    [
        ('text_da', 'text_query', '@r', 7, 'test-search-bm25-text'),
        (
            'multimodal_da',
            'multimodal_query',
            '@c',
            [7, 5],
            'test-search-bm25-multimodal',
        ),
    ],
)
def test_search_with_bm25(
    da: DocumentArray,
    query_da: DocumentArray,
    traversal_paths: str,
    dims: Union[int, List[int]],
    index_name: str,
    request,
):
    da = request.getfixturevalue(da)
    query_da = request.getfixturevalue(query_da)
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={
            'hosts': hosts,
            'index_name': index_name,
            'dims': dims,
            'traversal_paths': traversal_paths,
        },
    ) as f:
        f.index(da)
        x = f.search(
            query_da,
            parameters={'apply_bm25': True},
        )
        assert len(x) != 0
        assert len(x[0].matches) != 0


@pytest.mark.parametrize(
    'da, query_da, traversal_paths, dims, index_name',
    [
        ('text_da', 'text_query', '@r', 7, 'test-search-filter-text'),
        (
            'multimodal_da',
            'multimodal_query',
            '@c',
            [7, 5],
            'test-search-filter-multimodal',
        ),
    ],
)
def test_search_with_filter(
    da: DocumentArray,
    query_da: DocumentArray,
    traversal_paths: str,
    index_name: str,
    dims: Union[int, List[int]],
    request,
):
    da = request.getfixturevalue(da)
    query_da = request.getfixturevalue(query_da)
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={
            'hosts': hosts,
            'index_name': index_name,
            'traversal_paths': traversal_paths,
            'dims': dims,
        },
    ) as f:
        f.index(da)
        x = f.search(
            query_da,
            parameters={'apply_bm25': True, 'filter': {'cost': {'gte': 20.0}}},
        )
        assert len(x) != 0
        assert len(x[0].matches) == 1


@pytest.mark.parametrize(
    'da, traversal_paths, dims, index_name',
    [
        ('text_da', '@r', 7, 'test-list-text'),
        ('multimodal_da', '@c', [7, 5], 'test-list-multimodal'),
    ],
)
def test_list(
    da: DocumentArray,
    traversal_paths: str,
    dims: Union[int, List[int]],
    index_name: str,
    request,
):
    da = request.getfixturevalue(da)
    hosts = 'http://localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={
            'hosts': hosts,
            'index_name': index_name,
            'traversal_paths': traversal_paths,
            'dims': dims,
        },
    ) as f:
        f.index(da)
        list_res = f.post(on='/list', parameters={'limit': 1, 'offset': 1})
        assert list_res[0].id == '456'


@pytest.mark.parametrize(
    'da, traversal_paths, dims, index_name',
    [
        ('text_da', '@r', 7, 'test-delete-text'),
        ('multimodal_da', '@c', [7, 5], 'test-delete-multimodal'),
    ],
)
def test_delete(
    da: DocumentArray,
    traversal_paths: str,
    dims: Union[int, List[int]],
    index_name: str,
    request,
):
    da = request.getfixturevalue(da)
    hosts = 'http://elastic:elastic@localhost:9200'
    with Flow().add(
        uses=ElasticIndexer,
        uses_with={
            'hosts': hosts,
            'index_name': index_name,
            'traversal_paths': traversal_paths,
            'dims': dims,
        },
    ) as f:
        f.index(da)
        es = Elasticsearch(hosts=hosts)
        # fetch all indexed documents
        res = es.search(index=index_name, size=100, query={'match_all': {}})
        assert len(res['hits']['hits']) == 2
        f.post(on='/delete', parameters={'filter': {'cost': {'gte': 15.0}}})
        res = es.search(index=index_name, size=100, query={'match_all': {}})
        assert len(res['hits']['hits']) == 0
