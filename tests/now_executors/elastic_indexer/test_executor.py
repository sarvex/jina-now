from typing import Union, List

import pytest
from docarray import DocumentArray
from elasticsearch import Elasticsearch
from jina import Flow

from now_executors.elastic_indexer.elastic_indexer import ElasticIndexer


@pytest.mark.parametrize(
    'da, traversal_paths, dims',
    [('text_da', '@r', 7), ('multimodal_da', '@c', [7, 5])],
)
def test_indexing(
    da: DocumentArray,
    traversal_paths: str,
    dims: Union[str, List[int]],
    setup_service_running,
    es_connection_params,
    request,
):
    da = request.getfixturevalue(da)
    index_name = 'test-indexing'
    hosts, _ = es_connection_params
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
    setup_service_running,
    es_connection_params,
    request,
):
    da = request.getfixturevalue(da)
    query_da = request.getfixturevalue(query_da)
    hosts, _ = es_connection_params
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
    setup_service_running,
    es_connection_params,
    request,
):
    da = request.getfixturevalue(da)
    query_da = request.getfixturevalue(query_da)
    hosts, _ = es_connection_params
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
    setup_service_running,
    es_connection_params,
    request,
):
    da = request.getfixturevalue(da)
    hosts, _ = es_connection_params
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
    setup_service_running,
    es_connection_params,
    request,
):
    da = request.getfixturevalue(da)
    hosts, _ = es_connection_params
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
