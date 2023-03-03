import os

from docarray.typing import Text

from now.executor.indexer.elastic.elastic_indexer import (
    FieldEmbedding,
    NOWElasticIndexer,
)
from now.now_dataclasses import UserInput


def test_generate_es_mappings(setup_service_running, random_index_name):
    """
    This test should check, whether the static
    generate_es_mappings method works as expected.
    """
    document_mappings = [
        FieldEmbedding('clip', 8, ['title']),
    ]
    user_input = UserInput()
    user_input.index_fields = ['title']
    user_input.index_field_candidates_to_modalities = {'title': Text}
    user_input.field_names_to_dataclass_fields = {'title': 'text_0'}
    es_indexer = NOWElasticIndexer(
        document_mappings=document_mappings, user_input_dict=user_input.to_safe_dict()
    )
    expected_mapping = {
        'properties': {
            'id': {'type': 'keyword'},
            'text_0': {'type': 'text', 'analyzer': 'standard'},
            'title-clip': {
                'properties': {
                    'embedding': {
                        'type': 'dense_vector',
                        'dims': '8',
                        'similarity': 'cosine',
                        'index': 'true',
                    }
                }
            },
        }
    }
    result = es_indexer.generate_es_mapping()
    assert result == expected_mapping


def test_index_and_search_with_multimodal_docs(
    setup_service_running, es_inputs, random_index_name
):
    """
    This test runs indexing with the NOWElasticIndexer using multimodal docs.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    ) = es_inputs

    indexer = NOWElasticIndexer(
        document_mappings=document_mappings,
        user_input_dict=user_input.to_safe_dict(),
    )

    indexer.index(index_docs_map)
    # check if documents are indexed
    es = indexer.es
    res = es.search(index=os.getenv('ES_INDEX_NAME'), size=100, query={'match_all': {}})
    assert len(res['hits']['hits']) == len(index_docs_map['clip'])
    results = indexer.search(
        query_docs_map,
        parameters={
            'get_score_breakdown': True,
            'score_calculation': default_score_calculation,
        },
    )
    # asserts about matches
    for (
        query_field,
        document_field,
        encoder,
        linear_weight,
    ) in default_score_calculation:
        if encoder == 'bm25':
            assert 'bm25_normalized' in results[0].matches[0].scores
            assert 'bm25_raw' in results[0].matches[0].scores
            assert isinstance(
                results[0].matches[0].scores['bm25_normalized'].value, float
            )
            assert isinstance(results[0].matches[0].scores['bm25_raw'].value, float)
        else:
            score_string = '-'.join(
                [
                    query_field,
                    document_field,
                    encoder,
                    str(linear_weight),
                ]
            )
            assert score_string in results[0].matches[0].scores
            assert isinstance(results[0].matches[0].scores[score_string].value, float)


def test_list_endpoint(setup_service_running, es_inputs, random_index_name):
    """
    This test tests the list endpoint of the NOWElasticIndexer.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    ) = es_inputs
    es_indexer = NOWElasticIndexer(
        document_mappings=document_mappings,
        user_input_dict=user_input.to_safe_dict(),
    )
    es_indexer.index(index_docs_map)
    result = es_indexer.list()
    assert len(result) == len(index_docs_map['clip'])
    limit = 1
    result_with_limit = es_indexer.list(parameters={'limit': limit})
    assert len(result_with_limit) == limit
    offset = 1
    result_with_offset = es_indexer.list(parameters={'offset': offset})
    assert len(result_with_offset) == len(index_docs_map['clip']) - offset


def test_count_endpoint(setup_service_running, es_inputs, random_index_name):
    """
    This test tests the count endpoint of the NOWElasticIndexer.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    ) = es_inputs
    es_indexer = NOWElasticIndexer(
        document_mappings=document_mappings,
        user_input_dict=user_input.to_safe_dict(),
    )
    es_indexer.index(index_docs_map)
    result = es_indexer.count()
    assert result[0].tags['count'] == len(index_docs_map['clip'])
    limit = 1
    result_with_limit = es_indexer.count(parameters={'limit': limit})
    assert result_with_limit[0].tags['count'] == limit
    offset = 1
    result_with_offset = es_indexer.count(parameters={'offset': offset})
    assert result_with_offset[0].tags['count'] == len(index_docs_map['clip']) - offset


def test_delete_by_id(setup_service_running, es_inputs, random_index_name):
    """
    This test tests the delete endpoint of the NOWElasticIndexer, by deleting a list of IDs.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    ) = es_inputs
    es_indexer = NOWElasticIndexer(
        document_mappings=document_mappings,
        user_input_dict=user_input.to_safe_dict(),
    )
    es_indexer.index(index_docs_map)
    # delete by id
    ids = [doc.id for doc in index_docs_map['clip']]
    es_indexer.delete(parameters={'ids': ids})

    es = es_indexer.es
    res = es.search(index=os.getenv('ES_INDEX_NAME'), size=100, query={'match_all': {}})
    assert len(res['hits']['hits']) == 0


def test_delete_by_filter(setup_service_running, es_inputs, random_index_name):
    """
    This test tests the delete endpoint of the NOWElasticIndexer, by deleting a filter.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    ) = es_inputs
    es_indexer = NOWElasticIndexer(
        document_mappings=document_mappings,
        user_input_dict=user_input.to_safe_dict(),
    )
    es_indexer.index(index_docs_map)

    # delete by filter
    es_indexer.delete(parameters={'filter': {'tags__price': {'$gte': 0}}})

    es = es_indexer.es
    res = es.search(index=os.getenv('ES_INDEX_NAME'), size=100, query={'match_all': {}})
    assert len(res['hits']['hits']) == 0


def test_custom_mapping_and_search(setup_service_running, es_inputs, random_index_name):
    """
    This test tests the custom mapping and bm25 functionality of the NOWElasticIndexer.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    ) = es_inputs
    es_mapping = {
        'properties': {
            'id': {'type': 'keyword'},
            'title': {'type': 'text', 'analyzer': 'standard'},
            'excerpt': {'type': 'text', 'analyzer': 'standard'},
            'title-clip': {
                'properties': {
                    'embedding': {
                        'type': 'dense_vector',
                        'dims': '8',
                        'similarity': 'cosine',
                        'index': 'true',
                    }
                }
            },
            'gif-clip': {
                'properties': {
                    'embedding': {
                        'type': 'dense_vector',
                        'dims': '8',
                        'similarity': 'cosine',
                        'index': 'true',
                    }
                }
            },
        }
    }
    es_indexer = NOWElasticIndexer(
        document_mappings=document_mappings,
        es_mapping=es_mapping,
        user_input_dict=user_input.to_safe_dict(),
    )
    # do indexing
    es_indexer.index(index_docs_map)

    results = es_indexer.search(
        query_docs_map,
        parameters={
            'get_score_breakdown': True,
            'score_calculation': default_score_calculation,
        },
    )
    assert len(results[0].matches) == 2
    assert results[0].matches[0].id == '0'
    assert results[0].matches[1].id == '1'


def test_search_with_filter(setup_service_running, es_inputs, random_index_name):
    """
    This test tests the search endpoint of the NOWElasticIndexer using filters.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        user_input,
    ) = es_inputs
    es_indexer = NOWElasticIndexer(
        document_mappings=document_mappings,
        user_input_dict=user_input.to_safe_dict(),
    )
    es_indexer.index(index_docs_map)

    res = es_indexer.search(
        query_docs_map,
        parameters={
            'get_score_breakdown': True,
            'score_calculation': default_score_calculation,
            'filter': {'tags__price': {'$lte': 1}},
        },
    )
    assert len(res[0].matches) == 1
    assert res[0].matches[0].tags['price'] < 1
