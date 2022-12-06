import random
from collections import namedtuple
from typing import List

import numpy as np
import pytest
from docarray import dataclass
from docarray.score import NamedScore
from docarray.typing import Image, Text
from jina import Document, DocumentArray

from now.executor.indexer.elastic.elastic_indexer import (
    ElasticIndexer,
    FieldEmbedding,
    SemanticScore,
)
from now.executor.indexer.elastic.es_converter import ESConverter


def random_index_name():
    return f"test-index-{random.randint(0, 10000)}"


def test_generate_es_mappings(setup_service_running):
    """
    This test should check, whether the static
    generate_es_mappings method works as expected.
    """
    document_mappings = [
        FieldEmbedding('clip', 8, ['title']),
        FieldEmbedding('sbert', 5, ['title', 'excerpt']),
    ]
    expected_mapping = {
        'properties': {
            'id': {'type': 'keyword'},
            'bm25_text': {'type': 'text', 'analyzer': 'standard'},
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
            'title-sbert': {
                'properties': {
                    'embedding': {
                        'type': 'dense_vector',
                        'dims': '5',
                        'similarity': 'cosine',
                        'index': 'true',
                    },
                }
            },
            'excerpt-sbert': {
                'properties': {
                    'embedding': {
                        'type': 'dense_vector',
                        'dims': '5',
                        'similarity': 'cosine',
                        'index': 'true',
                    }
                }
            },
        }
    }
    result = ElasticIndexer.generate_es_mapping(
        document_mappings=document_mappings, metric='cosine'
    )
    assert result == expected_mapping


@pytest.fixture
def es_inputs() -> namedtuple:
    @dataclass
    class MMDoc:
        title: Text
        excerpt: Text
        gif: List[Image]

    @dataclass
    class MMQuery:
        query_text: Text

    document_mappings = [
        FieldEmbedding('clip', 8, ['title', 'gif']),
        FieldEmbedding('sbert', 5, ['title', 'excerpt']),
    ]

    default_semantic_scores = [
        SemanticScore('query_text', 'clip', 'title', 'clip', 1),
        SemanticScore('query_text', 'clip', 'gif', 'clip', 1),
        SemanticScore('query_text', 'sbert', 'title', 'sbert', 1),
        SemanticScore('query_text', 'sbert', 'excerpt', 'sbert', 3),
        SemanticScore('query_text', 'bm25', 'my_bm25_query', 'bm25', 1),
    ]
    docs = [
        MMDoc(
            title='cat test title cat',
            excerpt='cat test excerpt cat',
            gif=[
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
            ],
        ),
        MMDoc(
            title='test title dog',
            excerpt='test excerpt 2',
            gif=[
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
                'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg',
            ],
        ),
    ]
    prep_docs = DocumentArray()
    clip_docs = DocumentArray()
    sbert_docs = DocumentArray()
    # encode our documents
    for i, doc in enumerate(docs):
        prep_doc = Document(doc)
        prep_doc.id = str(i)
        clip_doc = Document(prep_doc, copy=True)
        clip_doc.id = prep_doc.id
        sbert_doc = Document(prep_doc, copy=True)
        sbert_doc.id = prep_doc.id

        clip_doc.title.embedding = np.random.random(8)
        clip_doc.gif[0].embedding = np.random.random(8)
        clip_doc.gif[1].embedding = np.random.random(8)
        clip_doc.gif[2].embedding = np.random.random(8)
        sbert_doc.title.embedding = np.random.random(5)
        sbert_doc.excerpt.embedding = np.random.random(5)

        prep_docs.append(prep_doc)
        clip_docs.append(clip_doc)
        sbert_docs.append(sbert_doc)

    index_docs_map = {
        'preprocessor': prep_docs,
        'clip': clip_docs,
        'sbert': sbert_docs,
    }

    query = MMQuery(query_text='cat')

    query_doc = Document(query)
    clip_doc = Document(query_doc, copy=True)
    clip_doc.id = query_doc.id
    sbert_doc = Document(query_doc, copy=True)
    sbert_doc.id = query_doc.id

    clip_doc.query_text.embedding = np.random.random(8)
    sbert_doc.query_text.embedding = np.random.random(5)

    query_docs_map = {
        'preprocessor': DocumentArray([query_doc]),
        'clip': DocumentArray([clip_doc]),
        'sbert': DocumentArray([sbert_doc]),
    }
    EsInputs = namedtuple(
        'EsInputs',
        [
            'index_docs_map',
            'query_docs_map',
            'document_mappings',
            'default_semantic_scores',
        ],
    )
    return EsInputs(
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    )


def test_doc_map_to_es(setup_service_running, es_inputs):
    """
    This test should check whether the docs_map is correctly
    transformed to the expected format.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    ) = es_inputs
    encoder_to_fields = {
        document_mapping.encoder: document_mapping.fields
        for document_mapping in document_mappings
    }
    index_name = random_index_name()
    es_converter = ESConverter()
    first_doc_clip = index_docs_map['clip'][0]
    first_doc_sbert = index_docs_map['sbert'][0]
    first_result = es_converter.convert_doc_map_to_es(
        docs_map=index_docs_map,
        index_name=index_name,
        encoder_to_fields=encoder_to_fields,
    )[0]
    assert first_result['id'] == first_doc_clip.id
    assert len(first_result['title-clip.embedding']) == len(
        first_doc_clip.title.embedding.tolist()
    )
    assert len(first_result['title-sbert.embedding']) == len(
        first_doc_sbert.title.embedding.tolist()
    )
    assert len(first_result['excerpt-sbert.embedding']) == len(
        first_doc_sbert.excerpt.embedding.tolist()
    )
    assert (
        first_result['bm25_text']
        == first_doc_clip.title.text
        + ' '
        + first_doc_sbert.title.text
        + ' '
        + first_doc_sbert.excerpt.text
        + ' '
    )
    assert first_result['_op_type'] == 'index'


def test_index_and_search_with_multimodal_docs(setup_service_running, es_inputs):
    """
    This test runs indexing with the ElasticIndexer using multimodal docs.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    ) = es_inputs

    # default should be: all combinations ?? TODO: clarify if that is true
    index_name = random_index_name()

    indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        # es_config={'api_key': os.environ['ELASTIC_API_KEY']},
        # hosts='https://5280f8303ccc410295d02bbb1f3726f7.eu-central-1.aws.cloud.es.io:443',
        hosts='http://localhost:9200',
        index_name=index_name,
        document_structure='MMDoc()',
    )

    indexer.index(index_docs_map)
    # check if documents are indexed
    es = indexer.es
    res = es.search(index=index_name, size=100, query={'match_all': {}})
    assert len(res['hits']['hits']) == len(index_docs_map['clip'])
    results = indexer.search(
        query_docs_map,
        parameters={'get_score_breakdown': True, 'apply_default_bm25': True},
    )

    # asserts about matches
    for (
        query_field,
        query_encoder,
        document_field,
        document_encoder,
        linear_weight,
    ) in default_semantic_scores:
        if document_encoder == 'bm25':
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
                    document_encoder,
                    str(linear_weight),
                ]
            )
            assert score_string in results[0].matches[0].scores
            assert isinstance(results[0].matches[0].scores[score_string].value, float)


def test_list_endpoint(setup_service_running, es_inputs):
    """
    This test tests the list endpoint of the ElasticIndexer.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    ) = es_inputs
    index_name = random_index_name()
    es_indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        hosts='http://localhost:9200',
        index_name=index_name,
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


def test_delete_by_id(setup_service_running, es_inputs):
    """
    This test tests the delete endpoint of the ElasticIndexer, by deleting a list of IDs.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    ) = es_inputs
    index_name = random_index_name()
    es_indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        hosts='http://localhost:9200',
        index_name=index_name,
    )
    es_indexer.index(index_docs_map)
    # delete by id
    ids = [doc.id for doc in index_docs_map['clip']]
    es_indexer.delete(parameters={'ids': ids})

    es = es_indexer.es
    res = es.search(index=index_name, size=100, query={'match_all': {}})
    assert len(res['hits']['hits']) == 0


def test_calculate_score_breakdown(setup_service_running, es_inputs):
    """
    This test tests the calculate_score_breakdown function of the ESConverter.
    """
    default_semantic_scores = es_inputs.default_semantic_scores
    metric = 'cosine'
    query_doc = Document(
        tags={
            'embeddings': {
                'query_text-clip': np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
                'query_text-sbert': np.array([1.0, 0.2, 0.3, 0.4, 0.5]),
            }
        },
    )
    retrieved_doc = Document(
        tags={
            'embeddings': {
                'title-clip.embedding': np.array(
                    [0.1, 0.3, 0.2, 0.6, 0.5, 0.1, 0.7, 0.8]
                ),
                'gif-clip.embedding': np.array(
                    [0.1, 0.3, 0.2, 0.6, 0.5, 0.1, 0.7, 0.8]
                ),
                'title-sbert.embedding': np.array([0.1, 0.6, 0.3, 0.4, 0.9]),
                'excerpt-sbert.embedding': np.array([0.4, 0.2, 0.3, 0.7, 0.5]),
            }
        },
        scores={metric: NamedScore(value=5.0)},
    )
    es_converter = ESConverter()
    doc_score_breakdown = es_converter.calculate_score_breakdown(
        query_doc=query_doc,
        retrieved_doc=retrieved_doc,
        metric=metric,
        semantic_scores=default_semantic_scores,
    )
    scores = {
        'cosine': {'value': 5.0},
        'query_text-title-clip-1': {'value': 0.921791},
        'query_text-gif-clip-1': {'value': 0.921791},
        'query_text-title-sbert-1': {'value': 0.619954},
        'query_text-excerpt-sbert-3': {'value': 2.524923},
        'bm25_normalized': {'value': 0.011541},
        'bm25_raw': {'value': -9.88459},
    }
    for score, val in scores.items():
        assert doc_score_breakdown.scores[score].value == val['value']


def test_custom_mapping_and_custom_bm25_search(setup_service_running, es_inputs):
    """
    This test tests the custom mapping and bm25 functionality of the ElasticIndexer.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    ) = es_inputs
    index_name = random_index_name()
    es_mapping = {
        'properties': {
            'id': {'type': 'keyword'},
            'bm25_text': {'type': 'text', 'analyzer': 'standard'},
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
            'title-sbert': {
                'properties': {
                    'embedding': {
                        'type': 'dense_vector',
                        'dims': '5',
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
            'excerpt-sbert': {
                'properties': {
                    'embedding': {
                        'type': 'dense_vector',
                        'dims': '5',
                        'similarity': 'cosine',
                        'index': 'true',
                    }
                }
            },
        }
    }
    es_indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        es_mapping=es_mapping,
        hosts='http://localhost:9200',
        index_name=index_name,
    )
    # do indexing
    es_indexer.index(index_docs_map)
    # search with custom bm25 query with field boosting
    custom_bm25_query = {
        'multi_match': {
            'query': 'this cat is cute',
            'fields': ['bm25_text^7'],
            'tie_breaker': 0.3,
        }
    }
    results = es_indexer.search(
        query_docs_map,
        parameters={
            'get_score_breakdown': True,
            'custom_bm25_query': custom_bm25_query,
        },
    )
    assert len(results[0].matches) == 2
    print(results[0].matches[0].title.text)
    print(results[0].matches[0].scores)
    print(results[0].matches[1].title.text)
    print(results[0].matches[1].scores)
    assert results[0].matches[0].id == '0'
    assert results[0].matches[1].id == '1'
