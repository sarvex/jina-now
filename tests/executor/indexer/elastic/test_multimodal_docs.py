import random
from collections import namedtuple

import numpy as np
import pytest
from docarray import dataclass
from docarray.typing import Text
from jina import Document, DocumentArray

from now.executor.indexer.elastic.elastic_indexer import (
    ElasticIndexer,
    FieldEmbedding,
    SemanticScore,
)


def random_index_name():
    return f"test-index-{random.randint(0, 10000)}"


def test_generate_es_mappings():
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

    @dataclass
    class MMQuery:
        query_text: Text

    document_mappings = [
        FieldEmbedding('clip', 8, ['title']),
        FieldEmbedding('sbert', 5, ['title', 'excerpt']),
    ]

    default_semantic_scores = [
        SemanticScore('query_text', 'clip', 'title', 'clip', 1),
        SemanticScore('query_text', 'sbert', 'title', 'sbert', 1),
        SemanticScore('query_text', 'sbert', 'excerpt', 'sbert', 3),
        SemanticScore('query_text', 'bm25', 'my_bm25_query', 'bm25', 1),
    ]
    docs = [
        MMDoc(title='test title', excerpt='test excerpt'),
        MMDoc(title='test title 2', excerpt='test excerpt 2'),
    ]
    clip_docs = DocumentArray()
    sbert_docs = DocumentArray()
    # encode our documents
    for doc in docs:
        clip_doc = Document(doc)
        sbert_doc = Document(clip_doc, copy=True)
        sbert_doc.id = clip_doc.id

        clip_doc.title.embedding = np.random.random(8)
        sbert_doc.title.embedding = np.random.random(5)
        sbert_doc.excerpt.embedding = np.random.random(5)

        clip_docs.append(clip_doc)
        sbert_docs.append(sbert_doc)

    index_docs_map = {
        'clip': clip_docs,
        'sbert': sbert_docs,
    }

    query = MMQuery(query_text='bla')

    clip_doc = Document(query)
    sbert_doc = Document(clip_doc, copy=True)
    sbert_doc.id = clip_doc.id

    clip_doc.query_text.embedding = np.random.random(8)
    sbert_doc.query_text.embedding = np.random.random(5)

    query_docs_map = {
        'clip': DocumentArray([clip_doc]),
        'sbert': DocumentArray([sbert_doc]),
    }
    EsInputs = namedtuple(
        'EsInputs',
        [
            'index_docs_map',
            'query_docs_map',
            'query',
            'document_mappings',
            'default_semantic_scores',
        ],
    )
    return EsInputs(
        index_docs_map,
        query_docs_map,
        query,
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
        query,
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
    first_doc_clip = index_docs_map['clip'][0]
    first_doc_sbert = index_docs_map['sbert'][0]
    first_result = es_indexer._doc_map_to_es(docs_map=index_docs_map)[0]
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


def test_index_and_search_with_multimodal_docs(es_inputs):
    """
    This test runs indexing with the ElasticIndexer using multimodal docs.

    TODO:
    - score explanation
      -
    - recreate the MMDoc
    - only index correct docs
    """
    (
        index_docs_map,
        query_docs_map,
        query,
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
    results = indexer.search(query_docs_map)

    # add asserts about results


def test_list_endpoint(setup_service_running, es_inputs):
    """
    This test tests the list endpoint of the ElasticIndexer.
    """
    (
        index_docs_map,
        query_docs_map,
        query,
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
