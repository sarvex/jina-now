import random

import numpy as np
import os
from docarray import dataclass
from docarray.typing import Image, Text
from elasticsearch import Elasticsearch
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


def test_doc_map_to_es(setup_service_running):
    """
    This test should check whether the docs_map is correctly
    transformed to the expected format.
    """

    @dataclass
    class MMDoc:
        title: Text
        excerpt: Text

    doc = MMDoc(title='bla', excerpt='excer')
    clip_doc = Document(doc)
    sbert_doc = Document(clip_doc, copy=True)
    sbert_doc.id = clip_doc.id

    clip_doc.title.embedding = np.random.random(8)
    sbert_doc.title.embedding = np.random.random(5)
    sbert_doc.excerpt.embedding = np.random.random(5)

    docs_map = {
        'clip': DocumentArray([clip_doc]),
        'sbert': DocumentArray([sbert_doc]),
    }
    document_mappings = [
        FieldEmbedding('clip', 8, ['title']),
        FieldEmbedding('sbert', 5, ['title', 'excerpt']),
    ]

    default_semantic_scores = [
        SemanticScore('text', 'clip', 'title', 'clip', 1),
        SemanticScore('text', 'sbert', 'title', 'sbert', 1),
    ]
    index_name = random_index_name()
    es_indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        hosts='http://localhost:9200',
        index_name=index_name,
    )
    result = es_indexer._doc_map_to_es(docs_map=docs_map)[0]
    assert result['id'] == clip_doc.id
    assert len(result['title-clip.embedding']) == len(clip_doc.title.embedding.tolist())
    assert len(result['title-sbert.embedding']) == len(
        sbert_doc.title.embedding.tolist()
    )
    assert len(result['excerpt-sbert.embedding']) == len(
        sbert_doc.excerpt.embedding.tolist()
    )
    assert (
        result['bm25_text']
        == clip_doc.title.text
        + ' '
        + sbert_doc.title.text
        + ' '
        + sbert_doc.excerpt.text
        + ' '
    )
    assert result['_op_type'] == 'index'


def test_index_with_multimodal_docs():
    """
    This test runs indexing with the ElasticIndexer using multimodal docs.

    TODO:
    - score explanation
      -
    - recreate the MMDoc
    - only index correct docs
    """

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

    # default should be: all combinations ?? TODO: clarify if that is true
    index_name = random_index_name()

    indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        es_config={'api_key': os.environ['ELASTIC_API_KEY']},
        hosts='https://5280f8303ccc410295d02bbb1f3726f7.eu-central-1.aws.cloud.es.io:443',
        # hosts='http://localhost:9200',
        index_name=index_name,
        document_structure='MMDoc()',
    )
    doc = MMDoc(title='bla', excerpt='excer')
    clip_doc = Document(doc)

    clip_doc.tags['bm25_text'] = 'my text'
    doc_id = clip_doc.id
    sbert_doc = Document(clip_doc, copy=True)
    sbert_doc.id = clip_doc.id

    clip_doc.title.embedding = np.random.random(8)
    sbert_doc.title.embedding = np.random.random(5)
    sbert_doc.excerpt.embedding = np.random.random(5)

    index_docs_map = {
        'clip': DocumentArray([clip_doc]),
        'sbert': DocumentArray([sbert_doc]),
    }

    indexer.index(index_docs_map)
    # check if single document is indexed
    es = indexer.es
    res = es.search(index=index_name, size=100, query={'match_all': {}})
    assert len(res['hits']['hits']) == 1

    query = MMQuery(query_text='bla')

    clip_doc = Document(query)
    clip_doc.text = 'my search term'
    sbert_doc = Document(clip_doc, copy=True)
    sbert_doc.id = clip_doc.id

    clip_doc.query_text.embedding = np.random.random(8)
    sbert_doc.query_text.embedding = np.random.random(5)

    query_docs_map = {
        'clip': DocumentArray([clip_doc]),
        'sbert': DocumentArray([sbert_doc]),
    }

    print(indexer._build_es_queries(docs_map=query_docs_map, apply_bm25=True))
    results = indexer.search(query_docs_map)
    print(results[0].matches[0].tags)
    assert results[0].matches[0].id == doc_id
