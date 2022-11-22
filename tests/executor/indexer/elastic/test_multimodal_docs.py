import random

import numpy as np
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


def test_doc_map_to_es():
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


def test_index_with_multimodal_docs(setup_service_running):
    """
    This test runs indexing with the ElasticIndexer using multimodal docs.
    """

    @dataclass
    class MMDoc:
        title: Text
        excerpt: Text

    @dataclass
    class MMQuery:
        text: Text
        image: Image

    document_mappings = [
        FieldEmbedding('clip', 8, ['title']),
        FieldEmbedding('sbert', 5, ['title', 'excerpt']),
    ]

    default_semantic_scores = [
        SemanticScore('text', 'clip', 'title', 'clip', 1),
        SemanticScore('text', 'sbert', 'title', 'sbert', 1),
    ]

    semantic_scores = [
        SemanticScore('text', 'clip', 'title', 'clip', 1),
        SemanticScore('text', 'sbert', 'title', 'sbert', 3),
    ]

    # default should be: all combinations ?? TODO: clarify if that is true
    index_name = random_index_name()
    indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        hosts='http://localhost:9200',
        index_name=index_name,
    )
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

    indexer.index(docs_map)
    # check if single document is indexed
    es = Elasticsearch(hosts='http://localhost:9200')
    res = es.search(index=index_name, size=100, query={'match_all': {}})
    assert len(res['hits']['hits']) == 1
