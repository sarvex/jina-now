import random
from collections import namedtuple
import numpy as np

from jina import Document, DocumentArray
from docarray.typing import Image, Text, List
from docarray import dataclass

from now.executor.indexer.elastic.elastic_indexer import (
    ElasticIndexer,
    FieldEmbedding,
    SemanticScore,
)


def random_index_name():
    return f"test-index-{random.randint(0,10000)}"


def test_generate_es_mappings():
    """This test should check, whether the static generate_es_mappings method works as expected.
    TODO: Fill it
    """


def test_with_multimodal_docs(setup_service_running):
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

    # perhaps remove query_mappings
    # query_mappings = {'text__clip': 8, 'text_sbert': 5, 'image__clip': 786}

    default_semantic_scores = [
        SemanticScore('text', 'clip', 'title', 'clip', 1),
        SemanticScore('text', 'sbert', 'title', 'sbert', 1),
    ]

    semantic_scores = [
        SemanticScore('text', 'clip', 'title', 'clip', 1),
        SemanticScore('text', 'sbert', 'title', 'sbert', 3),
    ]

    # default should be: all combinations ?? TODO: clarify if that is true

    indexer = ElasticIndexer(
        traversal_paths='c',
        document_mappings=document_mappings,
        default_semantic_scores=default_semantic_scores,
        hosts='http://localhost:9200',
        index_name=random_index_name(),
    )
    doc = MMDoc(title='bla', excerpt='excer')
    clip_doc = Document(doc)
    sbert_doc = Document(clip_doc, copy=True)
    sbert_doc.id = clip_doc.id

    clip_doc.title.embedding = np.random.random((1, 8))
    sbert_doc.title.embedding = np.random.random((1, 5))
    sbert_doc.excerpt.embedding = np.random.random((1, 5))

    docs_map = {
        'clip': DocumentArray([clip_doc]),
        'sbert': DocumentArray([sbert_doc]),
    }

    indexer.index(docs_map)
    assert False
    results = indexer.search(
        MMQuery(query_text='I want to know'), parameters={'scoring': semantic_scores}
    )

    assert results[0].title == 'my fancy stuff'
    assert isinstance(results[0], MMDoc)


"""
tomorrow:
- make test runnable
- enable index setup in ES indexer
- enable indexing
    - refactor how docs are translated into ES docs
- enable searching

"""
