import random
from collections import namedtuple
from typing import List

import numpy as np
import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Image, Text

from now.executor.indexer.elastic.elastic_indexer import FieldEmbedding
from now.executor.indexer.elastic.es_query_builder import SemanticScore


@pytest.fixture
def random_index_name():
    return f"test-index-{random.randint(0, 10000)}"


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
        SemanticScore('query_text', 'title', 'clip', 1),
        SemanticScore('query_text', 'gif', 'clip', 1),
        SemanticScore('query_text', 'title', 'sbert', 1),
        SemanticScore('query_text', 'excerpt', 'sbert', 3),
        SemanticScore('query_text', 'my_bm25_query', 'bm25', 1),
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

        clip_docs.append(clip_doc)
        sbert_docs.append(sbert_doc)

    index_docs_map = {
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
