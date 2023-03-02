import os

import numpy as np
from docarray import Document
from docarray.score import NamedScore

from now.executor.indexer.elastic.elastic_indexer import aggregate_embeddings
from now.executor.indexer.elastic.es_converter import (
    calculate_score_breakdown,
    convert_doc_map_to_es,
)


def test_convert_doc_map_to_es(es_inputs, random_index_name):
    """
    This test should check whether the docs_map is correctly
    transformed to the expected format.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
        _,
    ) = es_inputs
    document_mappings = document_mappings[0]
    encoder_to_fields = {document_mappings[0]: document_mappings[2]}
    first_doc_clip = index_docs_map['clip'][0]
    aggregate_embeddings(index_docs_map)
    index_name = os.getenv('ES_INDEX_NAME')
    first_result = convert_doc_map_to_es(
        docs_map=index_docs_map,
        index_name=index_name,
        encoder_to_fields=encoder_to_fields,
    )[0]
    assert first_result['id'] == first_doc_clip.id
    assert first_result['bm25_text'] == first_doc_clip.title.text + ' '
    assert first_result['_op_type'] == 'index'


def test_calculate_score_breakdown(es_inputs):
    """
    This test tests the calculate_score_breakdown function.
    """
    default_semantic_scores = es_inputs.default_semantic_scores
    metric = 'cosine'
    query_doc = Document(
        tags={
            'embeddings': {
                'query_text-clip': np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
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
            }
        },
        scores={metric: NamedScore(value=5.0)},
    )
    doc_score_breakdown = calculate_score_breakdown(
        query_doc=query_doc,
        retrieved_doc=retrieved_doc,
        metric=metric,
        score_calculation=default_semantic_scores,
    )
    scores = {
        'total': {'value': 5.0},
        'query_text-title-clip-1': {'value': 0.921791},
        'query_text-gif-clip-1': {'value': 0.921791},
        'bm25_normalized': {'value': 2.156418},
        'bm25_raw': {'value': 21.56418},
    }
    for score, val in scores.items():
        assert doc_score_breakdown.scores[score].value == val['value']
