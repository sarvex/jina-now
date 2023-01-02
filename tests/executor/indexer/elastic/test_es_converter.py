import numpy as np
from docarray import Document
from docarray.score import NamedScore

from now.executor.indexer.elastic.elastic_indexer import aggregate_embeddings
from now.executor.indexer.elastic.es_converter import (
    calculate_score_breakdown,
    convert_doc_map_to_es,
)
from now.executor.indexer.elastic.es_preprocessing import merge_subdocuments


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
    ) = es_inputs
    encoder_to_fields = {
        document_mapping[0]: document_mapping[2]
        for document_mapping in document_mappings
    }
    first_doc_clip = index_docs_map['clip'][0]
    first_doc_sbert = index_docs_map['sbert'][0]
    aggregate_embeddings(index_docs_map)
    processed_docs_map = merge_subdocuments(index_docs_map, encoder_to_fields)
    first_result = convert_doc_map_to_es(
        docs_map=processed_docs_map,
        index_name=random_index_name,
        encoder_to_fields=encoder_to_fields,
    )[0]
    assert first_result['id'] == first_doc_clip.id
    assert len(first_result['title-clip.embedding']) == len(
        first_doc_clip.title.chunks[0].embedding.tolist()
    )
    assert len(first_result['title-sbert.embedding']) == len(
        first_doc_sbert.title.chunks[0].embedding.tolist()
    )
    assert len(first_result['excerpt-sbert.embedding']) == len(
        first_doc_sbert.excerpt.chunks[0].embedding.tolist()
    )
    assert (
        first_result['bm25_text']
        == first_doc_clip.title.chunks[0].text
        + ' '
        + first_doc_sbert.title.chunks[0].text
        + ' '
        + first_doc_sbert.excerpt.chunks[0].text
        + ' '
    )
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
    doc_score_breakdown = calculate_score_breakdown(
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
