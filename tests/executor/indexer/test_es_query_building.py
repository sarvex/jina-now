from now.executor.indexer.elastic.elastic_indexer import aggregate_embeddings
from now.executor.indexer.elastic.es_query_building import (
    build_es_queries,
    generate_score_calculation,
)


def test_generate_score_calculation(es_inputs):
    """
    This test tests the generate_score_calculation function from es_query_building.
    It should return a list of score calculation, with comparisons between
    all query field + search field combinations that are in the same vector space (same encoder)
    and assign the default linear weight of 1.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        _,
    ) = es_inputs
    document_mappings = document_mappings[0]
    encoder_to_fields = {document_mappings[0]: document_mappings[2]}
    default_score_calculation = default_score_calculation[
        :-1
    ]  # omit the last score calculation, contains bm25
    score_calculation = generate_score_calculation(query_docs_map, encoder_to_fields)
    assert score_calculation == default_score_calculation


def test_build_es_queries(es_inputs):
    """
    This test tests the build_es_queries function es_query_building.
    It should return a list of ES queries, with cosine comparisons between
    all query-doc field pairs that are in the same vector space (same encoder)
    and assign the same linear weight of 1.
    """
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_score_calculation,
        _,
    ) = es_inputs
    aggregate_embeddings(query_docs_map)
    _, es_query = build_es_queries(
        docs_map=query_docs_map,
        apply_default_bm25=True,
        get_score_breakdown=False,
        score_calculation=default_score_calculation,
    )[0]
    print(es_query)
    assert es_query == {
        'script_score': {
            'query': {
                'bool': {
                    'should': [
                        {'match_all': {}},
                        {'multi_match': {'query': 'cat', 'fields': ['bm25_text']}},
                    ]
                }
            },
            'script': {
                'source': "1.0 + _score / (_score + 10.0) + 1.0*cosineSimilarity(params.query_query_text_clip, 'title-clip.embedding') + 1.0*cosineSimilarity(params.query_query_text_clip, 'gif-clip.embedding')",
                'params': {
                    'query_query_text_clip': query_docs_map['clip'][
                        0
                    ].query_text.embedding,
                },
            },
        }
    }
