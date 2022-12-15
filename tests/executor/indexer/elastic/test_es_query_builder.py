from now.executor.indexer.elastic.es_query_builder import ESQueryBuilder, SemanticScore


def test_generate_semantic_scores(es_inputs):
    """
    This test tests the generate_semantic_scores function of the ESQueryBuilder.
    It should return a list of SemanticScores, with cosine comparisons between
    all query-doc field pairs that are in the same vector space (same encoder)
    and assign the same linear weight of 1.
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
    default_semantic_scores = [
        SemanticScore('query_text', 'title', 'clip', 1),
        SemanticScore('query_text', 'gif', 'clip', 1),
        SemanticScore('query_text', 'title', 'sbert', 1),
        SemanticScore('query_text', 'excerpt', 'sbert', 1),
    ]
    semantic_scores = ESQueryBuilder.generate_semantic_scores(
        query_docs_map, encoder_to_fields
    )
    assert semantic_scores == default_semantic_scores


def test_build_es_queries(es_inputs):
    """
    This test tests the build_es_queries function of the ESQueryBuilder.
    It should return a list of ES queries, with cosine comparisons between
    all query-doc field pairs that are in the same vector space (same encoder)
    and assign the same linear weight of 1.
    """
    query_builder = ESQueryBuilder()
    (
        index_docs_map,
        query_docs_map,
        document_mappings,
        default_semantic_scores,
    ) = es_inputs
    query_doc, es_query = query_builder.build_es_queries(
        docs_map=query_docs_map,
        apply_default_bm25=True,
        get_score_breakdown=False,
        semantic_scores=default_semantic_scores,
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
                'source': "1.0 + _score / (_score + 10.0) + 1.0*cosineSimilarity(params.query_query_text_clip, 'title-clip.embedding') + 1.0*cosineSimilarity(params.query_query_text_clip, 'gif-clip.embedding') + 1.0*cosineSimilarity(params.query_query_text_sbert, 'title-sbert.embedding') + 3.0*cosineSimilarity(params.query_query_text_sbert, 'excerpt-sbert.embedding')",
                'params': {
                    'query_query_text_clip': query_docs_map['clip'][
                        0
                    ].query_text.embedding,
                    'query_query_text_sbert': query_docs_map['sbert'][
                        0
                    ].query_text.embedding,
                },
            },
        }
    }
