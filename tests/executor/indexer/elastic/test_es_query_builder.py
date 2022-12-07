from now.executor.indexer.elastic.es_query_builder import ESQueryBuilder, SemanticScore


def test_generate_semantic_scores(es_inputs):
    """
    This test tests the generate_semantic_scores function of the ESQueryBuilder.
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
