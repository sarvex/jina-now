from docarray import Document


def test_extraction(setup_extractor):
    extractor, _ = setup_extractor
    transformed_docs = extractor.extract()
    assert len(transformed_docs) == 50
    assert isinstance(transformed_docs[0], Document)
    assert len(transformed_docs[0].chunks) == 3
    assert sorted(
        [doc.tags['field_name'] for doc in transformed_docs[0].chunks]
    ) == sorted(['title', 'text', 'uris'])
