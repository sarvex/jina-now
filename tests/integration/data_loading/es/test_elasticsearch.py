from docarray import DocumentArray, Document

from now.data_loading.utils import transform_es_doc


def test_extraction(setup_extractor):
    extractor, _ = setup_extractor
    transformed_docs = DocumentArray([transform_es_doc(doc) for doc in extractor])
    assert len(transformed_docs) == 50
    assert isinstance(transformed_docs[0], Document)
    assert len(transformed_docs[0].chunks) == 3
    assert sorted(
        [doc.tags['field_name'] for doc in transformed_docs[0].chunks]
    ) == sorted(['title', 'text', 'uris'])
