from now.data_loading.utils import transform_es_doc


def test_extraction(setup_extractor):
    extractor, index_name = setup_extractor
    for doc in extractor:
        trans = transform_es_doc(doc)
        for chunk in trans.chunks:
            print(chunk.tags['field_name'])
            print(chunk.content, chunk.text, chunk.uri, chunk.modality)
        break
