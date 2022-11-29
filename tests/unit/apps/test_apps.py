from docarray import Document, DocumentArray

from now.app.image_text_retrieval.app import ImageTextRetrieval
from now.common.options import construct_app
from now.constants import Apps


def test_app_attributes():
    """Test if all essential app attributes are defined"""
    for app in Apps():
        app_instance = construct_app(app)
        if app_instance.is_enabled:
            assert app_instance.app_name
            assert app_instance.description
            assert app_instance.input_modality
            assert app_instance.output_modality


def test_split_text_preprocessing():
    """Test if splitting of sentences is carried out when preprocessing text documents at indexing time"""
    app = ImageTextRetrieval()
    da = DocumentArray(
        [Document(chunks=[Document(text='test. test', modality='text')])]
    )
    new_da = app.preprocess(da)
    assert len(new_da) == 1
    assert len(new_da[0].chunks) == 1
    assert len(new_da[0].chunks[0].chunks) == 2
