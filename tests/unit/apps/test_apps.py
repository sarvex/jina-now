from docarray import Document, DocumentArray

from now.common.options import construct_app
from now.constants import Apps, DatasetTypes
from now.now_dataclasses import UserInput


def test_app_attributes():
    """Test if all essential app attributes are defined"""
    for app in Apps():
        app_instance = construct_app(app)
        if app_instance.is_enabled:
            assert app_instance.app_name
            assert app_instance.description
            assert app_instance.input_modality
            assert app_instance.output_modality


def test_split_text_preprocessing(mocker):
    """Test if splitting of sentences is carried out when preprocessing text documents at indexing time"""
    mocked_preprocess = mocker.patch('now.common.preprocess.preprocess_text')
    from now.app.text_to_text.app import TextToText

    app = TextToText()
    da = DocumentArray([Document(text='test. test')])
    app.preprocess(da=da, user_input=UserInput(), is_indexing=True)
    mocked_preprocess.assert_called_with(da=da, split_by_sentences=True)


def test_split_text_preprocessing_demo(mocker):
    """Test if splitting of sentences is carried out when preprocessing text documents at indexing time"""
    mocked_preprocess = mocker.patch('now.common.preprocess.preprocess_text')
    from now.app.text_to_text.app import TextToText

    app = TextToText()
    da = DocumentArray([Document(text='test. test')])
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DEMO
    app.preprocess(da=da, user_input=user_input, is_indexing=True)
    mocked_preprocess.assert_called_with(da=da, split_by_sentences=False)
