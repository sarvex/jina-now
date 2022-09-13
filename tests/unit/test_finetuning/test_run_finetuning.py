from now.apps.text_to_text_and_image.app import TextToTextAndImage
from now.constants import ModelNames
from now.finetuning.run_finetuning import _get_model_options
from now.finetuning.settings import FinetuneSettings
import pytest
from docarray import DocumentArray
from now.now_dataclasses import UserInput


def test_get_model_options():
    settings = FinetuneSettings(
        model_name='mlp',
        add_embeddings=True,
        loss='TripletMarginLoss',
        pre_trained_embedding_size=512,
        perform_finetuning=True,
        bi_modal=True,
    )
    input_size = settings.pre_trained_embedding_size * 2
    expected_options = {
        'input_size': input_size,
        'hidden_sizes': settings.hidden_sizes,
        'l2': True,
        'bias': False if settings.bi_modal else True,
    }
    options = _get_model_options(settings)
    assert expected_options == options
    settings.model_name = 'not_mlp'
    expected_options = {}
    options = _get_model_options(settings)
    assert expected_options == options


@pytest.mark.parametrize('encoder_type', ['text-to-text', 'text-to-image'])
def test_nest_construct_finetune_settings(encoder_type, get_nest_config_path):
    user_input = UserInput()
    app = TextToTextAndImage()
    user_input.app_instance = app
    app._read_task_config(task_config_path=get_task_config_path, user_input=user_input)
    dataset = DocumentArray().empty(2)

    settings = app._construct_finetune_settings(
        user_input=user_input, dataset=dataset, encoder_type=encoder_type
    )

    assert settings.perform_finetuning
    if encoder_type == 'text-to-text':
        assert settings.model_name == ModelNames.SBERT
        assert settings.loss == 'TripletMarginLoss'
    else:
        assert settings.model_name == ModelNames.CLIP
        assert settings.loss == 'CLIPLoss'
    assert not settings.add_embeddings
    assert settings.epochs == 2
