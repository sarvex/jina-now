from now.apps.text_to_text_and_image.app import TextToTextAndImage
from now.dialog import configure_option
from now.now_dataclasses import UserInput


def test_text_to_text_and_image_options(mocker, get_nest_config_path):
    mocker.patch('now.utils._prompt_value', return_value=get_nest_config_path)

    app = TextToTextAndImage()
    config_option = app.options[0]
    user_input = UserInput()

    configure_option(option=config_option, user_input=user_input)

    assert user_input.task_config
    assert user_input.task_config.name == 'online-shop-multi-modal-search'
    assert len(user_input.task_config.encoders) == 2
    assert user_input.task_config.data == 'extracted-data-online-shop-50-flat'
