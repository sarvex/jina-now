"""
Test the dialog.py module.

Patches the `prompt` method to mock user input via the command line.
"""
from typing import Dict

import pytest
from pytest_mock import MockerFixture

from now.constants import Apps, DemoDatasets
from now.dialog import configure_user_input
from now.now_dataclasses import UserInput


class CmdPromptMock:
    def __init__(self, predefined_answers: Dict[str, str]):
        self._answers = predefined_answers

    def __call__(self, question: Dict):
        return {question['name']: self._answers[question['name']]}


MOCKED_DIALOGS_WITH_CONFIGS = [
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'data': 'music-genres-mid',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'data': 'music-genres-mix',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.TEXT_TO_IMAGE,
            'data': 'tll',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.TEXT_TO_IMAGE,
            'data': 'nih-chest-xrays',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.TEXT_TO_IMAGE,
            'data': 'custom',
            'custom_dataset_type': 'docarray',
            'dataset_name': 'xxx',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'data': 'custom',
            'custom_dataset_type': 'docarray',
            'dataset_name': 'xxx',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'data': 'custom',
            'custom_dataset_type': 'path',
            'dataset_path': 'xxx',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'data': 'custom',
            'custom_dataset_type': 'url',
            'dataset_url': 'xxx',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'app': Apps.TEXT_TO_IMAGE,
            'data': 'custom',
            'custom_dataset_type': 'docarray',
            'dataset_name': 'xxx',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
        UserInput(),
    ),
    (
        {
            'data': 'music-genres-mid',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {'app': Apps.MUSIC_TO_MUSIC},
        UserInput(),
    ),
    (
        {
            'data': DemoDatasets.TLL,
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {'app': Apps.TEXT_TO_IMAGE},
        UserInput(),
    ),
    (
        {
            'data': DemoDatasets.ROCK_LYRICS,
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {'app': Apps.IMAGE_TO_TEXT},
        UserInput(),
    ),
    (
        {
            'app': Apps.IMAGE_TO_TEXT,
        },
        {
            'data': DemoDatasets.POP_LYRICS,
            'cluster': 'new',
            'deployment_type': 'local',
        },
        UserInput(),
    ),
]


@pytest.mark.parametrize(
    ('mocked_user_answers', 'configure_kwargs', 'expected_user_input'),
    MOCKED_DIALOGS_WITH_CONFIGS,
)
def test_configure_user_input(
    mocker: MockerFixture,
    mocked_user_answers: Dict[str, str],
    configure_kwargs: Dict,
    expected_user_input: UserInput,
):
    expected_user_input.__dict__.update(mocked_user_answers)
    expected_user_input.__dict__.update(configure_kwargs)
    mocker.patch('now.utils.prompt', CmdPromptMock(mocked_user_answers))

    _, user_input = configure_user_input(**configure_kwargs)

    if user_input.deployment_type == 'remote':
        user_input.__dict__.update({'jwt': None, 'admin_emails': None})

    user_input.__dict__.update({'app_instance': None})

    assert user_input == expected_user_input
