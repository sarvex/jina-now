"""
Test the dialog.py module.

Patches the `prompt` method to mock user input via the command line.
"""
from typing import Dict

import pytest
from pytest_mock import MockerFixture

from now.constants import DEFAULT_FLOW_NAME, Apps, DatasetTypes
from now.demo_data import DemoDatasetNames
from now.dialog import configure_app, configure_user_input
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
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': 'music-genres-mid',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': 'music-genres-mix',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'output_modality': 'text',
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': 'tll',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'output_modality': 'image',
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': 'nih-chest-xrays',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'output_modality': 'image',
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DOCARRAY,
            'dataset_name': 'xxx',
            'search_fields': 'x, y',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DOCARRAY,
            'dataset_name': 'xxx',
            'search_fields': 'x, y',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.PATH,
            'dataset_path': 'xxx',
            'search_fields': 'x, y',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.MUSIC_TO_MUSIC,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.URL,
            'dataset_url': 'xxx',
            'search_fields': 'x, y',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'output_modality': 'image',
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DOCARRAY,
            'dataset_name': 'xxx',
            'search_fields': 'x, y',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'dataset_type': DatasetTypes.DEMO,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_name': 'music-genres-mid',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {'app': Apps.MUSIC_TO_MUSIC},
    ),
    (
        {
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': DemoDatasetNames.TLL,
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {'app': Apps.IMAGE_TEXT_RETRIEVAL, 'output_modality': 'text'},
    ),
    (
        {
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': DemoDatasetNames.TLL,
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {'app': Apps.IMAGE_TEXT_RETRIEVAL, 'output_modality': 'text'},
    ),
    (
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'output_modality': 'text',
        },
        {
            'flow_name': 'testapp',
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': DemoDatasetNames.BEST_ARTWORKS,
            'cluster': 'new',
            'deployment_type': 'local',
        },
    ),
]


@pytest.mark.parametrize(
    ('mocked_user_answers', 'configure_kwargs'),
    MOCKED_DIALOGS_WITH_CONFIGS,
)
def test_configure_user_input(
    mocker: MockerFixture,
    mocked_user_answers: Dict[str, str],
    configure_kwargs: Dict,
):
    expected_user_input = UserInput()
    expected_user_input.__dict__.update(mocked_user_answers)
    expected_user_input.__dict__.update(configure_kwargs)
    expected_user_input.__dict__.pop('app')
    mocker.patch('now.utils.prompt', CmdPromptMock(mocked_user_answers))

    app_instance = configure_app(**configure_kwargs)
    user_input = configure_user_input(app_instance=app_instance, **configure_kwargs)

    if user_input.deployment_type == 'remote':
        user_input.__dict__.update({'jwt': None, 'admin_emails': None})

    user_input.__dict__.update({'app_instance': None})
    if expected_user_input.dataset_type != DatasetTypes.DEMO:
        expected_user_input.search_fields = ['x', 'y']

    assert user_input == expected_user_input
