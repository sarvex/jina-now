"""
Test the dialog.py module.

Patches the `prompt` method to mock user input via the command line.
"""
import os
from typing import Dict

import pytest
from pytest_mock import MockerFixture

from now.constants import DEFAULT_FLOW_NAME, Apps, DatasetTypes
from now.demo_data import DemoDatasetNames
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
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': 'totally-looks-like',
            'search_fields_modalities': {'label': 'Text', 'image': 'MyImage'},
            'search_fields': ['label'],
            # No filter fields for this particular dataset
            'filter_fields': [],
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'search_fields_modalities': {'label': 'Text', 'image': 'MyImage'},
            'search_fields': ['image'],
            'filter_fields': [],
            'filter_fields_modalities': {'label': 'Text'},
            'dataset_name': 'nih-chest-xrays',
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.PATH,
            'dataset_path': os.path.join(
                os.path.dirname(__file__), '..', 'resources', 'image'
            ),
            'search_fields': ['.jpg'],
            'search_fields_modalities': {'.jpg': 'image'},
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {},
    ),
    (
        {
            'flow_name': DEFAULT_FLOW_NAME,
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': DemoDatasetNames.TLL,
            'search_fields': ['x', 'y'],
            'search_fields_modalities': {'label': 'Text', 'image': 'MyImage'},
            # No filter fields for this particular dataset
            'filter_fields': [],
            'cluster': 'new',
            'deployment_type': 'local',
        },
        {'app': Apps.IMAGE_TEXT_RETRIEVAL},
    ),
    (
        {},
        {
            'app': Apps.IMAGE_TEXT_RETRIEVAL,
            'flow_name': 'testapp',
            'dataset_type': DatasetTypes.DEMO,
            'dataset_name': DemoDatasetNames.MUSIC_GENRES_MIX,
            'search_fields': ['audio', 'artist'],
            'search_fields_modalities': {
                'audio': 'MyAudio',
                'artist': 'Text',
                'title': 'Text',
                'genre_tags': 'List[Text]',
            },
            'filter_fields': ['title'],
            'filter_fields_modalities': {
                'artist': 'Text',
                'title': 'Text',
                'album_cover_image_url': 'stringValue',
                'location': 'numberValue',
                'sr': 'numberValue',
                'track_id': 'stringValue',
            },
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
    # expected user input
    expected_user_input = UserInput()
    expected_user_input.__dict__.update(mocked_user_answers)
    expected_user_input.__dict__.update(configure_kwargs)
    expected_user_input.__dict__.pop('app')

    # mocked user input
    mocker.patch('now.utils.prompt', CmdPromptMock(mocked_user_answers))
    user_input = configure_user_input(**configure_kwargs)
    user_input.__dict__.update({'jwt': None, 'admin_emails': None})
    user_input.__dict__.update({'app_instance': None})

    assert user_input == expected_user_input
