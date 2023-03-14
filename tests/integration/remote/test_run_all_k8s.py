from argparse import Namespace

import pytest

from now.cli import cli
from now.constants import DatasetTypes, Models
from now.demo_data import DemoDatasetNames


@pytest.mark.parametrize(
    'query_fields, index_fields, filter_fields, model_selection, dataset',
    [
        # needs to be put back once wolf can handle it
        # (
        #     'image',
        #     ['image', 'label'],
        #     [],
        #     {
        #         'image_model': [Models.CLIP_MODEL],
        #         'label_model': [Models.CLIP_MODEL, Models.SBERT_MODEL],
        #     },
        #     DemoDatasetNames.BIRD_SPECIES,
        # ),
        (
            'text',
            ['video', 'description'],
            [],
            {
                'video_model': [Models.CLIP_MODEL],
                'description_model': [Models.CLIP_MODEL],
            },
            DemoDatasetNames.TUMBLR_GIFS_10K,
        ),
    ],
)
def test_flow_logs(
    cleanup,
    random_flow_name,
    query_fields,
    index_fields,
    filter_fields,
    model_selection,
    dataset,
):
    kwargs = {
        'now': 'logs',
        'flow_name': random_flow_name,
        'dataset_type': DatasetTypes.DEMO,
        'admin_name': 'team-now',
        'index_fields': index_fields,
        'filter_fields': filter_fields,
        'dataset_name': dataset,
        'secured': True,
        'api_key': None,
        'additional_user': False,
    }
    kwargs.update(model_selection)
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    print('RESPONSE: ', response)
    assert len(response) > 0
