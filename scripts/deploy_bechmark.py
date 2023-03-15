import argparse
import os
from argparse import Namespace

from now.cli import cli
from now.constants import Apps, DatasetTypes, Models

os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'


def deploy_scenario(scenario):
    kwargs_general = {
        'now': 'start',
        'app': Apps.SEARCH_APP,
        'secured': True,
        'additional_user': True,
        'user_emails': [],
    }

    if scenario == 'video+text':
        kwargs_scenario = {
            'dataset_type': DatasetTypes.S3_BUCKET,
            'dataset_path': '',
            'aws_access_key_id': '',
            'aws_secret_access_key': '',
            'aws_region_name': '',
            'index_fields': ['file.gif', 'title'],
            'filter_fields': ['title'],
            'model_choices': {
                'file.gif_model': [Models.CLIP_MODEL],
                'title_model': [Models.CLIP_MODEL, Models.SBERT_MODEL],
            },
        }
    elif scenario == 'image':
        kwargs_scenario = {
            'dataset_type': DatasetTypes.DOCARRAY,
            'dataset_name': 'laion',
            'index_fields': ['image'],
            'filter_fields': [],
            'model_choices': {
                'image_model': [Models.CLIP_MODEL],
            },
        }
    kwargs = Namespace(**{**kwargs_general, **kwargs_scenario})
    response = cli(args=kwargs)

    # benchmark query time

    # write to Grafana

    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', type=int, choices=['video+text', 'image'])
    args = parser.parse_args()

    result = deploy_scenario(args.scenario)
