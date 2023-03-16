import argparse
import json
import os
from argparse import Namespace

import hubble
from integration.conftest import get_flow_id_from_name
from integration.remote.assertions import assert_indexed_all_docs

from now.admin.benchmark_flow import benchmark_deployment
from now.cli import cli
from now.constants import MAX_DOCS_FOR_BENCHMARKING, Apps, DatasetTypes, Models
from now.deployment.deployment import terminate_wolf
from now.log.log import TIME_PROFILER_RESULTS

os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'


def deploy_scenario(scenario):
    os.environ['NOW_BENCHMARK_RUN'] = 'True'

    flow_name = f'benchmark-{scenario.replace("+", "-")}'
    kwargs_general = {
        'now': 'start',
        'flow_name': flow_name,
        'app': Apps.SEARCH_APP,
        'secured': True,
        'additional_user': False,
        'api_key': None,
    }

    if scenario == 'video+text':
        kwargs_scenario = {
            'dataset_type': DatasetTypes.S3_BUCKET,
            'dataset_path': os.environ['CUSTOM_S3_BUCKET'],
            'aws_access_key_id': os.environ['CUSTOM_AWS_ACCESS_KEY_ID'],
            'aws_secret_access_key': os.environ['CUSTOM_AWS_SECRET_ACCESS_KEY'],
            'aws_region_name': os.environ['CUSTOM_AWS_REGION_NAME'],
            'index_fields': ['file.gif', 'title'],
            'filter_fields': ['title'],
            'file.gif_model': [Models.CLIP_MODEL],
            'title_model': [Models.CLIP_MODEL, Models.SBERT_MODEL],
        }
    elif scenario == 'image':
        kwargs_scenario = {
            'dataset_type': DatasetTypes.PATH,
            'dataset_path': '/Users/joschkabraun/Downloads/laion400m_part_0.bin',
            'index_fields': ['image'],
            'filter_fields': [],
            'image_model': [Models.CLIP_MODEL],
        }
    kwargs = Namespace(**{**kwargs_general, **kwargs_scenario})
    response = cli(args=kwargs)

    assert_indexed_all_docs(
        response['host_http'], kwargs=kwargs, limit=MAX_DOCS_FOR_BENCHMARKING
    )

    # benchmark index time
    total_time_indexing = TIME_PROFILER_RESULTS['now.run_backend:call_flow']
    index_benchmark = {
        'indexing_total_time': total_time_indexing,
        'indexing_time_per_doc': total_time_indexing / MAX_DOCS_FOR_BENCHMARKING,
    }
    print(f'Indexing time:\n{json.dumps(index_benchmark, indent=4)}')

    # benchmark query time
    query_benchmark = benchmark_deployment(
        http_host=response['host_http'],
        search_text='this is a test',
        limit=30,
        jwt=hubble.get_token(),
    )

    print(f'Query time:\n{json.dumps(query_benchmark, indent=4)}')

    # delete flow
    terminate_wolf(get_flow_id_from_name(flow_name))

    # write to Grafana

    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', type=str, choices=['video+text', 'image'])
    args = parser.parse_args()

    result = deploy_scenario(args.scenario)
