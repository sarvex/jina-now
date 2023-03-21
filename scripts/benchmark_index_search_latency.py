import argparse
import itertools
import json
import os
from argparse import Namespace

import hubble
import pandas as pd
import requests
from matplotlib import pyplot as plt
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tests.integration.conftest import get_flow_id_from_name

from now.admin.benchmark_flow import (
    benchmark_deployment_latency,
    benchmark_deployment_qps_ps,
)
from now.admin.utils import get_default_request_body
from now.cli import cli
from now.constants import MAX_DOCS_FOR_BENCHMARKING, Apps, DatasetTypes, Models
from now.deployment.deployment import terminate_wolf
from now.log.log import TIME_PROFILER_RESULTS

os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'


def deploy_scenario(scenario):
    os.environ['NOW_BENCHMARK_RUN'] = 'True'

    limit = 30
    search_text = 'this is a test'
    payload_slack_n_latency_calls = 10
    payload_slack_n_qps_calls = 100
    payload_slack_n_qps_workers = 10

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
            'dataset_path': os.getenv('CUSTOM_S3_BUCKET'),
            'aws_access_key_id': os.getenv('CUSTOM_AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('CUSTOM_AWS_SECRET_ACCESS_KEY'),
            'aws_region_name': os.getenv('CUSTOM_AWS_REGION_NAME'),
            'index_fields': ['file.gif', 'title'],
            'filter_fields': ['title'],
            'file.gif_model': [Models.CLIP_MODEL],
            'title_model': [Models.CLIP_MODEL, Models.SBERT_MODEL],
        }
    elif scenario == 'image':
        kwargs_scenario = {
            'dataset_type': DatasetTypes.DOCARRAY,
            'dataset_name': 'team-now/laion400m_part_0',
            'index_fields': ['image'],
            'filter_fields': [],
            'image_model': [Models.CLIP_MODEL],
        }
    kwargs = Namespace(**{**kwargs_general, **kwargs_scenario})
    response = cli(args=kwargs)

    # some files are compromised in 'video+text' dataset
    request_body = get_default_request_body(secured=kwargs.secured)
    request_body['limit'] = MAX_DOCS_FOR_BENCHMARKING
    count_response = requests.post(
        f"{response['host_http']}/api/v1/info/count",
        json=request_body,
    )
    num_docs_indexed = count_response.json()['number_of_docs']

    # benchmark index time
    total_time_indexing = TIME_PROFILER_RESULTS['now.run_backend:call_flow']
    slack_payload = {
        'indexing_total_time': total_time_indexing,
        'indexing_time_per_doc': total_time_indexing / MAX_DOCS_FOR_BENCHMARKING,
    }

    # benchmark query time: latency
    df_dicts_latency = []
    for n_latency_calls in [10, 30, 50]:
        latency = benchmark_deployment_latency(
            http_host=response['host_http'],
            search_text=search_text,
            limit=limit,
            jwt=hubble.get_token(),
            n_latency_calls=n_latency_calls,
        )
        df_dicts_latency.append(
            {
                'n_latency_calls': n_latency_calls,
                'latency': latency,
            }
        )
    df_latency = pd.DataFrame(df_dicts_latency)
    df_latency.to_csv(f'latency_{flow_name}.csv', index=False)
    slack_payload['latency'] = df_latency.loc[
        df_latency['n_latency_calls'] == payload_slack_n_latency_calls, 'latency'
    ].values[0]

    # benchmark query time: qps
    df_dicts_qps = []
    for n_qps_calls, n_qps_workers in itertools.product([100, 250, 500], [5, 10, 20]):
        qps_ps = benchmark_deployment_qps_ps(
            http_host=response['host_http'],
            search_text=search_text,
            limit=limit,
            jwt=hubble.get_token(),
            n_qps_calls=n_qps_calls,
            n_qps_workers=n_qps_workers,
        )
        df_dicts_qps.append(
            {
                'n_qps_calls': n_qps_calls,
                'n_qps_workers': n_qps_workers,
                **qps_ps,
            }
        )
    df_qps = pd.DataFrame(df_dicts_qps)
    df_qps.to_csv(f'qps_{flow_name}.csv', index=False)
    for metric in ['qps', 'p0', 'p50', 'p95', 'p99', 'p99.9']:
        slack_payload[metric] = df_qps.loc[
            (df_qps['n_qps_calls'] == payload_slack_n_qps_calls)
            & (df_qps['n_qps_workers'] == payload_slack_n_qps_workers),
            metric,
        ].values[0]
    # create plot for qps, p0, p50, p95, p99, p99.9 vs n_qps_calls and n_qps_workers
    fig, axes = plt.subplots(2, 3, figsize=(25, 10), sharex=True)
    fig.set_facecolor('white')

    def plot_ax(ax, attr):
        # Plot QPS vs n_qps_calls and n_qps_workers
        for n_workers, group in df_qps.groupby('n_qps_workers'):
            ax.plot(
                group['n_qps_calls'],
                group[attr],
                label=f'{n_workers} workers',
                marker='o',
                linestyle='-',
            )
        ax.set_ylabel(attr.upper())
        ax.set_title(f'{attr.upper()} vs Number of QPS Calls and Workers')
        ax.legend()
        ax.title.set_color('black')
        ax.xaxis.label.set_color('black')
        ax.yaxis.label.set_color('black')
        ax.tick_params(colors='black')

    plot_ax(axes[0][0], 'qps')
    plot_ax(axes[0][1], 'p0')
    plot_ax(axes[0][2], 'p50')
    plot_ax(axes[1][0], 'p95')
    plot_ax(axes[1][1], 'p99')
    plot_ax(axes[1][2], 'p99.9')
    fig.savefig(f'plots_{flow_name}.png', dpi=300, bbox_inches='tight')

    # write to slack
    client = WebClient(token=os.getenv('SLACK_API_TOKEN'))
    channel_name = "#notifications-search-performance"

    try:
        response = client.chat_postMessage(
            channel=channel_name,
            text=f"*Hello* 👋 below are the benchmarking results for indexing and querying when indexing "
            f"{MAX_DOCS_FOR_BENCHMARKING} {scenario} documents when using {payload_slack_n_latency_calls} calls to "
            f"measure latency and using {payload_slack_n_qps_calls} total calls with {payload_slack_n_qps_workers} "
            f"of them concurrently to measure QPS and P's:\n"
            f"{json.dumps(slack_payload, indent=4)}\n"
            f"{num_docs_indexed} / {MAX_DOCS_FOR_BENCHMARKING} documents were indexed successfully.\n"
            f"Please find the attached plot and CSVs for more details.",
        )

        # Get the timestamp of the original message
        original_message_ts = response["ts"]
        for file_path in [
            f'latency_{flow_name}.csv',
            f'qps_{flow_name}.csv',
            f'plots_{flow_name}.png',
        ]:
            client.files_upload(
                channels=channel_name,
                file=file_path,
                title=os.path.basename(file_path),
                thread_ts=original_message_ts,  # Use the timestamp to specify the thread
            )
    except SlackApiError as e:
        import traceback

        traceback.print_exc()
        print(f"Error posting message: {e}")

    # delete flow
    terminate_wolf(get_flow_id_from_name(flow_name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', type=str, choices=['video+text', 'image'])
    args = parser.parse_args()

    deploy_scenario(args.scenario)
