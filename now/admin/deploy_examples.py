import os
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor

import boto3

from now.cli import cli
from now.constants import DemoDatasets
from now.deployment.deployment import list_all_wolf, terminate_wolf

NAMESPACE = 'examples'
os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'
DEFAULT_EXAMPLE_HOSTED = {
    'image_to_image': [
        DemoDatasets.BEST_ARTWORKS,
        DemoDatasets.TLL,
        # DemoDatasets.DEEP_FASHION,
    ],
    # 'image_to_text': [DemoDatasets.RAP_LYRICS],
    # 'image_to_imaage': [DemoDatasets.TLL],
    # 'text_to_text': [DemoDatasets.ROCK_LYRICS],
    # 'music_to_music': [DemoDatasets.MUSIC_GENRES_ROCK],
}


client = boto3.client(
    'route53',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
)


def upsert_cname_record(source, target):
    try:
        response = client.change_resource_record_sets(
            HostedZoneId=os.environ['AWS_HOSTED_ZONE_ID'],
            ChangeBatch={
                'Comment': 'add %s -> %s' % (source, target),
                'Changes': [
                    {
                        'Action': 'UPSERT',
                        'ResourceRecordSet': {
                            'Name': source,
                            'Type': 'CNAME',
                            'TTL': 300,
                            'ResourceRecords': [{'Value': target}],
                        },
                    }
                ],
            },
        )
    except Exception as e:
        print(e)


def deploy(app, data):
    kwargs = {
        'now': 'start',
        'app': app,
        'data': data,
        'deployment_type': 'remote',
        'proceed': True,
        'secured': False,
        'ns': NAMESPACE,
    }
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    # parse the response
    if response:
        host_target = response.get('host')
        if host_target and host_target.startswith('grpcs://'):
            host_target = host_target.replace('grpcs://', '')
            host_source = f'now-example-{app}-{data}.dev.jina.ai'.replace('_', '-')
            # update the CNAME entry in the Route53 records
            upsert_cname_record(host_source, host_target)
    return response


if __name__ == '__main__':
    os.environ['JINA_AUTH_TOKEN'] = os.environ.get('WOLF_TOKEN')
    os.environ['NOW_EXAMPLES'] = 'True'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'
    os.environ['NOW_CI_RUN'] = 'True'

    # List all deployments and delete them
    flows = list_all_wolf(namespace=NAMESPACE)
    flow_ids = [f['id'].replace('jflow-', '') for f in flows]
    with ThreadPoolExecutor() as thread_executor:
        # call delete function with each flow
        delete_results = thread_executor.map(lambda x: terminate_wolf(x), flow_ids)

    for i, result in enumerate(delete_results):
        print(f'Deleted {i} deployment', result)

    # Create new deployments
    results = []
    with ThreadPoolExecutor() as thread_executor:
        futures = []
        for app, data in DEFAULT_EXAMPLE_HOSTED.items():
            for ds_name in data:
                f = thread_executor.submit(deploy, app, ds_name)
                futures.append(f)
        for f in futures:
            results.append(f.result())
    print(results)
