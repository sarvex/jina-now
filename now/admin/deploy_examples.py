import os
from argparse import Namespace
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import boto3

from now.cli import cli
from now.constants import DEFAULT_EXAMPLE_HOSTED
from now.deployment.deployment import list_all_wolf, terminate_wolf


def upsert_cname_record(source, target):
    client = boto3.client(
        'route53',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
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
    NAMESPACE = f'examples-{app}-{data}'.replace('_', '-')
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
        else:
            print(
                'No host returned starting with "grpcs://". Make sure Jina NOW returns host'
            )
    else:
        raise ValueError(f'Deployment failed for {app} and {data}. Re-run it')
    return response


if __name__ == '__main__':
    os.environ['JINA_AUTH_TOKEN'] = os.environ.get('WOLF_TOKEN')
    os.environ['NOW_EXAMPLES'] = 'True'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'

    # List all deployments and delete them
    flows = list_all_wolf(namespace=None)
    flow_ids = [f['id'].replace('jflow-', '') for f in flows]
    with ThreadPoolExecutor() as thread_executor:
        # call delete function with each flow
        delete_results = thread_executor.map(lambda x: terminate_wolf(x), flow_ids)

    for i, result in enumerate(delete_results):
        print(f'Deleted {i} deployment ', result if result else '')

    # Create new deployments
    results = []
    with ProcessPoolExecutor() as thread_executor:
        futures = []
        for app, data in DEFAULT_EXAMPLE_HOSTED.items():
            for ds_name in data:
                f = thread_executor.submit(deploy, app, ds_name)
                futures.append(f)
        for f in futures:
            results.append(f.result())
    print(results)
