import os
from argparse import Namespace
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import boto3
from jina import Client

from now.cli import cli
from now.constants import DEFAULT_EXAMPLE_HOSTED
from now.deployment.deployment import list_all_wolf, terminate_wolf

aws_client = boto3.client(
    'route53',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
)


def upsert_cname_record(source, target):
    try:
        aws_client.change_resource_record_sets(
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


def deploy(app_name, app_data):
    NAMESPACE = f'examples-{app_name}-{app_data}'.replace('_', '-')
    kwargs = {
        'now': 'start',
        'app': app_name,
        'data': app_data,
        'deployment_type': 'remote',
        'proceed': True,
        'secured': False,
        'ns': NAMESPACE,
    }
    kwargs = Namespace(**kwargs)
    response_cli = cli(args=kwargs)

    # parse the response
    if response_cli:
        host_target_ = response_cli.get('host')
        if host_target_ and host_target_.startswith('grpcs://'):
            host_target_ = host_target_.replace('grpcs://', '')
            host_source = f'now-example-{app_name}-{app_data}.dev.jina.ai'.replace(
                '_', '-'
            )
            # update the CNAME entry in the Route53 records
            upsert_cname_record(host_source, host_target_)
        else:
            print(
                'No host returned starting with "grpcs://". Make sure Jina NOW returns host'
            )
    else:
        raise ValueError(f'Deployment failed for {app_name} and {app_data}. Re-run it')
    return response_cli


if __name__ == '__main__':
    os.environ['JINA_AUTH_TOKEN'] = os.environ.get('WOLF_TOKEN')
    os.environ['NOW_EXAMPLES'] = 'True'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'
    deployment_type = os.environ.get('DEPLOYMENT_TYPE', 'partial')
    to_deploy = set()

    if deployment_type == 'all':
        # List all deployments and delete them
        flows = list_all_wolf(namespace=None)
        flow_ids = [f['id'].replace('jflow-', '') for f in flows]
        with ThreadPoolExecutor() as thread_executor:
            # call delete function with each flow
            delete_results = thread_executor.map(lambda x: terminate_wolf(x), flow_ids)
    else:
        # check if deployment is already running else delete it
        for app, data in DEFAULT_EXAMPLE_HOSTED.items():
            for ds_name in data:
                host = f'noow-example-{app}-{ds_name}.dev.jina.ai'.replace('_', '-')
                jina_client = Client(host=host)
                try:
                    jina_client.post('/dry_run', timeout=2)
                except ConnectionError:
                    # This deployment is not running. Get its host_target from CNAME record and delete it
                    try:
                        response = aws_client.list_resource_record_sets(
                            HostedZoneId=os.environ['AWS_HOSTED_ZONE_ID'],
                            StartRecordName=host,
                            StartRecordType='CNAME',
                            MaxItems='1',
                        )
                        host_target = response['ResourceRecordSets'][0][
                            'ResourceRecords'
                        ][0]['Value']
                        if host == host_target:
                            terminate_wolf(host_target)
                        else:
                            to_deploy.add((app, ds_name))
                    except Exception as e:
                        print(e)

    for i, result in enumerate(delete_results):
        print(f'Deleted {i} deployment ', result if result else '')

    # Create new deployments
    results = []
    with ProcessPoolExecutor(max_workers=2) as thread_executor:
        futures = []
        if deployment_type == 'all':
            for app, data in DEFAULT_EXAMPLE_HOSTED.items():
                for ds_name in data:
                    f = thread_executor.submit(deploy, app, ds_name)
                    futures.append(f)
        else:
            for app, ds_name in to_deploy:
                f = thread_executor.submit(deploy, app, ds_name)
                futures.append(f)
        for f in futures:
            results.append(f.result())
    print(results)
