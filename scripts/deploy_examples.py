import os
import sys
from argparse import Namespace

import boto3
import requests

from now.cli import cli
from now.constants import DEMO_NS, DatasetTypes
from now.demo_data import AVAILABLE_DATASETS
from now.deployment.deployment import list_all_wolf, terminate_wolf


def upsert_cname_record(source, target):
    aws_client = boto3.client(
        'route53',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )
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


def deploy(demo_ds):
    print(f'Deploying search app with data: {demo_ds.name}')
    NAMESPACE = DEMO_NS.format(demo_ds.name.split("/")[-1])
    kwargs = {
        'now': 'start',
        'dataset_type': DatasetTypes.DEMO,
        'dataset_name': demo_ds.name,
        'deployment_type': 'remote',
        'proceed': True,
        'secured': False,
        'ns': NAMESPACE,
        'flow_name': NAMESPACE,
        'index_fields': demo_ds.index_fields,
        'filter_fields': '__all__',
    }
    kwargs = Namespace(**kwargs)
    try:
        response_cli = cli(args=kwargs)
    except Exception as e:  # noqa E722
        raise e
    # parse the response
    host_target_ = response_cli.get('host')
    if host_target_ and host_target_.startswith('grpcs://'):
        host_target_ = host_target_.replace('grpcs://', '')
        host_source = f'{DEMO_NS.format(demo_ds.name.split("/")[-1])}.dev.jina.ai'
        # update the CNAME entry in the Route53 records
        upsert_cname_record(host_source, host_target_)
    else:
        print(
            'No host returned starting with "grpcs://". Make sure Jina NOW returns host'
        )
    return response_cli


if __name__ == '__main__':
    os.environ['JINA_AUTH_TOKEN'] = os.environ.get('WOLF_TOKEN')
    os.environ['NOW_EXAMPLES'] = 'True'
    os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'
    deployment_type = os.environ.get('DEPLOYMENT_TYPE', 'partial').lower()
    index = int(sys.argv[-1])
    # get all the available demo datasets list
    dataset_list = []
    for _, ds_list in AVAILABLE_DATASETS.items():
        for ds in ds_list:
            dataset_list.append(ds)

    if index > len(dataset_list):
        print(f'Index {index} is out of range. Max index is {len(dataset_list)}')
        exit(0)
    to_deploy = dataset_list[index]

    print(f'Deploying -> ({to_deploy}) with deployment type ``{deployment_type}``')

    if deployment_type == 'all':
        # List all deployments and delete them
        flow = list_all_wolf(namespace=to_deploy.name.split("/")[-1])
        terminate_wolf(flow['id'])
        print(f'{to_deploy} successfully deleted!!')
    else:
        # check if deployment is already running else add to the queue
        bff = 'https://nowrun.jina.ai/api/v1/admin/getStatus'
        host = f'grpcs://{DEMO_NS.format(to_deploy.name.split("/")[-1])}.dev.jina.ai'
        request_body = {
            'host': host,
            'jwt': {'token': os.environ['WOLF_TOKEN']},
        }
        resp = requests.post(bff, json=request_body)
        if resp.status_code == 200:
            print(f'{to_deploy} already deployed!!')
            exit(0)
        print('Deploying -> ', to_deploy)
    deploy(to_deploy)
