import os
import sys
from argparse import Namespace
from dataclasses import dataclass

import boto3
from jcloud.flow import CloudFlow
from jina import Client

from now.cli import cli
from now.common.detect_schema import set_field_names_from_docarray
from now.constants import DEMO_NS, MODALITY_TO_MODELS, DatasetTypes
from now.demo_data import AVAILABLE_DATASETS
from now.deployment.deployment import get_or_create_eventloop, terminate_wolf
from now.now_dataclasses import UserInput


# TODO: Remove this once the Jina NOW version is bumped
@dataclass
class AWSProfile:
    aws_access_key_id: str
    aws_secret_access_key: str
    region: str


def get_aws_profile():
    session = boto3.Session()
    credentials = session.get_credentials()
    aws_profile = (
        AWSProfile(credentials.access_key, credentials.secret_key, session.region_name)
        if credentials
        else AWSProfile(None, None, session.region_name)
    )
    return aws_profile


def upsert_cname_record(source, target):
    aws_profile = get_aws_profile()
    aws_client = boto3.client(
        'route53',
        aws_access_key_id=aws_profile.aws_access_key_id,
        aws_secret_access_key=aws_profile.aws_secret_access_key,
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
    except Exception as e:  # noqa
        print(e)


def list_all_wolf(status='Serving', namespace='nowapi'):
    flows = []
    loop = get_or_create_eventloop()
    jflows = loop.run_until_complete(CloudFlow().list_all(phase=status))['flows']
    # Transform the JCloud flow response to a much simpler list of dicts
    for flow in jflows:
        executor_name = list(flow['status']['endpoints'].keys())[0]
        flows.append(
            {
                'id': flow['id'],
                'name': flow['status']['endpoints'][executor_name],
                'created_at': flow['ctime'],
                'finished_at': flow['utime'],
            }
        )
    # filter by namespace - if the namespace is contained in the flow name
    if namespace:
        return [f for f in flows if namespace in f['id']]
    return flows


def delete_flow(ns, reverse=False, failed=False):
    # append `-nowapi` to the namespace to make it unique
    flow = list_all_wolf(namespace=ns + '-nowapi')
    flow = sorted(flow, key=lambda x: x['created_at'], reverse=reverse)
    if failed or len(flow) > 1:
        print(f'Found {len(flow)} flows for namespace {ns}')
        terminate_wolf(flow[0]['id'])
        print(f'{flow[0]["id"]} successfully deleted!!')


def deploy(demo_ds):
    print(f'Deploying search app with data: {demo_ds.name}')
    NAMESPACE = DEMO_NS.format(demo_ds.name.split("/")[-1])
    # Get the schema
    user_input = UserInput()
    user_input.dataset_name = demo_ds.name
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.jwt = {'token': os.environ['JINA_AUTH_TOKEN']}
    set_field_names_from_docarray(user_input)

    # Get all model for each of the index fields
    model_kwargs = {}
    for field, modality in user_input.index_field_candidates_to_modalities.items():
        if (
            field == demo_ds.index_fields
        ):  # TODO: remove this if check when __all__ is supported
            model_kwargs[f'{field}_model'] = [
                models['value'] for models in MODALITY_TO_MODELS[modality]
            ]

    kwargs = {
        'now': 'start',
        'dataset_type': DatasetTypes.DEMO,
        'dataset_name': demo_ds.name,
        'index_fields': [demo_ds.index_fields],  # TODO: replace with '__all__'
        'filter_fields': '__all__',
        'proceed': True,
        'secured': False,
        'ns': NAMESPACE,
        'flow_name': NAMESPACE,
        **model_kwargs,
    }
    kwargs = Namespace(**kwargs)
    try:
        response_cli = cli(args=kwargs)
    except Exception as e:  # noqa E722
        # delete flow with broken index, i.e. latest flow
        delete_flow(ns=NAMESPACE, reverse=True, failed=True)
        raise e
    finally:
        # Delete the remaining old flow because either is it not reachable or it is to be replaced
        delete_flow(ns=NAMESPACE)

    # parse the response
    host_target_ = response_cli.get('host')
    if host_target_ and host_target_.startswith('grpcs://'):
        host_target_ = host_target_.replace('grpcs://', '')
        host_source = f'{DEMO_NS.format(demo_ds.name.split("/")[-1])}.dev.jina.ai'
        # update the CNAME entry in the Route53 records
        print(f'Updating CNAME record for `{host_source}` -> `{host_target_}`')
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
    deployment_type = os.environ.get('DEPLOYMENT_TYPE', None)
    if not deployment_type:
        deployment_type = 'partial'
    deployment_type = deployment_type.lower()
    print(f'Deployment type: {deployment_type}')
    index = int(sys.argv[-1])
    # get all the available demo datasets list
    dataset_list = []
    for _, ds_list in AVAILABLE_DATASETS.items():
        for ds in ds_list:
            dataset_list.append(ds)

    for idx, ds in enumerate(dataset_list):
        print(f'Index {idx}: {ds.name}: {DEMO_NS.format(ds.name.split("/")[-1])}')

    while index < len(dataset_list):
        to_deploy = dataset_list[index]

        print('\n----------------------------------------')
        print(f'Deploying -> Index {index}: ({to_deploy})')
        print('----------------------------------------')

        if deployment_type == 'partial':
            # check if deployment is already running then return
            client = Client(
                host=f'grpcs://{DEMO_NS.format(to_deploy.name.split("/")[-1])}.dev.jina.ai'
            )
            try:
                response = client.post('/dry_run', return_results=True)
                print(f'Index {index}: already {to_deploy.name} deployed')
                index += 5
                continue
            except Exception as e:  # noqa E722
                print('Not deployed yet')

        print('Deploying -> Index {index}: ', to_deploy.name)
        deploy(to_deploy)
        print('------------------ Deployment Successful----------------------')
        index += 5
