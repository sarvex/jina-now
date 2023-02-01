import base64
import os

import boto3
import click


def get_secret_value(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' in response:
        return response['SecretString']
    else:
        return base64.b64decode(response['SecretBinary'])


def set_environment_variables(secret_name, region_name):
    secret_value = get_secret_value(secret_name, region_name)
    os.environ['AWS_ACCESS_KEY_ID'] = secret_value['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_value['AWS_SECRET_ACCESS_KEY']


@click.command()
@click.argument('secret', type=str, required=True)
@click.option('-r', '--region', type=str, default='eu-central-1', show_default=True)
def main(secret, region):
    set_environment_variables(secret, region)


if __name__ == '__main__':
    main()
