import requests
from tests.integration.test_end_to_end import get_default_request_body

#  TODO needs to be updated to work on cli / or even putting things like this into a new client package


def update_emails(deployment_type, emails):
    if deployment_type == 'remote':
        url = f"https://nowrun.jina.ai/api/v1"  # remote
    else:
        url = f'http://localhost:30090/api/v1'  # local
    # url = f'http://localhost:8080/api/v1'  # for local testing
    request_body = get_default_request_body(deployment_type, secured=True)
    # request_body['host'] = f'grpc://0.0.0.0'  # for local testing
    # request_body['port'] = 9090  # for local testing
    request_body['user_emails'] = emails
    response = requests.post(
        f'{url}/admin/updateUserEmails',
        json=request_body,
    )
    print()


if __name__ == '__main__':
    update_emails(
        deployment_type='remote',
        emails=['hoenicke.florian@gmail.com'],
    )
