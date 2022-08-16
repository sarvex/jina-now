import requests
from tests.integration.test_end_to_end import get_default_request_body

# TODO needs to be updated to work on cli / or even putting things like this into a new client package
url = f'http://localhost:30090/api/v1'
deployment_type = 'remote'
emails = ['florian.hoenicke@jina.ai']
request_body = get_default_request_body(deployment_type, secured=True)
request_body['user_emails'] = emails
response = requests.post(
    f'{url}/admin/updateUserEmails',
    json=request_body,
)
