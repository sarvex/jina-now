import requests

val = {
    'dataset_type': 'docarray',
    'dataset_name': 'laion_400m_part2',
    'dataset_url': None,
    'dataset_path': None,
    'aws_access_key_id': None,
    'aws_secret_access_key': None,
    'aws_region_name': None,
    'task_config': None,
    'es_text_fields': None,
    'es_image_fields': None,
    'es_index_name': None,
    'es_host_name': None,
    'es_additional_args': None,
    'cluster': None,
    'deployment_type': 'remote',
    'secured': True,
    'jwt': {
        'token': '8c663c34de520f04dfa1bc22cc06e6d1:13d67a04223dae505f54df11590b72f542dadad3'
    },
    'admin_emails': ['kalim.akram@jina.ai'],
    'user_emails': None,
    'additional_user': None,
    'api_key': '2e2660108dd3498386e7be2217e437bf',
}

scheduler_params = {
    'flow_id': '123',  # get_flow_id(gateway_host_internal),
    'api_key': val['api_key'],
    'user_input': val,
}
cookies = {'st': val['jwt']['token']}
resp = requests.post(
    'http://0.0.0.0:8080/api/v1/schedule_sync', json=scheduler_params, cookies=cookies
)

print(resp.text)
