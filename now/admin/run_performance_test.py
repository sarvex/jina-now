from time import time

from now.admin.benchmark_flow import measure_qps
from now.client import Client
from now.constants import Apps


def call(client, use_bff):
    if use_bff:
        send_fn = client.send_request_bff
        end_point = 'text-to-video/search'
    else:
        send_fn = client.send_request
        end_point = '/search'
    start = time()

    try:
        send_fn(endpoint=end_point, text='girl on a motorbike', limit=60)
    except Exception as e:
        import traceback

        traceback.print_exc()
    dif = time() - start
    return dif


def benchmark_deployment(jcloud_id, api_key):
    print(f"benchmark flow: {jcloud_id}")

    client = Client(
        jcloud_id=jcloud_id,
        app=Apps.SEARCH_APP,
        api_key=api_key,
    )

    for call_point, use_bff in zip(['BFF', 'Gateway'], [True, False]):
        # Latency Test
        start = time()
        for i in range(10):
            call(client=client, use_bff=use_bff)
        print(f'Latency {call_point}: {(time() - start) / 10}s')

        measure_qps(500, 30)

        print('\n')


if __name__ == '__main__':
    benchmark_deployment(jcloud_id='jcloud_id', api_key='api_key')
