from concurrent.futures import ProcessPoolExecutor
from time import time

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
    return time() - start


def benchmark_deployment(jcloud_id, api_key):
    print(f"benchmark flow: {jcloud_id}")

    client = Client(
        jcloud_id=jcloud_id,
        app=Apps.SEARCH_APP,
        api_key=api_key,
    )

    num_queries = 500
    for call_point, use_bff in zip(['BFF', 'Gateway'], [True, False]):
        # Latency Test
        start = time()
        for _ in range(10):
            call(client=client, use_bff=use_bff)
        print(f'Latency {call_point}: {(time() - start) / 10}s')

        worker = 30
        with ProcessPoolExecutor(max_workers=worker) as executor:
            # QPS test
            start = time()
            futures = []
            for _ in range(num_queries):
                future = executor.submit(call, client, use_bff)
                futures.append(future)
            latencies = [future.result() for future in futures]
            print(f'QPS {call_point}: {num_queries / (time() - start)}s')

        latencies = sorted(latencies)
        for p in [0, 50, 75, 85, 90, 95, 99, 99.9]:
            print(f"P{p} {call_point}: {latencies[int(len(latencies) * p / 100)]}")

        print('\n')


if __name__ == '__main__':
    benchmark_deployment(jcloud_id='jcloud_id', api_key='api_key')
