from concurrent.futures import ProcessPoolExecutor
from time import time

from now.client import Client
from now.constants import Apps


def call(client, use_bff):
    if use_bff:
        return client.send_request(
            '/search',
            text='girl on a motorbike',
            limit=60,
        )
    else:
        return client.send_request_bff(
            'text-to-video/search',
            text='girl on a motorbike',
            limit=60,
        )


def benchmark_deployment(jcloud_id, api_key):
    print(f"benchmark flow: {jcloud_id}")

    client = Client(
        jcloud_id=jcloud_id,
        app=Apps.TEXT_TO_VIDEO,
        api_key=api_key,
    )

    for call_point, use_bff in zip(['BFF', 'Gateway'], [True, False]):
        # Latency Test
        start = time()
        for i in range(10):
            call(client=client, use_bff=use_bff)
        print(f'Latency {call_point}: {(time() - start) / 10}s')

        num_queries = 500
        worker = 30
        with ProcessPoolExecutor(max_workers=worker) as executor:
            # QPS test
            start = time()
            futures = []
            latencies = []

            for i in range(num_queries):
                future = executor.submit(call, client, use_bff)
                futures.append(future)
            for future in futures:
                latencies.append(future.result())
            print(f'QPS {call_point}: {num_queries / (time() - start)}s')

        latencies = sorted(latencies)
        for p in [0, 50, 75, 85, 90, 95, 99, 99.9]:
            print(f"P{p} {call_point}: {latencies[int(len(latencies) * p / 100)]}")

        print('\n')


for jcloud_id, api_key in [('', ''), ('', '')]:
    benchmark_deployment(jcloud_id=jcloud_id, api_key=api_key)
