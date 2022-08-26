from concurrent.futures import ProcessPoolExecutor
from time import time

from now.client import Client
from now.constants import Apps

client = Client(
    jcloud_id='...',
    app=Apps.TEXT_TO_VIDEO,
    api_key='...',
)


def call():
    return client.send_request(
        '/search',
        text='girl on a motorbike',
        limit=60,
    )
    # return client.send_request_bff(
    #     'text-to-video/search',
    #     text='girl on a motorbike',
    #     limit=60,
    # )


# Latency Test
start = time()
for i in range(10):
    call()
print(f'Latency: {(time() - start) / 10}s')


num_queries = 500
worker = 30
with ProcessPoolExecutor(max_workers=worker) as executor:
    # QPS test
    start = time()
    futures = []
    latencies = []

    for i in range(num_queries):
        future = executor.submit(call)
        futures.append(future)
    for future in futures:
        latencies.append(future.result())
    print(f'QPS: {num_queries / (time() - start)}s')

latencies = sorted(latencies)
for p in [0, 50, 75, 85, 90, 95, 99, 99.9]:
    print(f"P{p}: {latencies[int(len(latencies)*p/100)]}")
