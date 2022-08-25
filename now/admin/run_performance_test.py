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
    return client.send_request_bff(
        'text-to-video/search',
        text='girl on a motorbike',
        limit=60,
    )


# Latency Test
start = time()
for i in range(10):
    call()
print(f'Latency: {(time() - start) / 10}s')

# QPS test
start = time()
num_queries = 3000
worker = 100
with ProcessPoolExecutor(max_workers=worker) as executor:
    futures = []

    for i in range(num_queries):
        future = executor.submit(call)
        futures.append(future)
    for future in futures:
        future.result()
print(f'QPS: {num_queries / (time() - start)}s')
