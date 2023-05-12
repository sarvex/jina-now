from concurrent.futures import ProcessPoolExecutor
from time import time

from jina import Client, Document

flow_id = ''
api_key = ''
search_text = ''
limit = 60


client = Client(host=f'grpcs://nowapi-{flow_id}.wolf.jina.ai')


def call():
    t0 = time()
    result = client.post(
        '/search',
        inputs=Document(chunks=Document(text=search_text)),
        parameters={
            'api_key': api_key,
            'limit': limit,
        },
    )
    return time() - t0


# measure latency
start = time()
for _ in range(10):
    call()
print(f'Latency : {(time() - start) / 10}s')


# measure QPS
num_queries = 100
worker = 5
with ProcessPoolExecutor(max_workers=worker) as executor:
    # QPS test
    start = time()
    futures = []
    for _ in range(num_queries):
        future = executor.submit(call)
        futures.append(future)
    latencies = [future.result() for future in futures]
    print(f'QPS: {num_queries / (time() - start)}s')

latencies = sorted(latencies)
for p in [0, 50, 75, 85, 90, 95, 99, 99.9]:
    print(f"P{p}: {latencies[int(len(latencies) * p / 100)]}")
