from concurrent.futures import ProcessPoolExecutor
from time import time
from typing import Dict

import requests


def benchmark_deployment(
    http_host: str,
    search_text: str,
    limit: int = 9,
    api_key: str = None,
    jwt: str = None,
) -> Dict[str, float]:
    """Benchmarks a deployment by measuring latency and QPS on '/search' endpoint."""
    request_body = {
        'query': [{'name': 'query_text', 'modality': 'text', 'value': search_text}],
        'limit': limit,
    }
    if api_key:
        request_body['api_key'] = api_key
    if jwt:
        request_body['jwt'] = {'token': jwt}

    def call():
        t0 = time()
        resp = requests.post(f'{http_host}/api/v1/search-app/search', json=request_body)
        if resp.status_code != 200:
            raise Exception(
                f'Bad response code: {resp.status_code}\nContent: {resp.content}'
            )
        return time() - t0

    # measure latency
    latency_calls = 10
    start = time()
    for i in range(latency_calls):
        call()
    latency = (time() - start) / latency_calls
    result = {'latency': latency}
    print(f'Latency : {(time() - start) / 10}s')

    # measure QPS
    num_queries = 100
    worker = 5
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
        print(f"P{p}: {latencies[int(len(latencies) * p / 100)]}")
        result[f'p{p}'] = latencies[int(len(latencies) * p / 100)]

    return result


if __name__ == '__main__':
    flow_name = ''
    api_key = ''
    search_text = ''
    limit = 60

    host = f'https://{flow_name}-http.wolf.jina.ai'
    benchmark_deployment(
        http_host=host, search_text=search_text, limit=limit, api_key=api_key
    )
