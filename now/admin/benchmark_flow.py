import json
from concurrent.futures import ProcessPoolExecutor
from time import time
from typing import Dict

import requests


def _call(http_host, request_body) -> float:
    t0 = time()
    resp = requests.post(f'{http_host}/api/v1/search-app/search', json=request_body)
    if resp.status_code != 200:
        raise Exception(
            f'Bad response code: {resp.status_code}\nContent: {resp.content}'
        )
    return time() - t0


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

    # measure latency
    latency_calls = 30
    start = time()
    for i in range(latency_calls):
        _call(http_host=http_host, request_body=request_body)
    latency = (time() - start) / latency_calls
    result = {'latency': latency}

    # measure QPS
    num_queries = 250
    worker = 10
    with ProcessPoolExecutor(max_workers=worker) as executor:
        # QPS test
        start = time()
        futures = []
        latencies = []

        for i in range(num_queries):
            future = executor.submit(_call, http_host, request_body)
            futures.append(future)
        for future in futures:
            latencies.append(future.result())
        qps = num_queries / (time() - start)
        result['qps'] = qps

    latencies = sorted(latencies)
    for p in [0, 50, 75, 85, 90, 95, 99, 99.9]:
        result[f'p{p}'] = latencies[int(len(latencies) * p / 100)]

    return result


if __name__ == '__main__':
    flow_name = ''
    api_key = ''
    search_text = ''
    limit = 60

    http_host = f'https://{flow_name}-http.wolf.jina.ai'
    result = benchmark_deployment(
        http_host=http_host, search_text=search_text, limit=limit, api_key=api_key
    )
    print(json.dumps(result, indent=4))
