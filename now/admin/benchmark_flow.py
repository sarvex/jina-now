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


def benchmark_deployment_latency(
    http_host: str,
    search_text: str,
    limit: int = 9,
    api_key: str = None,
    jwt: str = None,
    n_latency_calls: int = 30,
) -> Dict[str, float]:
    """Benchmarks a deployment by measuring latency on '/search' endpoint."""
    request_body = {
        'query': [{'name': 'query_text', 'modality': 'text', 'value': search_text}],
        'limit': limit,
    }
    if api_key:
        request_body['api_key'] = api_key
    if jwt:
        request_body['jwt'] = {'token': jwt}

    # measure latency
    start = time()
    for i in range(n_latency_calls):
        _call(http_host=http_host, request_body=request_body)
    return (time() - start) / n_latency_calls


def benchmark_deployment_qps(
    http_host: str,
    search_text: str,
    limit: int = 9,
    api_key: str = None,
    jwt: str = None,
    n_qps_calls: int = 250,
    n_qps_workers: int = 10,
) -> Dict[str, float]:
    """Benchmarks a deployment by measuring QPS on '/search' endpoint."""
    request_body = {
        'query': [{'name': 'query_text', 'modality': 'text', 'value': search_text}],
        'limit': limit,
    }
    if api_key:
        request_body['api_key'] = api_key
    if jwt:
        request_body['jwt'] = {'token': jwt}

    result = {}
    # measure QPS
    with ProcessPoolExecutor(max_workers=n_qps_calls) as executor:
        # QPS test
        start = time()
        futures = []
        latencies = []

        for i in range(n_qps_workers):
            future = executor.submit(_call, http_host, request_body)
            futures.append(future)
        for future in futures:
            latencies.append(future.result())
        qps = n_qps_workers / (time() - start)
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
    latency = benchmark_deployment_latency(
        http_host=http_host, search_text=search_text, limit=limit, api_key=api_key
    )

    qps_dict = benchmark_deployment_qps(
        http_host=http_host, search_text=search_text, limit=limit, api_key=api_key
    )

    print(json.dumps({'latency': latency, **qps_dict}, indent=4))
