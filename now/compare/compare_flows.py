import json
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from time import sleep
from typing import Dict, List, Tuple

import pandas as pd
import requests
from docarray import Document, DocumentArray
from tqdm import tqdm

from now.admin.utils import get_default_request_body
from now.executor.gateway.bff.app.v1.models.search import SearchResponseModel
from now.utils import get_chunk_by_field_name


def compare_flows_for_queries(
    da: DocumentArray,
    flow_ids_http_semantic_scores: List[Tuple],
    limit: int,
    results_per_table: int = 20,
    disable_to_datauri: bool = False,
):
    """This function compares flows by querying every flow with the multi-modal query documents of da and retrieving
    limit results from them. It converts the responses into HTML which are then saved as a table to disk. It splits
    the created HTML into multiple files if there are a lot of queries.

    :param da: DocumentArray of queries defined as multi-modal documents
    :param flow_ids_http_semantic_scores: a list consisting of tuples which are (flow ID, host of HTTP gateway,
    semantic scores)
    :param limit: the number of results to retrieve
    :param results_per_table: number of queries which should be put into the same table
    :param disable_to_datauri: if True, the images are not converted to DataURI
    """
    # call flow/api/v1/search-app/search for all queries and all flow_ids_http_semantic_scores
    print(f'Comparing {len(da)} queries')
    now = datetime.now()
    folder = str(
        os.path.abspath(
            f'compare'
            f'-{"-".join(set([cihs[0] for cihs in flow_ids_http_semantic_scores]))}'
            f'-limit_{limit}'
            f'-{now.strftime("%Y%m%d")}-{now.strftime("%H%M")}'
        )
    )
    os.mkdir(folder)
    rows = []
    cnt_tables = 0
    with tqdm(total=len(da)) as pbar:
        with ProcessPoolExecutor(max_workers=min(len(da), 20)) as ex:
            futures = [
                ex.submit(
                    _evaluate_query,
                    query,
                    flow_ids_http_semantic_scores,
                    limit,
                    disable_to_datauri,
                )
                for query in da
            ]
            for future in futures:
                rows.append(future.result())
                pbar.update(1)
                if len(rows) == results_per_table:
                    df = pd.DataFrame(rows)
                    df.to_html(
                        os.path.join(
                            folder,
                            f'queries-{cnt_tables * results_per_table}-to-{(cnt_tables + 1) * results_per_table}.html',
                        ),
                        escape=False,
                    )
                    cnt_tables += 1
                    rows = []
    if rows:
        df = pd.DataFrame(rows[cnt_tables : len(rows)])
        df.to_html(
            os.path.join(
                folder,
                f'queries-{cnt_tables * results_per_table}-to-{cnt_tables * results_per_table + len(rows) - 1}.html',
            ),
            escape=False,
        )
    print(f'Comparison tables were saved as HTMLs under: {folder}')


def _evaluate_query(
    query: Document,
    flow_ids_http_semantic_scores: List[Tuple],
    limit: int,
    disable_to_datauri: bool,
) -> Dict[str, str]:
    """Sends the query to each flow and returns a dictionary which maps 'query' and flow names with their semantic
    scores to HTML of the results.

    :param query: query defined as multi-modal document
    :param flow_ids_http_semantic_scores: a list consisting of tuples which are (flow ID, host of HTTP gateway,
    semantic scores)
    :param limit: the number of results to retrieve
    :param disable_to_datauri: if True, the images are not converted to DataURI
    """
    query_dict_search_request = []
    query_dict_response = {}
    for field in query._metadata['multi_modal_schema'].keys():
        field_chunk = get_chunk_by_field_name(query, field)
        if field_chunk.modality != 'text':
            if field_chunk.uri:
                field_chunk.convert_uri_to_datauri()
            else:
                field_chunk.convert_content_to_datauri()
        query_dict_response[field] = {
            'uri': field_chunk.uri,
            'blob': field_chunk.blob,
            'text': field_chunk.text,
        }
        query_dict_search_request.append(
            {
                'name': field,
                'modality': field_chunk.modality,
                'value': field_chunk.uri or field_chunk.content,
            }
        )
    row = {
        'query': SearchResponseModel(id=query.id, fields=query_dict_response).to_html(
            disable_to_datauri
        )
    }

    for flow_name, http_host, semantic_scores in flow_ids_http_semantic_scores:
        request_body = get_default_request_body(secured=True)
        request_body['limit'] = limit
        request_body['query'] = query_dict_search_request
        request_body['create_temp_link'] = True
        request_body['semantic_scores'] = semantic_scores
        for _ in range(5):
            try:
                response = requests.post(
                    f'{http_host}/api/v1/search-app/search', json=request_body
                )
                if response.status_code == 200:
                    break
            except ConnectionError:
                sleep(1)
                continue
        row[
            f'{flow_name} - {json.dumps(semantic_scores)}'
        ] = SearchResponseModel.responses_to_html(
            [SearchResponseModel(**r) for r in response.json()], disable_to_datauri
        )

    return row
