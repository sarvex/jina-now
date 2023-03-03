import base64
from typing import Dict, List

import requests
import streamlit as st
from docarray import Document, DocumentArray
from docarray.score import NamedScore

from now.constants import NOWGATEWAY_BFF_PORT

from .constants import Parameters


def get_query_params() -> Parameters:
    query_parameters = st.experimental_get_query_params()
    parameters = Parameters()
    for key, val in query_parameters.items():
        if val:
            setattr(
                parameters,
                key,
                val[0],
            )
    return parameters


def call_flow(url_host, data, endpoint):
    st.session_state.search_count += 1
    response = requests.post(
        url_host, json=data, headers={"Content-Type": "application/json; charset=utf-8"}
    )

    try:
        if endpoint == 'suggestion':
            docs = DocumentArray.from_json(response.content)
        elif endpoint == 'search':
            docs = DocumentArray()
            # todo: use multimodal doc in the future

            for response_json in response.json():
                chunks = []
                for name, type_to_content in sorted(response_json['fields'].items()):
                    chunk = Document(**type_to_content)
                    if chunk.blob:
                        base64_bytes = chunk.blob.encode('utf-8')
                        chunk.blob = base64.decodebytes(base64_bytes)

                    chunks.append(chunk)
                doc = Document(
                    id=response_json['id'],
                    tags=response_json['tags'],
                    chunks=chunks,
                )
                for metric, value in response_json['scores'].items():
                    doc.scores[metric] = NamedScore(value=value['value'])
                docs.append(doc)
    except Exception:
        try:
            json_response = response.json()

            if response.status_code == 401:
                st.session_state.error_msg = json_response['detail']
            else:
                st.session_state.error_msg = json_response['message'].replace('"', "'")
        except Exception:
            st.session_state.error_msg = response.text
        return None

    st.session_state.error_msg = None

    if endpoint in ['search', 'suggestion']:
        return docs
    else:
        return response.json()


def multimodal_search(
    query_field_values_modalities: List[Dict],
    jwt,
    top_k=None,
    filter_dict=None,
    endpoint='search',
):
    params = get_query_params()
    url_host = f"http://localhost:{NOWGATEWAY_BFF_PORT}/api/v1/search-app/{endpoint}"

    updated_dict = {}
    if filter_dict:
        updated_dict = {k: v for k, v in filter_dict.items() if v != 'All'}
    data = {
        'limit': top_k if top_k else params.top_k,
        'filters': updated_dict,
        'score_calculation': list(st.session_state.score_calculation.values()),
        'get_score_breakdown': st.session_state.show_score_breakdown,
    }
    if endpoint == 'suggestion':
        data['text'] = query_field_values_modalities[0]['value']
    elif endpoint == 'search':
        data['query'] = query_field_values_modalities
        data['create_temp_link'] = True
        # in case the jwt is none, no jwt will be sent. This is the case when no authentication is used for that flow
    if jwt:
        data['jwt'] = jwt
    return call_flow(url_host, data, endpoint)
