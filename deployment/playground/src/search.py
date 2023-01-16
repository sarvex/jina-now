import base64
from typing import Dict

import requests
import streamlit as st
from docarray import Document, DocumentArray
from docarray.score import NamedScore

from .constants import Parameters


def get_query_params() -> Parameters:
    query_parameters = st.experimental_get_query_params()
    parameters = Parameters()
    for key, val in query_parameters.items():
        if val is not None:
            setattr(
                parameters,
                key,
                val[0],
            )
    return parameters


def get_suggestion(text, jwt):
    return multimodal_search(
        {'text': (text, 'text')},
        jwt,
        endpoint='suggestion',
    )


def call_flow(url_host, data, domain, endpoint):
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

    return docs


def search_by_text(search_text, jwt, filter_selection) -> DocumentArray:
    return multimodal_search(
        {'text': (search_text, 'text')}, jwt, filter_dict=filter_selection
    )


def search_by_image(document: Document, jwt, filter_selection) -> DocumentArray:
    """
    Wrap file in Jina Document for searching, and do all necessary conversion to make similar to indexed Docs
    """
    query_doc = document
    if query_doc.blob == b'':
        if query_doc.tensor is not None:
            query_doc.convert_image_tensor_to_blob()
        elif (query_doc.uri is not None) and query_doc.uri != '':
            query_doc.load_uri_to_blob(timeout=10)

    return multimodal_search(
        {'blob': (base64.b64encode(query_doc.blob).decode('utf-8'), 'image')},
        jwt,
        filter_dict=filter_selection,
    )


def multimodal_search(
    query_field_values_modalities: Dict,
    jwt,
    top_k=None,
    filter_dict=None,
    endpoint='search',
):
    params = get_query_params()
    if params.host == 'gateway':  # need to call now-bff as we communicate between pods
        domain = f"http://now-bff"
    else:
        domain = f"https://nowrun.jina.ai"
    URL_HOST = f"{domain}/api/v1/search-app/{endpoint}"

    updated_dict = {}
    if filter_dict is not None:
        updated_dict = {k: v for k, v in filter_dict.items() if v != 'All'}
    data = {
        'host': params.host,
        'limit': top_k if top_k else params.top_k,
        'filters': updated_dict,
    }
    if endpoint == 'suggestion':
        data['text'] = query_field_values_modalities['text'][0]
    elif endpoint == 'search':
        data['query'] = {}
        for field_name, (
            field_value,
            field_modality,
        ) in query_field_values_modalities.items():
            data['query'][field_name] = [{field_modality: field_value}, field_modality]
        data['create_temp_link'] = True
        # in case the jwt is none, no jwt will be sent. This is the case when no authentication is used for that flow
    if jwt is not None:
        data['jwt'] = jwt
    if params.port:
        data['port'] = params.port
    print(data)
    return call_flow(URL_HOST, data, domain, endpoint)
