import base64

import requests
import streamlit as st
from docarray import Document, DocumentArray

from .constants import TOP_K, Parameters


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


def search(attribute_name, attribute_value, jwt, top_k=TOP_K):
    print(f'Searching by {attribute_name}')
    st.session_state.search_count += 1
    params = get_query_params()

    if params.host == 'gateway':  # need to call now-bff as we communicate between pods
        domain = f"http://now-bff"
    else:
        domain = f"https://nowrun.jina.ai"
    URL_HOST = (
        f"{domain}/api/v1/{params.input_modality}-to-{params.output_modality}/search"
    )

    data = {
        'host': params.host,
        attribute_name: attribute_value,
        'limit': top_k,
    }
    # in case the jwt is none, no jwt will be sent. This is the case when no authentication is used for that flow
    if jwt is not None:
        data['jwt'] = jwt
    if params.port:
        data['port'] = params.port
    response = requests.post(
        URL_HOST, json=data, headers={"Content-Type": "application/json; charset=utf-8"}
    )
    if response.status_code == 401:
        st.session_state.error_msg = response.json()['detail']
        return None
    st.session_state.error_msg = None

    docs = DocumentArray.from_json(response.content)
    # update URI to temporary URI for any cloud bucket resources
    docs_cloud = docs.find({'uri': {'$regex': r"\As3://"}})
    if len(docs_cloud) > 0:
        del data[attribute_name]
        del data['limit']
        data['ids'] = docs_cloud[:, 'id']
        data['uris'] = docs_cloud[:, 'uri']

        response_temp_links = requests.post(
            f"{domain}/api/v1/cloud-bucket-utils/temp_link",
            json=data,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        docs_temp_links = DocumentArray.from_json(response_temp_links.content)
        for _id, _uri in zip(*docs_temp_links[:, ['id', 'uri']]):
            docs[_id].uri = _uri

    return docs


def search_by_text(search_text, jwt) -> DocumentArray:
    return search('text', search_text, jwt)


def search_by_image(document: Document, jwt) -> DocumentArray:
    """
    Wrap file in Jina Document for searching, and do all necessary conversion to make similar to indexed Docs
    """
    st.session_state.search_count += 1
    print(f"Searching by image")
    query_doc = document
    if query_doc.blob == b'':
        if query_doc.tensor is not None:
            query_doc.convert_image_tensor_to_blob()
        elif (query_doc.uri is not None) and query_doc.uri != '':
            query_doc.load_uri_to_blob()

    return search('image', base64.b64encode(query_doc.blob).decode('utf-8'), jwt)


def search_by_audio(document: Document, jwt):
    result = search(
        'song', base64.b64encode(document.blob).decode('utf-8'), jwt, TOP_K * 3
    )

    already_added_tracks = set()
    final_result = DocumentArray()
    for doc in result:
        if doc.tags['track_id'] in already_added_tracks or 'location' not in doc.tags:
            continue
        else:
            final_result.append(doc)
            already_added_tracks.add(doc.tags['track_id'])
        if len(final_result) >= TOP_K:
            break
    return final_result
