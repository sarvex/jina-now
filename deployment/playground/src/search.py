import base64
import functools
from collections.abc import Collection, Hashable, Mapping

import requests
import streamlit as st
from docarray import Document, DocumentArray
from frozendict import frozendict

from .constants import Parameters


def deep_freeze(thing):
    if thing is None or isinstance(thing, str):
        return thing
    elif isinstance(thing, Mapping):
        return frozendict({k: deep_freeze(v) for k, v in thing.items()})
    elif isinstance(thing, Collection):
        return tuple(deep_freeze(i) for i in thing)
    elif not isinstance(thing, Hashable):
        raise TypeError(f"un-freezable type: '{type(thing)}'")
    else:
        return thing


def deep_freeze_args(func):
    """
    Transform mutable dictionary into immutable. Useful to be compatible with cache.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return func(*deep_freeze(args), **deep_freeze(kwargs))

    return wrapped


def unfreeze_param(data):
    new_data = {}
    for k, v in data.items():
        if isinstance(v, frozendict):
            d = unfreeze_param(v)
            new_data[k] = dict(d)
        else:
            new_data[k] = v
    return new_data


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


def search(attribute_name, attribute_value, jwt, top_k=None, filter_dict=None):
    print(f'Searching by {attribute_name}')
    params = get_query_params()
    if params.host == 'gateway':  # need to call now-bff as we communicate between pods
        domain = f"http://now-bff"
    else:
        domain = f"https://nowrun.jina.ai"
    URL_HOST = (
        f"{domain}/api/v1/{params.input_modality}-to-{params.output_modality}/search"
    )

    updated_dict = {k: v for k, v in filter_dict.items() if v != 'All'}

    data = {
        'host': params.host,
        attribute_name: attribute_value,
        'limit': top_k if top_k else params.top_k,
        'filters': updated_dict if updated_dict else {},
    }
    # in case the jwt is none, no jwt will be sent. This is the case when no authentication is used for that flow
    if jwt is not None:
        data['jwt'] = jwt
    if params.port:
        data['port'] = params.port

    return call_flow(URL_HOST, data, attribute_name, domain)


@deep_freeze_args
@functools.lru_cache(maxsize=10, typed=False)
def call_flow(url_host, data, attribute_name, domain):
    st.session_state.search_count += 1
    data = unfreeze_param(data)

    response = requests.post(
        url_host, json=data, headers={"Content-Type": "application/json; charset=utf-8"}
    )

    try:
        docs = DocumentArray.from_json(response.content)
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


def search_by_text(search_text, jwt, filter_selection) -> DocumentArray:
    return search('text', search_text, jwt, filter_dict=filter_selection)


def search_by_image(document: Document, jwt, filter_selection) -> DocumentArray:
    """
    Wrap file in Jina Document for searching, and do all necessary conversion to make similar to indexed Docs
    """
    query_doc = document
    if query_doc.blob == b'':
        if query_doc.tensor is not None:
            query_doc.convert_image_tensor_to_blob()
        elif (query_doc.uri is not None) and query_doc.uri != '':
            query_doc.load_uri_to_blob()

    return search(
        'image',
        base64.b64encode(query_doc.blob).decode('utf-8'),
        jwt,
        filter_dict=filter_selection,
    )


def search_by_audio(document: Document, jwt, filter_selection):
    params = get_query_params()
    TOP_K = params.top_k
    result = search(
        'song',
        base64.b64encode(document.blob).decode('utf-8'),
        jwt,
        TOP_K * 3,
        filter_dict=filter_selection,
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
