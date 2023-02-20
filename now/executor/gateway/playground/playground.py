import base64
import gc
import io
import json
import os
from typing import List
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen

import extra_streamlit_components as stx
import requests
import streamlit as st
import streamlit.components.v1 as components
from better_profanity import profanity
from docarray import Document, DocumentArray
from docarray.typing import Image, Text
from streamlit.runtime import Runtime
from streamlit.runtime.scriptrunner import add_script_run_ctx
from streamlit.web.server.server import Server
from tornado.httputil import parse_cookie

from now.app.base.create_jcloud_name import create_jcloud_name
from now.constants import MODALITY_TO_MODELS, NOWGATEWAY_BFF_PORT
from now.executor.gateway.playground.src.constants import (
    BUTTONS,
    S3_DEMO_PATH,
    SSO_COOKIE,
    ds_set,
)
from now.executor.gateway.playground.src.search import (
    call_flow,
    get_query_params,
    multimodal_search,
)
from now.now_dataclasses import UserInput

dir_path = os.path.dirname(os.path.realpath(__file__))

# HEADER
st.set_page_config(page_title='NOW', page_icon='https://jina.ai/favicon.ico')
profanity.load_censor_words()


def convert_file_to_document(query):
    data = query.read()
    doc = Document(blob=data)
    return doc


@st.cache(allow_output_mutation=True, suppress_st_warning=True)
def get_cookie_manager():
    return stx.CookieManager()


cookie_manager = get_cookie_manager()


def _get_all_cookies() -> dict:
    session_id = add_script_run_ctx().streamlit_script_run_ctx.session_id
    session_info = Server.get_current()._get_session_info(session_id)
    header = session_info.ws.request.headers
    cookie_strings = [header_str for k, header_str in header.get_all() if k == 'Cookie']
    parsed_cookies = {k: v for c in cookie_strings for k, v in parse_cookie(c).items()}

    return parsed_cookies


def get_cookie_value(cookie_name):
    all_cookies = _get_all_cookies()
    for k, v in all_cookies.items():
        if k == cookie_name:
            return v


def nav_to(url):
    nav_script = """
        <meta http-equiv="refresh" content="0; url='%s'">
    """ % (
        url
    )
    st.write(nav_script, unsafe_allow_html=True)


def toggle_score_breakdown():
    if st.checkbox('Show score breakdown', key='scores'):
        st.session_state.show_score_breakdown = True
    else:
        st.session_state.show_score_breakdown = False


def deploy_streamlit(user_input: UserInput):
    """
    We want to provide the end-to-end experience to the user.
    Please deploy a streamlit playground on k8s/local to access the api.
    You can get the starting point for the streamlit application from alex.
    """
    # Start with setting up the vars default values then proceed to placing UI components
    # Set up session state vars if not already set
    setup_session_state()

    # Retrieve query params
    params = get_query_params()
    # update query params with user input
    for key, val in user_input.dict().items():
        setattr(params, key, val)
    redirect_to = render_auth_components(params)

    _, mid, _ = st.columns([0.8, 1, 1])
    with open(os.path.join(dir_path, 'logo.svg'), 'r') as f:
        svg = f.read()
    with mid:
        b64 = base64.b64encode(svg.encode('utf-8')).decode('utf-8')
        html = r'<img width="250" src="data:image/svg+xml;base64,%s"/>' % b64
        st.write(html, unsafe_allow_html=True)

    if redirect_to and st.session_state.login:
        nav_to(redirect_to)
    else:
        da_img, da_txt = load_example_queries(params.data)

        setup_design()

        if st.session_state.filters == 'notags':
            try:
                tags = get_info_from_endpoint(params, endpoint='tags')
                st.session_state.filters = tags
            except Exception as e:
                print("Filters couldn't be loaded from the endpoint properly.", e)
                st.session_state.filters = 'notags'
        if not st.session_state.index_fields_dict:
            st.session_state.index_fields_dict = get_info_from_endpoint(
                params, 'encoder_to_dataclass_fields_mods'
            )
            st.session_state.field_names_to_dataclass_fields = (
                user_input.field_names_to_dataclass_fields
            )

        filter_selection = {}
        if st.session_state.filters != 'notags':
            st.sidebar.title('Filters')
            if not st.session_state.filters_set:
                for tag, values in st.session_state.filters.items():
                    values.insert(0, 'All')
                    filter_selection[tag] = st.sidebar.selectbox(tag, values)
                st.session_state.filters_set = True
            else:
                for tag, values in st.session_state.filters.items():
                    filter_selection[tag] = st.sidebar.selectbox(tag, values)

        if st.session_state.filters != 'notags' and not st.session_state.filters_set:
            st.sidebar.title('Filters')
            for tag, values in st.session_state.filters.items():
                values.insert(0, 'All')
                filter_selection[tag] = st.sidebar.selectbox(tag, values)
        l, r = st.columns([5, 5])
        with l:
            st.header('Text')
            render_mm_query(st.session_state['query'], 'text')
        with r:
            st.header('Image')
            render_mm_query(st.session_state['query'], 'image')

        customize_semantic_scores()
        toggle_score_breakdown()
        search_mapping_list = list(st.session_state['query'].values())
        if any([d['value'] for d in search_mapping_list]):
            st.session_state.matches = multimodal_search(
                query_field_values_modalities=list(
                    filter(lambda x: x['value'], search_mapping_list)
                ),
                jwt=st.session_state.jwt_val,
                filter_dict=filter_selection,
            )
        render_matches()

        add_social_share_buttons()


def get_info_from_endpoint(params, endpoint) -> dict:
    parameters = {}
    if params.secured:
        parameters['jwt'] = {'token': st.session_state.jwt_val['token']}
    response = call_flow(
        f'http://localhost:{NOWGATEWAY_BFF_PORT}/api/v1/info/{endpoint}',
        parameters,
        endpoint,
    )
    # return the first value of the response as it's a dict with one key
    return list(response.values())[0]


# hack to access cookies in streamlit: https://github.com/streamlit/streamlit/issues/5166#issuecomment-1259901215
# can also use https://gist.github.com/asehmi/85c293df44d32957fb31a7cd332c3398 but this one is shorter
def st_runtime():

    global _st_runtime

    if _st_runtime:
        return _st_runtime

    for obj in gc.get_objects():
        if type(obj) is Runtime:
            _st_runtime = obj
            return _st_runtime


_st_runtime = None
runtime = st_runtime()


def get_cookie(cookie_name):
    session_id = add_script_run_ctx().streamlit_script_run_ctx.session_id
    session_info = runtime._get_session_info(session_id)
    header = session_info.client.request.headers
    cookie_strings = [header_str for k, header_str in header.get_all() if k == 'Cookie']
    parsed_cookies = {k: v for c in cookie_strings for k, v in parse_cookie(c).items()}

    return parsed_cookies.get(cookie_name, '')


def render_auth_components(params):
    if params.secured:
        st_cookie = get_cookie(SSO_COOKIE)
        resp_jwt = requests.get(
            url=f'https://api.hubble.jina.ai/v2/rpc/user.identity.whoami',
            cookies={SSO_COOKIE: st_cookie},
        ).json()
        redirect_to = None
        if resp_jwt['code'] != 200:
            redirect_to = _do_login(params)
        else:
            st.session_state.login = False
            if not st.session_state.jwt_val:
                new_resp = {'token': st_cookie, 'user': resp_jwt['data']}
                st.session_state.jwt_val = new_resp
            if not st.session_state.avatar_val:
                st.session_state.avatar_val = resp_jwt['data']['avatarUrl']
            if not st.session_state.token_val:
                st.session_state.token_val = st_cookie

        if not st.session_state.jwt_val:
            redirect_to = _do_login(params)
        _, logout, avatar = st.columns([0.7, 0.12, 0.12])
        if not st.session_state.login:
            with avatar:
                if st.session_state.avatar_val:
                    st.image(st.session_state.avatar_val, width=30)
            with logout:
                st.button('Logout', on_click=_do_logout)
        return redirect_to
    else:
        return None


def _do_login(params):
    flow_namespace = os.environ.get("K8S_NAMESPACE_NAME", "").split('-')[1]
    jcloud_name = create_jcloud_name(params.flow_name) + '-' + flow_namespace
    host = f'https://{jcloud_name}-http.wolf.jina.ai/playground'
    redirect_uri = f'{host}/playground/'
    if 'top_k' in st.experimental_get_query_params():
        redirect_uri += f'?top_k={params.top_k}'

    redirect_uri = quote(redirect_uri)
    redirect_uri = (
        'https://api.hubble.jina.ai/v2/oidc/authorize?prompt=login&target_link_uri='
        + redirect_uri
    )
    st.session_state.login = True
    return redirect_uri


def _do_logout():
    headers = {
        'Content-Type': 'application/json; charset=utf-8',
        'Authorization': 'Token ' + st.session_state.token_val,
    }
    st.session_state.jwt_val = None
    st.session_state.avatar_val = None
    st.session_state.token_val = None
    st.session_state.login = True
    requests.post(
        'https://api.hubble.jina.ai/v2/rpc/user.session.dismiss',
        headers=headers,
    )


def load_example_queries(data):
    da_img = None
    da_txt = None
    if data in ds_set:
        try:
            da_img = load_data(S3_DEMO_PATH + data + f'.img10.bin')
            da_txt = load_data(S3_DEMO_PATH + data + f'.txt10.bin')
        except HTTPError as exc:
            print('Could not load samples for the demo dataset', exc)
    return da_img, da_txt


def setup_design():
    class UI:
        about_block = """
        ### About
        This is a meme search engine using [Jina's neural search framework](https://github.com/jina-ai/jina/).
        - [Live demo](https://examples.jina.ai/memes)
        - [Play with it in a notebook](https://colab.research.google.com/github/jina-ai/workshops/blob/main/memes/meme_search.ipynb) (t-only)
        - [Repo](https://github.com/alexcg1/jina-meme-search)
        - [Dataset](https://www.kaggle.com/abhishtagatya/imgflipscraped-memes-caption-dataset)
        """

        css = """
        <style>
            .reportview-container .main .block-container{{
                max-width: 1200px;
                padding-top: 2rem;
                padding-right: 2rem;
                padding-left: 2rem;
                padding-bottom: 2rem;
            }}
            .reportview-container .main {{
                color: "#111";
                background-color: "#eee";
            }}
        </style>
        """

    # Layout
    st.markdown(
        body=UI.css,
        unsafe_allow_html=True,
    )
    # design and create toggle button
    st.write(
        '<style>div.row-widget.stRadio > div{flex-direction:row;justify-content: center;} </style>',
        unsafe_allow_html=True,
    )
    st.write(
        '<style>div.st-bf{flex-direction:column;} div.st-ag{font-weight:bold;padding-right:50px;}</style>',
        unsafe_allow_html=True,
    )


def delete_semantic_scores():
    st.session_state['len_semantic_scores'] = 0
    st.session_state.semantic_scores = {}


def toggle_bm25_slider():
    if st.session_state.show_bm25_slider:
        st.session_state.show_bm25_slider = False
    else:
        st.session_state.show_bm25_slider = True


def get_encoder_options(q_field: str, id_field: str) -> List[str]:
    encoders_options = [
        encoder
        for encoder in st.session_state.index_fields_dict.keys()
        if id_field in st.session_state.index_fields_dict[encoder].keys()
    ]
    if q_field.startswith('image'):
        modality_models = [model['value'] for model in MODALITY_TO_MODELS[Image]]
    elif q_field.startswith('text'):
        modality_models = [model['value'] for model in MODALITY_TO_MODELS[Text]]
    else:
        raise ValueError(f'Unknown modality for field {q_field}')
    return list(set(encoders_options) & set(modality_models))


def customize_semantic_scores():
    input_modalities = [
        field['modality'] for field in list(st.session_state.query.values())
    ]
    add, delete, bm25 = st.columns([0.3, 0.3, 0.3])
    if add.button('Add semantic score', key='sem_score_button'):
        st.session_state['len_semantic_scores'] += 1
    if st.session_state.len_semantic_scores > 0:
        delete.button(
            label='Delete all semantic scores',
            key='delete',
            on_click=delete_semantic_scores,
        )
    if 'text' in input_modalities and any(
        field_mod == 'text'
        for field_mod in [
            st.session_state.index_fields_dict[encoder][field]
            for encoder in st.session_state.index_fields_dict.keys()
            for field in st.session_state.index_fields_dict[encoder].keys()
        ]
    ):
        bm25.button('Add bm25 score', key='bm25', on_click=toggle_bm25_slider)
        if st.session_state.show_bm25_slider:
            query_field_selectbox, bm25_slider = st.columns([0.5, 0.5])
            q_field = query_field_selectbox.selectbox(
                'Select query field for bm25 scoring',
                options=[
                    field
                    for field in st.session_state.query.keys()
                    if field.startswith('text')
                ],
            )
            bm25_weight = bm25_slider.slider(
                label='Adjust bm25 weight',
                min_value=0.0,
                max_value=1.0,
                value=0.5,
                key='weight_bm25',
            )
            st.session_state.semantic_scores['bm25'] = [
                q_field,
                'bm25_text',
                'bm25',
                bm25_weight,
            ]

    for i in range(st.session_state['len_semantic_scores']):
        query_field, index_field, encoder, weight = st.columns([0.25, 0.25, 0.25, 0.25])
        q_field = query_field.selectbox(
            label='query field',
            options=list(st.session_state.query.keys()),
            key='query_field_' + str(i),
        )
        id_field = index_field.selectbox(
            label='index field',
            options=list(st.session_state.field_names_to_dataclass_fields.keys()),
            key='index_field_' + str(i),
        )
        id_field = st.session_state.field_names_to_dataclass_fields[id_field]
        encoder_options = get_encoder_options(q_field, id_field)
        enc = encoder.selectbox(
            label='encoder',
            options=encoder_options,
            key='encoder_' + str(i),
        )
        w = weight.slider(
            label='weight',
            min_value=0.0,
            max_value=1.0,
            value=0.5,
            key='weight_' + str(i),
        )
        st.session_state.semantic_scores[f'{i}'] = (q_field, id_field, enc, w)


def render_mm_query(query, modality):
    if st.button("\+", key=f'add_{modality}_field'):  # noqa: W605
        st.session_state[f"len_{modality}_choices"] += 1
    if modality == 'text':
        for field_number in range(st.session_state[f"len_{modality}_choices"]):
            key = f'{modality}_{field_number}'
            query[key] = {
                'name': key,
                'value': st.text_input(
                    label=f'text #{field_number}',
                    key=key,
                    on_change=clear_match,
                    placeholder=f'Write your text query #{field_number}',
                ),
                'modality': 'text',
            }

    else:
        for field_number in range(st.session_state[f"len_{modality}_choices"]):
            key = f'{modality}_{field_number}'
            uploaded_image = st.file_uploader(
                label=f'image #{field_number}', key=key, on_change=clear_match
            )
            if uploaded_image:
                doc = convert_file_to_document(uploaded_image)
                query_doc = doc
                if query_doc.blob == b'':
                    if query_doc.tensor is not None:
                        query_doc.convert_image_tensor_to_blob()
                    elif (query_doc.uri is not None) and query_doc.uri != '':
                        query_doc.load_uri_to_blob(timeout=10)
                query[key] = {
                    'name': key,
                    'value': base64.b64encode(query_doc.blob).decode('utf-8'),
                    'modality': 'image',
                }
    if st.session_state[f"len_{modality}_choices"] >= 1:
        st.button(
            label=f'\-',  # noqa: W605
            key=f'remove_{modality}_field',
            on_click=decrement_inputs,
            kwargs=dict(modality=modality),
        )


def render_matches():
    # TODO function is too large. Split up.
    if st.session_state.matches and not st.session_state.error_msg:
        list_matches = [
            st.session_state.matches[i : i + 9]
            for i in range(0, len(st.session_state.matches), 9)
        ]

        # render the current page or the last page if filtered documents are less
        if list_matches:
            st.session_state.page_number = min(
                st.session_state.page_number, len(list_matches) - 1
            )
            st.header('Search results')
            # Results area
            c1, c2, c3 = st.columns(3)
            c4, c5, c6 = st.columns(3)
            c7, c8, c9 = st.columns(3)
            all_cs = [c1, c2, c3, c4, c5, c6, c7, c8, c9]
            for c, match in zip(all_cs, list_matches[st.session_state.page_number]):
                render_multi_modal_result(match, c)

        if len(list_matches) > 1:
            # disable prev button or not
            if st.session_state.page_number <= 0:
                st.session_state.disable_prev = True
            else:
                st.session_state.disable_prev = False

            # disable next button or not
            if st.session_state.page_number + 1 >= len(list_matches):
                st.session_state.disable_next = True
            else:
                st.session_state.disable_next = False

            prev, _, page, _, next = st.columns([1, 4, 2, 4, 1])
            page.write(f'Page {st.session_state.page_number + 1}/{len(list_matches)}')
            next.button(
                'Next', disabled=st.session_state.disable_next, on_click=increment_page
            )
            prev.button(
                'Previous',
                disabled=st.session_state.disable_prev,
                on_click=decrement_page,
            )

    if st.session_state.error_msg:
        with st.expander(
            'Received error response from the server. Expand this to see the full error message'
        ):
            st.text(st.session_state.error_msg)


def render_multi_modal_result(match, c):
    for chunk in match.chunks:
        render_graphic_result(chunk, c)
        render_text_result(chunk, c)
    if st.session_state.show_score_breakdown:
        render_score_breakdown(match, c)


# I'm not so happy about these two functions, let's refactor them
def render_graphic_result(match, c):
    try:
        match.mime_type = 'text-or-image-or-video'
        if match.blob:
            match.convert_blob_to_datauri()
        elif match.tensor is not None:
            match.convert_image_tensor_to_uri()
        c.image(match.uri)
    except:
        pass


def render_text_result(match, c):
    try:
        if match.text == '' and match.uri != '':
            match.load_uri_to_text(timeout=10)
        display_text = profanity.censor(match.text).replace('\n', ' ')
        body = f"<!DOCTYPE html><html><body>{display_text}</body></html>"
        c.markdown(
            body=body,
            unsafe_allow_html=True,
        )
    except:
        pass


def render_score_breakdown(match, c):
    display_scores_string = "<br>".join(
        sorted(
            [
                name + ": " + str(round(score.value, 3))
                for name, score in match.scores.items()
            ]
        )
    )
    body = f'<!DOCTYPE html><html><body><p style = "font-size:10px;">{display_scores_string}</body></html>'
    c.markdown(
        body=body,
        unsafe_allow_html=True,
    )


def add_social_share_buttons():
    # Adding social share buttons
    _, twitter, linkedin, facebook = st.columns([0.55, 0.12, 0.12, 0.12])
    for column, name in [
        (twitter, 'twitter'),
        (linkedin, 'linkedin'),
        (facebook, 'facebook'),
    ]:
        with column:
            components.html(BUTTONS[name])


def display_song(attach_to, song_doc: Document):
    genre = (
        song_doc.tags['genre_tags'].split(' ')[:3]
        if isinstance(song_doc.tags['genre_tags'], str)
        else song_doc.tags['genre_tags'][:3]
    )
    attach_to.markdown(
        body=f"<!DOCTYPE html><html><body>"
        f"<p style=\"font-size: 20px; font-weight: 700; margin-bottom: -8px\">{song_doc.tags['name']}</p>"
        f"<p style=\"margin-bottom: -5px\">{song_doc.tags['artist']}</p>"
        f"<p style=\"font-size: 10px\">{' | '.join(genre)}</p>"
        f"</body></html>",
        unsafe_allow_html=True,
    )
    if 'album_cover_image_url' in song_doc.tags:
        attach_to.markdown(
            body="<!DOCTYPE html><html><body>"
            f"<img/ src={song_doc.tags['album_cover_image_url']} style=\"width: 200px; height: 200px; padding: 10px 0 10px 0\">"
            "</body></html>",
            unsafe_allow_html=True,
        )
    attach_to.audio(io.BytesIO(song_doc.blob))


def increment_page():
    st.session_state.page_number += 1


def decrement_page():
    st.session_state.page_number -= 1


def decrement_inputs(modality):
    st.session_state[f"len_{modality}_choices"] -= 1


def clear_match():
    st.session_state.matches = (
        None  # TODO move this to when we choose a suggestion or search button
    )
    st.session_state.slider = 0.0
    st.session_state.snap = None
    st.session_state.error_msg = None
    st.session_state.page_number = 0


def clear_text():
    st.session_state.text_search_box = ''
    clear_match()


def load_data(data_path: str) -> DocumentArray:
    if data_path.startswith('http'):
        os.makedirs('data/tmp', exist_ok=True)
        url = data_path
        data_path = (
            f"data/tmp/{base64.b64encode(bytes(url, 'utf-8')).decode('utf-8')}.bin"
        )
        if not os.path.exists(data_path):
            with urlopen(url) as f:
                content = f.read()
            with open(data_path, 'wb') as f:
                f.write(content)

    try:
        da = DocumentArray.load_binary(data_path)
    except Exception:
        da = DocumentArray.load_binary(data_path, compress='gzip')
    return da


def get_login_button(url):
    return (
        f'<a href="{url}" target="_self" class="button">'
        + 'Login'
        + """<style>
                                    .button {
                                      margin-top: -50px;
                                      position: relative;
                                      overflow: hidden;
                                      -webkit-transition: background 400ms;
                                      transition: background 400ms;
                                      color: #fff;
                                      background-color: #90ee90;
                                      padding: 0.5em 0.5rem;
                                      font-family: 'Roboto', sans-serif;
                                      font-size: 1.0rem;
                                      outline: 0;
                                      border: 0;
                                      border-radius: 0.05rem;
                                      -webkit-box-shadow: 0 0 0.5rem rgba(0, 0, 0, 0.3);
                                      box-shadow: 0 0 0.5rem rgba(0, 0, 0, 0.3);
                                      cursor: pointer;
                                    }
                                    </style>
                                """
        + '</a>'
    )


def get_logout_button(url):
    return (
        f'<a href="{url}" target="_self" class="button">'
        + 'Logout'
        + """<style>
                                    .button {
                                      margin-top: -50px;
                                      position: relative;
                                      overflow: hidden;
                                      -webkit-transition: background 400ms;
                                      transition: background 400ms;
                                      color: #fff;
                                      background-color: #90ee90;
                                      padding: 0.5em 0.5rem;
                                      font-family: 'Roboto', sans-serif;
                                      font-size: 1.0rem;
                                      outline: 0;
                                      border: 0;
                                      border-radius: 0.05rem;
                                      -webkit-box-shadow: 0 0 0.5rem rgba(0, 0, 0, 0.3);
                                      box-shadow: 0 0 0.5rem rgba(0, 0, 0, 0.3);
                                      cursor: pointer;
                                    }
                                    </style>
                                """
        + '</a>'
    )


def setup_session_state():
    if 'matches' not in st.session_state:
        st.session_state.matches = None

    if 'im' not in st.session_state:
        st.session_state.im = None

    if 'snap' not in st.session_state:
        st.session_state.snap = None

    if 'search_count' not in st.session_state:
        st.session_state.search_count = 0

    if 'jwt_val' not in st.session_state:
        st.session_state.jwt_val = None

    if 'avatar_val' not in st.session_state:
        st.session_state.avatar_val = None

    if 'token_val' not in st.session_state:
        st.session_state.token_val = None

    if 'login' not in st.session_state:
        st.session_state.login = False

    if 'error_msg' not in st.session_state:
        st.session_state.error_msg = None

    if 'page_number' not in st.session_state:
        st.session_state.page_number = 0

    if 'disable_next' not in st.session_state:
        st.session_state.disable_next = True

    if 'disable_prev' not in st.session_state:
        st.session_state.disable_prev = True

    if 'filters' not in st.session_state:
        st.session_state.filters = 'notags'

    if 'filters_set' not in st.session_state:
        st.session_state.filters_set = False

    if "len_text_choices" not in st.session_state:
        st.session_state["len_text_choices"] = 1

    if "len_image_choices" not in st.session_state:
        st.session_state["len_image_choices"] = 1

    if "query" not in st.session_state:
        st.session_state['query'] = dict()

    if 'len_semantic_scores' not in st.session_state:
        st.session_state['len_semantic_scores'] = 0

    if 'index_fields_dict' not in st.session_state:
        st.session_state.index_fields_dict = {}

    if 'field_names_to_dataclass_fields' not in st.session_state:
        st.session_state.field_names_to_dataclass_fields = {}

    if 'encoder' not in st.session_state:
        st.session_state.encoder = 'clip'

    if 'semantic_scores' not in st.session_state:
        st.session_state.semantic_scores = {}

    if 'show_bm25_slider' not in st.session_state:
        st.session_state.show_bm25_slider = False

    if 'show_score_breakdown' not in st.session_state:
        st.session_state.show_score_breakdown = False


if __name__ == '__main__':
    # read user_input from user_input.json in the home directory
    with open(os.path.join(os.path.expanduser('~'), 'user_input.json'), 'r') as f:
        user_input_dict = json.load(f)

    user_input = UserInput()
    for attr_name, prev_value in user_input.__dict__.items():
        setattr(
            user_input,
            attr_name,
            user_input_dict.get(attr_name, prev_value),
        )

    deploy_streamlit(user_input=user_input)
