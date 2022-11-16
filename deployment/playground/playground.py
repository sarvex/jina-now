import base64
import io
import json
import os
from collections import OrderedDict
from copy import deepcopy
from urllib.error import HTTPError
from urllib.parse import quote, unquote
from urllib.request import urlopen

import av
import extra_streamlit_components as stx
import requests
import streamlit as st
import streamlit.components.v1 as components
from better_profanity import profanity
from docarray import Document, DocumentArray
from jina import Client
from src.constants import (
    BUTTONS,
    JWT_COOKIE,
    RTC_CONFIGURATION,
    SURVEY_LINK,
    ds_set,
    root_data_dir,
)
from src.search import (
    get_query_params,
    search_by_audio,
    search_by_image,
    search_by_text,
)
from streamlit.scriptrunner import add_script_run_ctx
from streamlit.server.server import Server
from streamlit_webrtc import WebRtcMode, webrtc_streamer
from tornado.httputil import parse_cookie

dir_path = os.path.dirname(os.path.realpath(__file__))

# TODO: Uncomment the docarray_version when the file name on GCloud has been changed
# from docarray import __version__ as docarray_version
docarray_version = '0.13.17'

# HEADER
st.set_page_config(page_title="NOW", page_icon='https://jina.ai/favicon.ico')


def convert_file_to_document(query):
    data = query.read()
    doc = Document(blob=data)
    return doc


def load_music_examples(DATA) -> DocumentArray:
    ds_url = root_data_dir + 'music/' + DATA + f'-song5-{docarray_version}.bin'
    return load_data(ds_url)[0, 1, 4]


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


def deploy_streamlit():
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
    redirect_to = render_auth_components(params)

    _, mid, _ = st.columns([0.8, 1, 1])
    with open(os.path.join(dir_path, 'logo.svg'), 'r') as f:
        svg = f.read()
    with mid:
        b64 = base64.b64encode(svg.encode('utf-8')).decode("utf-8")
        html = r'<img width="250" src="data:image/svg+xml;base64,%s"/>' % b64
        st.write(html, unsafe_allow_html=True)

    if redirect_to and st.session_state.login:
        nav_to(redirect_to)
    else:
        media_type = 'Text'

        da_img, da_txt = load_example_queries(params.data, params.output_modality)

        if params.output_modality == 'text':
            # censor words in text incl. in custom data
            from better_profanity import profanity

            profanity.load_censor_words()
        setup_design()

        if params.host and st.session_state.filters == 'notags':
            client = Client(host=params.host)
            try:
                if params.secured.lower() == 'true':
                    response = client.post(
                        on='/tags',
                        parameters={
                            'jwt': {'token': st.session_state.jwt_val['token']}
                        },
                    )
                else:
                    response = client.post(on='/tags')
                st.session_state.filters = OrderedDict(response[0].tags['tags'])
            except Exception as e:
                print("Filters couldn't be loaded from the endpoint properly.", e)
                st.session_state.filters = 'notags'

        filter_selection = {}
        if st.session_state.filters != 'notags':
            st.sidebar.title("Filters")
            if not st.session_state.filters_set:
                for tag, values in st.session_state.filters.items():
                    values.insert(0, 'All')
                    filter_selection[tag] = st.sidebar.selectbox(tag, values)
                st.session_state.filters_set = True
            else:
                for tag, values in st.session_state.filters.items():
                    filter_selection[tag] = st.sidebar.selectbox(tag, values)

        if st.session_state.filters != 'notags' and not st.session_state.filters_set:
            st.sidebar.title("Filters")
            for tag, values in st.session_state.filters.items():
                values.insert(0, 'All')
                filter_selection[tag] = st.sidebar.selectbox(tag, values)

        if params.input_modality == 'image':
            media_type = st.radio(
                '',
                ["Image", 'Webcam'],
                on_change=clear_match,
            )
        elif params.input_modality == 'text':
            media_type = 'Text'
        elif params.input_modality == 'music':
            media_type = 'Music'

        if media_type == "Image":
            render_image(da_img, deepcopy(filter_selection))

        elif media_type == "Text":
            render_text(da_txt, deepcopy(filter_selection))

        elif media_type == 'Webcam':
            render_webcam(deepcopy(filter_selection))

        elif media_type == 'Music':
            render_music_app(params.data, deepcopy(filter_selection))

        render_matches(params.output_modality)

        add_social_share_buttons()


def render_auth_components(params):
    if params.secured.lower() == 'true':
        jwt_val = get_cookie_value(cookie_name=JWT_COOKIE)
        if jwt_val and not st.session_state.login:
            jwt_val = json.loads(unquote(jwt_val))
            if not st.session_state.jwt_val:
                st.session_state.jwt_val = jwt_val
            if not st.session_state.avatar_val:
                st.session_state.avatar_val = jwt_val['user']['avatarUrl']
            if not st.session_state.token_val:
                st.session_state.token_val = jwt_val['token']
        redirect_to = None
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
    code = params.code
    state = params.state
    if code and state:
        # Whether it is fail or success, clear the query param
        query_params_var = {
            'host': unquote(params.host),
            'input_modality': params.input_modality,
            'output_modality': params.output_modality,
            'data': params.data,
        }
        if params.secured:
            query_params_var['secured'] = params.secured
        st.experimental_set_query_params(**query_params_var)

        resp_jwt = requests.get(
            url=f'https://api.hubble.jina.ai/v2/rpc/user.identity.grant.auto'
            f'?code={code}&state={state}'
        ).json()
        if resp_jwt and resp_jwt['code'] == 200:
            st.session_state.jwt_val = resp_jwt['data']
            st.session_state.token_val = resp_jwt['data']['token']
            st.session_state.avatar_val = resp_jwt['data']['user']['avatarUrl']
            st.session_state.login = False
            cookie_manager.set(
                cookie=JWT_COOKIE, val=st.session_state.jwt_val, key=JWT_COOKIE
            )
            return
        else:
            st.session_state.login = True
            params.code = None
            params.state = None

    st.session_state.login = True
    redirect_uri = (
        f'https://nowrun.jina.ai/?host={params.host}&input_modality={params.input_modality}'
        f'&output_modality={params.output_modality}&data={params.data}'
        + f'&secured={params.secured}'
        if params.secured
        else ''
    )
    redirect_uri = quote(redirect_uri)
    rsp = requests.get(
        url=f'https://api.hubble.jina.ai/v2/rpc/user.identity.authorize'
        f'?provider=jina-login&response_mode=query&redirect_uri={redirect_uri}&scope=email%20profile%20openid&prompt=login'
    ).json()
    redirect_to = rsp['data']['redirectTo']
    return redirect_to


def _do_logout():

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": 'Token ' + st.session_state.token_val,
    }
    st.session_state.jwt_val = None
    st.session_state.avatar_val = None
    st.session_state.token_val = None
    st.session_state.login = True
    response = requests.post(
        'https://api.hubble.jina.ai/v2/rpc/user.session.dismiss',
        headers=headers,
    )
    cookie_manager.delete(cookie=JWT_COOKIE, key=JWT_COOKIE)


def load_example_queries(data, output_modality):
    da_img = None
    da_txt = None
    if data in ds_set:
        try:
            if output_modality == 'image' or output_modality == 'video':
                output_modality_dir = 'jpeg'
                data_dir = root_data_dir + output_modality_dir + '/'
                da_img, da_txt = load_data(
                    data_dir + data + f'.img10-{docarray_version}.bin'
                ), load_data(data_dir + data + f'.txt10-{docarray_version}.bin')
            elif output_modality == 'text':
                # for now deactivated sample images for text
                output_modality_dir = 'text'
                data_dir = root_data_dir + output_modality_dir + '/'
                da_txt = load_data(data_dir + data + f'.txt10-{docarray_version}.bin')
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


def render_image(da_img, filter_selection):
    upload_c, preview_c = st.columns([12, 1])
    query = upload_c.file_uploader("", on_change=clear_match)
    if query:
        doc = convert_file_to_document(query)
        st.image(doc.blob, width=160)
        st.session_state.matches = search_by_image(
            document=doc,
            jwt=st.session_state.jwt_val,
            filter_selection=filter_selection,
        )
    if da_img is not None:
        st.subheader("samples:")
        img_cs = st.columns(5)
        txt_cs = st.columns(5)
        for doc, c, txt in zip(da_img, img_cs, txt_cs):
            with c:
                st.image(doc.blob if doc.blob else doc.tensor, width=100)
            with txt:
                if st.button('Search', key=doc.id, on_click=clear_match):
                    st.session_state.matches = search_by_image(
                        document=doc,
                        jwt=st.session_state.jwt_val,
                        filter_selection=filter_selection,
                    )


def render_text(da_txt, filter_selection):
    query = st.text_input("", key="text_search_box", on_change=clear_match)
    if query:
        st.session_state.matches = search_by_text(
            search_text=query,
            jwt=st.session_state.jwt_val,
            filter_selection=filter_selection,
        )
    if st.button("Search", key="text_search", on_click=clear_match):
        st.session_state.matches = search_by_text(
            search_text=query,
            jwt=st.session_state.jwt_val,
            filter_selection=filter_selection,
        )

    if da_txt is not None:
        st.subheader("samples:")
        c1, c2, c3 = st.columns(3)
        c4, c5, c6 = st.columns(3)
        for doc, col in zip(da_txt, [c1, c2, c3, c4, c5, c6]):
            with col:
                if st.button(doc.content, key=doc.id, on_click=clear_text):
                    st.session_state.matches = search_by_text(
                        search_text=doc.content,
                        jwt=st.session_state.jwt_val,
                        filter_selection=filter_selection,
                    )


def render_matches(OUTPUT_MODALITY):
    # TODO function is too large. Split up.
    if st.session_state.matches and not st.session_state.error_msg:
        if st.session_state.search_count > 2:
            st.write(
                f"🔥 How did you like Jina NOW? [Please leave feedback]({SURVEY_LINK}) 🔥"
            )
        # make a copy and  sort them based on scores
        matches: DocumentArray = deepcopy(st.session_state.matches)
        for m in matches:
            m.scores['cosine'].value = 1 - m.scores['cosine'].value
        sorted(matches, key=lambda m: m.scores['cosine'].value, reverse=True)

        # filter based on threshold and then group them for pagination
        matches = DocumentArray(
            [
                m
                for m in matches
                if m.scores['cosine'].value > st.session_state.min_confidence
            ]
        )
        list_matches = [matches[i : i + 9] for i in range(0, len(matches), 9)]

        # render the current page or the last page if filtered documents are less
        if len(list_matches) > 0:
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
                match.mime_type = OUTPUT_MODALITY

                if OUTPUT_MODALITY == 'text':
                    if match.text == '' and match.uri != '':
                        match.load_uri_to_text(timeout=10)
                    display_text = profanity.censor(match.text).replace('\n', ' ')
                    body = f"<!DOCTYPE html><html><body><blockquote>{display_text}</blockquote>"
                    if match.tags.get('additional_info'):
                        additional_info = match.tags.get('additional_info')
                        if type(additional_info) == str:
                            additional_info_text = additional_info
                        elif type(additional_info) == list:
                            if len(additional_info) == 1:
                                # assumes just one line containing information on text name and creator, etc.
                                additional_info_text = additional_info
                            elif len(additional_info) == 2:
                                # assumes first element is text name and second element is creator name
                                additional_info_text = (
                                    f"<em>{additional_info[0]}</em> "
                                    f"<small>by {additional_info[1]}</small>"
                                )

                            else:
                                additional_info_text = " ".join(additional_info)
                        body += f"<figcaption>{additional_info_text}</figcaption>"
                    body += "</body></html>"
                    c.markdown(
                        body=body,
                        unsafe_allow_html=True,
                    )

                elif OUTPUT_MODALITY == 'music':
                    if match.uri:
                        match.load_uri_to_blob(timeout=10)
                    display_song(c, match)

                elif OUTPUT_MODALITY in ('image', 'video'):
                    if match.blob != b'':
                        match.convert_blob_to_datauri()
                    elif match.tensor is not None:
                        match.convert_image_tensor_to_uri()

                    if match.uri != '':
                        c.image(match.uri)
                else:
                    raise ValueError(f'{OUTPUT_MODALITY} not handled')

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
                "Next", disabled=st.session_state.disable_next, on_click=increment_page
            )
            prev.button(
                "Previous",
                disabled=st.session_state.disable_prev,
                on_click=decrement_page,
            )

        st.markdown("""---""")
        st.session_state.min_confidence = st.slider(
            'Confidence threshold',
            0.0,
            1.0,
            key='slider',
            on_change=update_conf,
        )

    if st.session_state.error_msg:
        with st.expander(
            "Received error response from the server. Expand this to see the full error message"
        ):
            st.text(st.session_state.error_msg)


def render_music_app(DATA, filter_selection):
    st.header('Welcome to JinaNOW music search 👋🏽')
    st.text('Upload a song to search with or select one of the examples.')
    st.text('Pro tip: You can download search results and use them to search again :)')
    query = st.file_uploader("", type=['mp3', 'wav'])
    if query:
        doc = convert_file_to_document(query)
        st.subheader('Play your song')
        st.audio(doc.blob)
        st.session_state.matches = search_by_audio(
            document=doc,
            jwt=st.session_state.jwt_val,
            filter_selection=filter_selection,
        )

    else:
        columns = st.columns(3)
        music_examples = load_music_examples(DATA)

        def on_button_click(doc_id: str):
            def callback():
                st.session_state.matches = search_by_audio(
                    music_examples[doc_id],
                    jwt=st.session_state.jwt_val,
                    filter_selection=filter_selection,
                )

            return callback

        for c, song in zip(columns, music_examples):
            display_song(c, song)
            c.button('Search', on_click=on_button_click(song.id), key=song.id)


def render_webcam(filter_selection):
    snapshot = st.button('Snapshot', on_click=clear_match)

    class VideoProcessor:
        def __init__(self) -> None:
            self.img = None

        def recv(self, frame):
            self.img = frame.to_ndarray(format="rgb24")

            return av.VideoFrame.from_ndarray(self.img, format="rgb24")

    ctx = webrtc_streamer(
        key="jina-now",
        mode=WebRtcMode.SENDRECV,
        rtc_configuration=RTC_CONFIGURATION,
        media_stream_constraints={"video": True, "audio": False},
        video_processor_factory=VideoProcessor,
    )
    if ctx.state.playing:
        if snapshot:
            query = ctx.video_processor.img
            st.image(query, width=160)
            st.session_state.snap = query
            doc = Document(tensor=query)
            doc.convert_image_tensor_to_blob()
            st.session_state.matches = search_by_image(
                document=doc,
                jwt=st.session_state.jwt_val,
                filter_selection=filter_selection,
            )
        elif st.session_state.snap is not None:
            st.image(st.session_state.snap, width=160)
    else:
        clear_match()


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


def update_conf():
    st.session_state.min_confidence = st.session_state.slider


def clear_match():
    st.session_state.matches = (
        None  # TODO move this to when we choose a suggestion or search button
    )
    st.session_state.slider = 0.0
    st.session_state.min_confidence = 0.0
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

    if 'min_confidence' not in st.session_state:
        st.session_state.min_confidence = 0.0

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


if __name__ == '__main__':
    deploy_streamlit()
