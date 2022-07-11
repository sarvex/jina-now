from streamlit_webrtc import ClientSettings


class Parameters:
    host: str
    port: str
    input_modality: str
    output_modality: str
    data: str


WEBRTC_CLIENT_SETTINGS = ClientSettings(
    rtc_configuration={"iceServers": [{"urls": ["stun:stun.l.google.com:19302"]}]},
    media_stream_constraints={"video": True, "audio": False},
)

root_data_dir = (
    'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/'
)

ds_set = [
    'nft-monkey',
    'deepfashion',
    'nih-chest-xrays',
    'stanford-cars',
    'bird-species',
    'best-artworks',
    'geolocation-geoguessr',
    'rock-lyrics',
    'pop-lyrics',
    'rap-lyrics',
    'indie-lyrics',
    'metal-lyrics',
]

SURVEY_LINK = 'https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli'
TOP_K = 9
