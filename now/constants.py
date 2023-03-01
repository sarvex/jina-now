from __future__ import annotations, print_function, unicode_literals

from docarray.typing import Image, Text, Video

from now.utils import BetterEnum

NOW_GATEWAY_VERSION = '0.0.4-fix-use-spot-0'
NOW_PREPROCESSOR_VERSION = '0.0.124-fix-use-spot-0'
NOW_ELASTIC_INDEXER_VERSION = '0.0.148-fix-use-spot-0'
NOW_AUTOCOMPLETE_VERSION = '0.0.11-fix-use-spot-0'


class Apps(BetterEnum):
    SEARCH_APP = 'search_app'


class DialogStatus(BetterEnum):
    CONTINUE = 'continue'
    BREAK = 'break'
    SKIP = 'skip'


class DatasetTypes(BetterEnum):
    DEMO = 'demo'
    PATH = 'path'
    DOCARRAY = 'docarray'
    S3_BUCKET = 's3_bucket'
    ELASTICSEARCH = 'elasticsearch'


class Models(BetterEnum):
    CLIP_MODEL = 'encoderclip'
    SBERT_MODEL = 'encodersbert'


SUPPORTED_FILE_TYPES = {
    Text: ['txt', 'md'],
    Image: ['jpg', 'jpeg', 'png', 'bmp', 'tiff', 'tif'],
    Video: ['gif'],
}

FILETYPE_TO_MODALITY = {
    filetype: modality
    for modality, filetypes in SUPPORTED_FILE_TYPES.items()
    for filetype in filetypes
}
AVAILABLE_MODALITIES_FOR_SEARCH = [Text, Image, Video]
AVAILABLE_MODALITIES_FOR_FILTER = [Text]
NOT_AVAILABLE_MODALITIES_FOR_FILTER = [Image, Video]

BASE_STORAGE_URL = (
    'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets'
)

EXTERNAL_CLIP_HOST = 'encoderclip-pretty-javelin-3aceb7f2cd.wolf.jina.ai'
EXTERNAL_SBERT_HOST = 'encodersbert-flow-external-sbert-5cc8f2c38e.wolf.jina.ai'

DEFAULT_FLOW_NAME = 'nowapi'
PREFETCH_NR = 10
NUM_FOLDERS_THRESHOLD = 100
MAX_DOCS_FOR_TESTING = 50
SURVEY_LINK = 'https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli'

TAG_INDEXER_DOC_HAS_TEXT = '_indexer_doc_has_text'
ACCESS_PATHS = '@cc'
FLOW_STATUS = 'Serving'
DEMO_NS = 'now-example-{}'

MODALITY_TO_MODELS = {
    Text: [
        {'name': 'Clip', 'value': Models.CLIP_MODEL},
        {'name': 'Sbert', 'value': Models.SBERT_MODEL},
    ],
    Image: [{'name': 'Clip', 'value': Models.CLIP_MODEL}],
    Video: [{'name': 'Clip', 'value': Models.CLIP_MODEL}],
}

NOWGATEWAY_BFF_PORT = 8080
