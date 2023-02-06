from __future__ import annotations, print_function, unicode_literals

from docarray.typing import Image, Text, Video

from now.utils import BetterEnum

DOCKER_BFF_PLAYGROUND_TAG = '0.0.153-fix-tags-in-bff-response-model-1'
NOW_PREPROCESSOR_VERSION = '0.0.121-clean-dataclass-creation-7'
NOW_ELASTIC_INDEXER_VERSION = '0.0.146-refactor-elastic-update-tags-3'
NOW_AUTOCOMPLETE_VERSION = '0.0.11-fix-av-requirements-2'


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

DEFAULT_FLOW_NAME = 'nowapi'
PREFETCH_NR = 10

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
