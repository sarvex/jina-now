from __future__ import annotations, print_function, unicode_literals

from now.utils import BetterEnum

# TODO: Uncomment the DEMO_DATASET_DOCARRAY_VERSION when the DocumentArray datasets on GCloud has been changed
# from docarray import __version__ as docarray_version

DEMO_DATASET_DOCARRAY_VERSION = '0.13.17'
# ----------------------------------
DOCKER_BFF_PLAYGROUND_TAG = '0.0.135-refactor-output_modality-29'
# ----------------------------------
NOW_PREPROCESSOR_VERSION = '0.0.114-refactor-output_modality-29'
NOW_QDRANT_INDEXER_VERSION = '0.0.11-enable-local-testing-4'
NOW_ELASTIC_INDEXER_VERSION = '0.0.10-enable-local-testing-4'
NOW_AUTOCOMPLETE_VERSION = '0.0.6-enable-local-testing-4'


class Modalities(BetterEnum):
    TEXT = 'text'
    IMAGE = 'image'
    VIDEO = 'video'


class Apps(BetterEnum):
    IMAGE_TEXT_RETRIEVAL = 'image_text_retrieval'
    TEXT_TO_VIDEO = 'text_to_video'


class DatasetTypes(BetterEnum):
    DEMO = 'demo'
    PATH = 'path'
    DOCARRAY = 'docarray'
    S3_BUCKET = 's3_bucket'
    ELASTICSEARCH = 'elasticsearch'


class ModelNames(BetterEnum):
    MLP = 'mlp'
    SBERT = 'sentence-transformers/msmarco-distilbert-base-v3'
    CLIP = 'openai/clip-vit-base-patch32'


class ModelDimensions(BetterEnum):
    SBERT = 768
    CLIP = 512


SUPPORTED_FILE_TYPES = {
    Modalities.TEXT: ['txt', 'md'],
    Modalities.IMAGE: ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'tif'],
    Modalities.VIDEO: ['gif'],
}
AVAILABLE_MODALITIES_FOR_SEARCH = [Modalities.TEXT, Modalities.IMAGE, Modalities.VIDEO]
NOT_AVAILABLE_MODALITIES_FOR_FILTER = [
    Modalities.IMAGE,
    Modalities.VIDEO,
]

BASE_STORAGE_URL = (
    'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets'
)

CLIP_USES = {
    'local': ('CLIPOnnxEncoder/0.8.1', 'ViT-B-32::openai', ModelDimensions.CLIP),
    'remote': ('CLIPOnnxEncoder/0.8.1-gpu', 'ViT-B-32::openai', ModelDimensions.CLIP),
}

EXTERNAL_CLIP_HOST = 'encoderclip-pretty-javelin-3aceb7f2cd.wolf.jina.ai'

DEFAULT_FLOW_NAME = 'nowapi'
PREFETCH_NR = 10

SURVEY_LINK = 'https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli'

TAG_OCR_DETECTOR_TEXT_IN_DOC = '_ocr_detector_text_in_doc'
TAG_INDEXER_DOC_HAS_TEXT = '_indexer_doc_has_text'
EXECUTOR_PREFIX = 'jinahub+docker://'
ACCESS_PATHS = '@cc'
