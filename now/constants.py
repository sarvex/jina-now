from __future__ import annotations, print_function, unicode_literals

from now.utils import BetterEnum

# TODO: Uncomment the DEMO_DATASET_DOCARRAY_VERSION when the DocArray datasets on GCloud has been changed
# from docarray import __version__ as docarray_version
# DEMO_DATASET_DOCARRAY_VERSION = docarray_version
DEMO_DATASET_DOCARRAY_VERSION = '0.13.17'

DOCKER_BFF_PLAYGROUND_TAG = '0.0.128-add-get-status-1'
NOW_PREPROCESSOR_VERSION = '0.0.92-refactor-post-process-5'
NOW_ANNLITE_INDEXER_VERSION = '0.0.12-feat-add-qdrant-indexer-35'
NOW_QDRANT_INDEXER_VERSION = '0.0.1-refactor-post-process-5'
NOW_ELASTIC_INDEXER_VERSION = '0.0.2-fix-elastic-1'


class Modalities(BetterEnum):
    TEXT = 'text'
    IMAGE = 'image'
    MUSIC = 'music'
    VIDEO = 'video'
    TEXT_AND_IMAGE = 'text_and_image'


class Apps(BetterEnum):
    TEXT_TO_TEXT = 'text_to_text'
    TEXT_TO_IMAGE = 'text_to_image'
    IMAGE_TO_TEXT = 'image_to_text'
    IMAGE_TO_IMAGE = 'image_to_image'
    MUSIC_TO_MUSIC = 'music_to_music'
    TEXT_TO_VIDEO = 'text_to_video'
    TEXT_TO_TEXT_AND_IMAGE = 'text_to_text_and_image'


class DatasetTypes(BetterEnum):
    DEMO = 'demo'
    PATH = 'path'
    URL = 'url'
    DOCARRAY = 'docarray'
    S3_BUCKET = 's3_bucket'
    ELASTICSEARCH = 'elasticsearch'


class Qualities(BetterEnum):
    MEDIUM = 'medium'
    GOOD = 'good'
    EXCELLENT = 'excellent'


class ModelNames(BetterEnum):
    MLP = 'mlp'
    SBERT = 'sentence-transformers/msmarco-distilbert-base-v3'
    CLIP = 'openai/clip-vit-base-patch32'


BASE_STORAGE_URL = (
    'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets'
)

CLIP_USES = {
    'local': ('CLIPOnnxEncoder/latest', 'ViT-B-32::openai', 512),
    'remote': ('CLIPOnnxEncoder/latest-gpu', 'ViT-B-32::openai', 512),
}

EXTERNAL_CLIP_HOST = 'encoderclip-bh-5f4efaff13.wolf.jina.ai'

PREFETCH_NR = 10

SURVEY_LINK = 'https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli'
