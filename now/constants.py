from now.now_dataclasses import DemoDataset
from now.utils import BetterEnum

# TODO: Uncomment the DEMO_DATASET_DOCARRAY_VERSION when the DocArray datasets on GCloud has been changed
# from docarray import __version__ as docarray_version
# DEMO_DATASET_DOCARRAY_VERSION = docarray_version
DEMO_DATASET_DOCARRAY_VERSION = '0.13.17'

DOCKER_BFF_PLAYGROUND_TAG = '0.0.127-secure-playgound-3'
NOW_PREPROCESSOR_VERSION = '0.0.88-refactor-request-size-4'
NOW_ANNLITE_INDEXER_VERSION = '0.0.6-annlite-update-list-endpoint-4'


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


class DemoDatasetNames(BetterEnum):
    BEST_ARTWORKS = 'best-artworks'
    NFT_MONKEY = 'nft-monkey'
    TLL = 'tll'
    BIRD_SPECIES = 'bird-species'
    STANFORD_CARS = 'stanford-cars'
    DEEP_FASHION = 'deepfashion'
    NIH_CHEST_XRAYS = 'nih-chest-xrays'
    GEOLOCATION_GEOGUESSR = 'geolocation-geoguessr'
    MUSIC_GENRES_ROCK = 'music-genres-mid'
    MUSIC_GENRES_MIX = 'music-genres-mix'
    ROCK_LYRICS = 'rock-lyrics'
    POP_LYRICS = 'pop-lyrics'
    RAP_LYRICS = 'rap-lyrics'
    INDIE_LYRICS = 'indie-lyrics'
    METAL_LYRICS = 'metal-lyrics'
    TUMBLR_GIFS = 'tumblr-gifs'
    TUMBLR_GIFS_10K = 'tumblr-gifs-10k'
    ES_ONLINE_SHOP_50 = 'extracted-data-online-shop-50-flat'


AVAILABLE_DATASET = {
    Modalities.IMAGE: [
        DemoDataset(
            name=DemoDatasetNames.BEST_ARTWORKS,
            display_modality=Modalities.IMAGE,
            display_name='🖼 artworks (≈8K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.NFT_MONKEY,
            display_modality=Modalities.IMAGE,
            display_name='💰 nft - bored apes (10K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.TLL,
            display_modality=Modalities.IMAGE,
            display_name='👬 totally looks like (≈12K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.BIRD_SPECIES,
            display_modality=Modalities.IMAGE,
            display_name='🦆 birds (≈12K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.STANFORD_CARS,
            display_modality=Modalities.IMAGE,
            display_name='🚗 cars (≈16K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.GEOLOCATION_GEOGUESSR,
            display_modality=Modalities.IMAGE,
            display_name='🌍 geolocation (≈50K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.DEEP_FASHION,
            display_modality=Modalities.IMAGE,
            display_name='👕 fashion (≈53K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.NIH_CHEST_XRAYS,
            display_modality=Modalities.IMAGE,
            display_name='☢ chest x-rays (≈100K docs)',
        ),
    ],
    Modalities.MUSIC: [
        DemoDataset(
            name=DemoDatasetNames.MUSIC_GENRES_ROCK,
            display_modality=Modalities.MUSIC,
            display_name='🎸 rock music (≈2K songs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.MUSIC_GENRES_MIX,
            display_modality=Modalities.MUSIC,
            display_name='🎸 multiple genres (≈2K songs)',
        ),
    ],
    Modalities.TEXT: [
        DemoDataset(
            name=DemoDatasetNames.ROCK_LYRICS,
            display_modality=Modalities.TEXT,
            display_name='🎤 rock lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.POP_LYRICS,
            display_modality=Modalities.TEXT,
            display_name='🎤 pop lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.RAP_LYRICS,
            display_modality=Modalities.TEXT,
            display_name='🎤 rap lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.INDIE_LYRICS,
            display_modality=Modalities.TEXT,
            display_name='🎤 indie lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.METAL_LYRICS,
            display_modality=Modalities.TEXT,
            display_name='🎤 metal lyrics (200K docs)',
        ),
    ],
    Modalities.VIDEO: [
        DemoDataset(
            name=DemoDatasetNames.TUMBLR_GIFS,
            display_modality=Modalities.VIDEO,
            display_name='🎦 tumblr gifs (100K gifs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.TUMBLR_GIFS_10K,
            display_modality=Modalities.VIDEO,
            display_name='🎦 tumblr gifs subset (10K gifs)',
        ),
    ],
    Modalities.TEXT_AND_IMAGE: [
        DemoDataset(
            name=DemoDatasetNames.ES_ONLINE_SHOP_50,
            display_modality=Modalities.TEXT_AND_IMAGE,
            display_name='online shop data (50 products)',
        )
    ],
}

JC_SECRET = '~/.cache/jina-now/wolf.json'

SURVEY_LINK = 'https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli'
