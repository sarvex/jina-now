from now.utils import BetterEnum

# TODO: Uncomment the DEMO_DATASET_DOCARRAY_VERSION when the DocArray datasets on GCloud has been changed
# from docarray import __version__ as docarray_version
# DEMO_DATASET_DOCARRAY_VERSION = docarray_version
DEMO_DATASET_DOCARRAY_VERSION = '0.13.17'

DOCKER_BFF_PLAYGROUND_TAG = '0.0.128-add-cname-1'
NOW_PREPROCESSOR_VERSION = '0.0.89-refactor-base-indexer-4'
NOW_ANNLITE_INDEXER_VERSION = '0.0.8-refactor-faster-docker-build-2'


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


class DemoDatasets:
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
        (DemoDatasets.BEST_ARTWORKS, '🖼 artworks (≈8K docs)'),
        (DemoDatasets.NFT_MONKEY, '💰 nft - bored apes (10K docs)'),
        (DemoDatasets.TLL, '👬 totally looks like (≈12K docs)'),
        (DemoDatasets.BIRD_SPECIES, '🦆 birds (≈12K docs)'),
        (DemoDatasets.STANFORD_CARS, '🚗 cars (≈16K docs)'),
        (DemoDatasets.GEOLOCATION_GEOGUESSR, '🏞 geolocation (≈50K docs)'),
        (DemoDatasets.DEEP_FASHION, '👕 fashion (≈53K docs)'),
        (DemoDatasets.NIH_CHEST_XRAYS, '☢️ chest x-ray (≈100K docs)'),
    ],
    Modalities.MUSIC: [
        (DemoDatasets.MUSIC_GENRES_ROCK, '🎸 rock music (≈2K songs)'),
        (DemoDatasets.MUSIC_GENRES_MIX, '🎸 multiple genres (≈2K songs)'),
    ],
    Modalities.TEXT: [
        (DemoDatasets.ROCK_LYRICS, '🎤 rock lyrics (200K docs)'),
        (DemoDatasets.POP_LYRICS, '🎤 pop lyrics (200K docs)'),
        (DemoDatasets.RAP_LYRICS, '🎤 rap lyrics (200K docs)'),
        (DemoDatasets.INDIE_LYRICS, '🎤 indie lyrics (200K docs)'),
        (DemoDatasets.METAL_LYRICS, '🎤 metal lyrics (200K docs)'),
    ],
    Modalities.VIDEO: [
        (DemoDatasets.TUMBLR_GIFS, '🎦 tumblr gifs (100K gifs)'),
        (DemoDatasets.TUMBLR_GIFS_10K, '🎦 tumblr gifs subset (10K gifs)'),
    ],
    Modalities.TEXT_AND_IMAGE: [
        (DemoDatasets.ES_ONLINE_SHOP_50, 'online shop data (50 products)')
    ],
}

SURVEY_LINK = 'https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli'

DEFAULT_EXAMPLE_HOSTED = {
    'text_to_image': [
        DemoDatasets.BEST_ARTWORKS,
        DemoDatasets.DEEP_FASHION,
    ],
    'image_to_text': [DemoDatasets.RAP_LYRICS],
    'image_to_image': [DemoDatasets.TLL],
    'text_to_text': [DemoDatasets.ROCK_LYRICS],
    'music_to_music': [DemoDatasets.MUSIC_GENRES_ROCK],
}
