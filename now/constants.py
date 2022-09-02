from now.utils import BetterEnum

# TODO: Uncomment the DEMO_DATASET_DOCARRAY_VERSION when the DocArray datasets on GCloud has been changed
# from docarray import __version__ as docarray_version
# DEMO_DATASET_DOCARRAY_VERSION = docarray_version
DEMO_DATASET_DOCARRAY_VERSION = '0.13.17'

DOCKER_BFF_PLAYGROUND_TAG = '0.0.116-use-cas-2'
NOW_PREPROCESSOR_VERSION = '0.0.71-optimize-ds'

NOW_AUTH_EXECUTOR_VERSION = '0.0.3'


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


class Qualities(BetterEnum):
    MEDIUM = 'medium'
    GOOD = 'good'
    EXCELLENT = 'excellent'


BASE_STORAGE_URL = (
    'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets'
)

CLIP_USES = {
    'local': ('CLIPTorchEncoder/latest', 'ViT-B-32::openai', 512),
    'remote': ('CLIPTorchEncoder/latest-gpu', 'ViT-L-14-336::openai', 768),
}

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
    Modalities.TEXT_AND_IMAGE: [],
}

JC_SECRET = '~/.cache/jina-now/wolf.json'

SURVEY_LINK = 'https://10sw1tcpld4.typeform.com/to/VTAyYRpR?utm_source=cli'
