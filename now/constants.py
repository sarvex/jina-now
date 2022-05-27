from now.utils import BetterEnum


class Modalities(BetterEnum):
    TEXT = 'text'
    IMAGE = 'image'
    MUSIC = 'music'


class Apps(BetterEnum):
    TEXT_TO_IMAGE = 'text_to_image'
    IMAGE_TO_TEXT = 'image_to_text'
    IMAGE_TO_IMAGE = 'image_to_image'
    MUSIC_TO_MUSIC = 'music_to_music'


class DatasetTypes(BetterEnum):
    DEMO = 'demo'
    PATH = 'path'
    URL = 'url'
    DOCARRAY = 'docarray'


class Qualities(BetterEnum):
    MEDIUM = 'medium'
    GOOD = 'good'
    EXCELLENT = 'excellent'


BASE_STORAGE_URL = (
    'https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets'
)

IMAGE_MODEL_QUALITY_MAP = {
    Qualities.MEDIUM: ('ViT-B32', 'openai/clip-vit-base-patch32'),
    Qualities.GOOD: ('ViT-B16', 'openai/clip-vit-base-patch16'),
    Qualities.EXCELLENT: ('ViT-L14', 'openai/clip-vit-large-patch14'),
}


class DemoDatasets:
    BEST_ARTWORKS = 'best-artworks'
    NFT_MONKEY = 'nft-monkey'
    TLL = 'tll'
    BIRD_SPECIES = 'bird-species'
    STANFORD_CARS = 'stanford-cars'
    DEEP_FASHION = 'deepfashion'
    NIH_CHEST_XRAYS = 'nih-chest-xrays'
    GEOLOCATION_GEOGUESSR = 'geolocation-geoguessr'
    MUSIC_GENRES_MID = 'music-genres-mid'
    MUSIC_GENRES_LARGE = 'music-genres-large'
    MUSIC_GENRES_EXTRA_LARGE = 'music-genres-xl'
    MUSIC_MID = 'music-mid'
    MUSIC_LARGE = 'music-large'
    MUSIC_EXTRA_LARGE = 'music-xl'
    ROCK_LYRICS = 'rock-lyrics'
    POP_LYRICS = 'pop-lyrics'
    RAP_LYRICS = 'rap-lyrics'
    INDIE_LYRICS = 'indie-lyrics'
    METAL_LYRICS = 'metal-lyrics'


AVAILABLE_DATASET = {
    Modalities.IMAGE: [
        (DemoDatasets.BEST_ARTWORKS, '🖼  artworks (≈8K docs)'),
        (DemoDatasets.NFT_MONKEY, '💰 nft - bored apes (10K docs)'),
        (DemoDatasets.TLL, '👬 totally looks like (≈12K docs)'),
        (DemoDatasets.BIRD_SPECIES, '🦆 birds (≈12K docs)'),
        (DemoDatasets.STANFORD_CARS, '🚗 cars (≈16K docs)'),
        (DemoDatasets.GEOLOCATION_GEOGUESSR, '🏞 geolocation (≈50K docs)'),
        (DemoDatasets.DEEP_FASHION, '👕 fashion (≈53K docs)'),
        (DemoDatasets.NIH_CHEST_XRAYS, '☢️ chest x-ray (≈100K docs)'),
    ],
    Modalities.MUSIC: [
        (DemoDatasets.MUSIC_GENRES_MID, '🎸 music mid (≈2K docs)'),
        (DemoDatasets.MUSIC_GENRES_LARGE, '🎸 music large (≈10K docs)'),
    ],
    Modalities.TEXT: [
        (DemoDatasets.ROCK_LYRICS, '🎤 rock lyrics (200K docs)'),
        (DemoDatasets.POP_LYRICS, '🎤 pop lyrics (200K docs)'),
        (DemoDatasets.RAP_LYRICS, '🎤 rap lyrics (200K docs)'),
        (DemoDatasets.INDIE_LYRICS, '🎤 indie lyrics (200K docs)'),
        (DemoDatasets.METAL_LYRICS, '🎤 metal lyrics (200K docs)'),
    ],
}

# APP_INFO = {
#     Apps.TEXT_TO_IMAGE: (Modalities.IMAGE, 'Search app for finding images given text'),
#     Apps.IMAGE_TO_TEXT: (Modalities.TEXT, 'Search app for finding text given images'),
#     Apps.IMAGE_TO_IMAGE: (Modalities.IMAGE, 'Search app for finding images given images'),
#     Apps.MUSIC_TO_MUSIC: (Modalities.MUSIC, 'Search app for finding music given music'),
# }

JC_SECRET = '~/.cache/jina-now/wolf.json'

SURVEY_LINK = 'https://docs.google.com/forms/d/e/1FAIpQLSex5gMN4wuQc63TriwRqREBfdijwOrATPe7RotcPaT1SSPfEw/viewform?usp=pp_url&entry.1126738320=Jina+NOW+pip+package'

PRE_TRAINED_LINEAR_HEADS_MUSIC = {
    DemoDatasets.MUSIC_GENRES_MID: 'FineTunedLinearHeadEncoder:93ea59dbd1ee3fe0bdc44252c6e86a87/'
    'linear_head_encoder_music_2k',
    DemoDatasets.MUSIC_GENRES_LARGE: 'FineTunedLinearHeadEncoder:93ea59dbd1ee3fe0bdc44252c6e86a87/'
    'linear_head_encoder_music_10k',
    DemoDatasets.MUSIC_GENRES_EXTRA_LARGE: 'FineTunedLinearHeadEncoder:93ea59dbd1ee3fe0bdc44252c6e86a87/'
    'linear_head_encoder_music_40k',
}
