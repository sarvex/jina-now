from __future__ import annotations, print_function, unicode_literals

from docarray import DocumentArray
from pydantic import BaseModel

from now.constants import Modalities
from now.data_loading.utils import _fetch_da_from_url, get_dataset_url
from now.utils import BetterEnum


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


class DemoDataset(BaseModel):
    name: str
    display_name: str
    display_modality: str

    def get_data(self, *args, **kwargs) -> DocumentArray:
        url = get_dataset_url(dataset=self.name, output_modality=self.display_modality)
        return _fetch_da_from_url(url)


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
DEFAULT_EXAMPLE_HOSTED = {
    'text_to_image': [
        DemoDatasetNames.BEST_ARTWORKS,
        DemoDatasetNames.DEEP_FASHION,
    ],
    'image_to_text': [DemoDatasetNames.RAP_LYRICS],
    'image_to_image': [DemoDatasetNames.TLL],
    'text_to_text': [DemoDatasetNames.ROCK_LYRICS],
    'music_to_music': [DemoDatasetNames.MUSIC_GENRES_MIX],
}
