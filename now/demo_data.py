from __future__ import annotations, print_function, unicode_literals

from docarray import DocumentArray
from docarray.typing import Image, Text, Video
from pydantic import BaseModel

from now.utils import BetterEnum


class DemoDatasetNames(BetterEnum):
    BEST_ARTWORKS = 'team-now/best-artworks'
    NFT_MONKEY = 'team-now/nft-monkey'
    TLL = 'team-now/totally-looks-like'
    BIRD_SPECIES = 'team-now/bird-species'
    STANFORD_CARS = 'team-now/stanford-cars'
    DEEP_FASHION = 'team-now/deepfashion'
    NIH_CHEST_XRAYS = 'team-now/nih-chest-xrays'
    GEOLOCATION_GEOGUESSR = 'team-now/geolocation-geoguessr'
    ROCK_LYRICS = 'team-now/rock-lyrics'
    POP_LYRICS = 'team-now/pop-lyrics'
    RAP_LYRICS = 'team-now/rap-lyrics'
    INDIE_LYRICS = 'team-now/indie-lyrics'
    METAL_LYRICS = 'team-now/metal-lyrics'
    TUMBLR_GIFS = 'team-now/tumblr-gifs'
    TUMBLR_GIFS_10K = 'team-now/tumblr-gifs-10k'
    ES_ONLINE_SHOP_50 = 'team-now/extracted-data-online-shop-50-flat'


class DemoDataset(BaseModel):
    name: str
    display_name: str

    def get_data(self, *args, **kwargs) -> DocumentArray:
        return DocumentArray.pull(self.name)


AVAILABLE_DATASETS = {
    Image: [
        DemoDataset(
            name=DemoDatasetNames.BEST_ARTWORKS,
            display_name='🖼 artworks (≈8K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.NFT_MONKEY,
            display_name='💰 nft - bored apes (10K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.TLL,
            display_name='👬 totally looks like (≈12K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.BIRD_SPECIES,
            display_name='🦆 birds (≈12K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.STANFORD_CARS,
            display_name='🚗 cars (≈16K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.GEOLOCATION_GEOGUESSR,
            display_name='🌍 geolocation (≈50K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.DEEP_FASHION,
            display_name='👕 fashion (≈53K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.NIH_CHEST_XRAYS,
            display_name='☢ chest x-rays (≈100K docs)',
        ),
    ],
    Text: [
        DemoDataset(
            name=DemoDatasetNames.ROCK_LYRICS,
            display_name='🎤 rock lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.POP_LYRICS,
            display_name='🎤 pop lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.RAP_LYRICS,
            display_name='🎤 rap lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.INDIE_LYRICS,
            display_name='🎤 indie lyrics (200K docs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.METAL_LYRICS,
            display_name='🎤 metal lyrics (200K docs)',
        ),
    ],
    Video: [
        DemoDataset(
            name=DemoDatasetNames.TUMBLR_GIFS,
            display_name='🎦 tumblr gifs (100K gifs)',
        ),
        DemoDataset(
            name=DemoDatasetNames.TUMBLR_GIFS_10K,
            display_name='🎦 tumblr gifs subset (10K gifs)',
        ),
    ],
}
DEFAULT_EXAMPLE_HOSTED = []
