from __future__ import annotations, print_function, unicode_literals

from docarray import DocumentArray
from docarray.typing import Image, Text, Video
from pydantic import BaseModel

from now.utils.common.helpers import BetterEnum


class DemoDatasetNames(BetterEnum):
    BEST_ARTWORKS = 'team-now/best-artworks'
    TLL = 'team-now/totally-looks-like'
    BIRD_SPECIES = 'team-now/bird-species'
    STANFORD_CARS = 'team-now/stanford-cars'
    DEEP_FASHION = 'team-now/deepfashion'
    POP_LYRICS = 'team-now/pop-lyrics'
    TUMBLR_GIFS_10K = 'team-now/tumblr-gifs-10k'
    ES_ONLINE_SHOP_50 = 'team-now/extracted-data-online-shop-50-flat'


class DemoDataset(BaseModel):
    name: str
    display_name: str
    index_fields: str  # To be removed once the app works with all index fields
    info: str
    source: str

    def get_data(self, *args, **kwargs) -> DocumentArray:
        return DocumentArray.pull(self.name)


AVAILABLE_DATASETS = {
    Image: [
        DemoDataset(
            name=DemoDatasetNames.BEST_ARTWORKS,
            display_name='🖼 artworks (≈8K docs)',
            index_fields='image',
            info='A collection of artworks of the 50 most influential artists of all time.',
            source='https://www.kaggle.com/datasets/ikarus777/best-artworks-of-all-time',
        ),
        DemoDataset(
            name=DemoDatasetNames.TLL,
            display_name='👬 totally looks like (≈12K docs)',
            index_fields='image',
            info='A collection of 6_016 image-pairs from '
            'the wild to cover the diversity at which humans operate',
            source='https://sites.google.com/view/totally-looks-like-dataset',
        ),
        DemoDataset(
            name=DemoDatasetNames.BIRD_SPECIES,
            display_name='🦆 birds (≈12K docs)',
            index_fields='image',
            info='A collection of images of birds containing 12_000 images of 200 species of birds.',
            source='https://www.vision.caltech.edu/datasets/cub_200_2011/',
        ),
        DemoDataset(
            name=DemoDatasetNames.STANFORD_CARS,
            display_name='🚗 cars (≈16K docs)',
            index_fields='image',
            info='A collection of images of cars containing 16_185 images of 196 classes of cars.',
            source='https://ai.stanford.edu/~jkrause/cars/car_dataset.html',
        ),
        DemoDataset(
            name=DemoDatasetNames.DEEP_FASHION,
            display_name='👕 fashion (≈53K docs)',
            index_fields='image',
            info='A collection of images of fashion items containing 53_000 images of 50 classes of fashion '
            'items ranging from well-posed shop images to unconstrained consumer photos.',
            source='https://mmlab.ie.cuhk.edu.hk/projects/DeepFashion.html',
        ),
    ],
    Text: [
        DemoDataset(
            name=DemoDatasetNames.POP_LYRICS,
            display_name='🎤 pop lyrics (200K docs)',
            index_fields='lyrics',
            info='A collection of pop song lyrics containing 200_000 song lyrics',
            source='NA',
        ),
    ],
    Video: [
        DemoDataset(
            name=DemoDatasetNames.TUMBLR_GIFS_10K,
            display_name='🎦 tumblr gifs subset (10K gifs)',
            index_fields='video',
            info='A collection of gifs from tumblr containing a (10_000) animated GIFs and sentences '
            'describing visual content of the animated GIFs',
            source='https://raingo.github.io/TGIF-Release/',
        ),
    ],
}
