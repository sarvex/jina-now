"""
This suite tests that the demo datasets are available
behind their URLs.
"""
import pytest
import requests
from starlette import status

from now.constants import DEMO_DATASET_DOCARRAY_VERSION, Modalities
from now.data_loading.data_loading import get_dataset_url
from now.demo_data import AVAILABLE_DATASET


@pytest.mark.parametrize(
    'modality, ds_name',
    [(m, d.name) for m in Modalities() for d in AVAILABLE_DATASET[m]],
)
def test_dataset_is_available(
    ds_name: str,
    modality: Modalities,
):
    url = get_dataset_url(ds_name, modality)

    assert requests.head(url).status_code == status.HTTP_200_OK


@pytest.mark.parametrize(
    'ds_url',
    [
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/nft-monkey.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/deepfashion.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/nih-chest-xrays.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/stanford-cars.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/bird-species.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/best-artworks.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/geolocation-geoguessr.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/text/rock-lyrics.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/text/pop-lyrics.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/text/rap-lyrics.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/text/indie-lyrics.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/text/metal-lyrics.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/deepfashion.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/nih-chest-xrays.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/stanford-cars.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/bird-species.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/best-artworks.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/jpeg/geolocation-geoguessr.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/music/music-genres-mid-song5-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/music/music-genres-mix-song5-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/video/tumblr-gifs-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/video/tumblr-gifs.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/video/tumblr-gifs.txt10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/video/tumblr-gifs-10k-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/video/tumblr-gifs-10k.img10-{}.bin",
        "https://storage.googleapis.com/jina-fashion-data/data/one-line/datasets/video/tumblr-gifs-10k.txt10-{}.bin",
    ],
)
def test_sample_data_is_available(ds_url: str):
    assert (
        requests.head(url=ds_url.format(DEMO_DATASET_DOCARRAY_VERSION)).status_code
        == status.HTTP_200_OK
    )
