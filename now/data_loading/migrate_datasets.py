import base64
import multiprocessing as mp
import os
import pathlib
import pickle
import sys
from os.path import join as osp
from typing import List, TypeVar

from docarray import Document, DocumentArray, dataclass, field
from docarray.typing import Blob, Image, Text
from tqdm import tqdm

from now.constants import BASE_STORAGE_URL, DEMO_DATASET_DOCARRAY_VERSION, Modalities
from now.demo_data import AVAILABLE_DATASET, DemoDataset
from now.utils import download

current_module = sys.modules[__name__]

MyVideo = TypeVar('MyVideo', bound=str)


def my_setter(value) -> 'Document':
    return Document(uri=value)


def my_getter(doc: 'Document'):
    return doc.uri


def _convert_gif_doc(old: Document):
    if old.mime_type != 'image':
        return

    @dataclass
    class MMDoc:
        description: Text = None
        video: MyVideo = field(setter=my_setter, getter=my_getter, default=None)

    new = Document(MMDoc(video=old.uri, description=old.tags['description']))
    # There is no additional meaningful tags
    return new


def _convert_music_doc(old: Document):
    @dataclass
    class MMDoc:
        artist: Text = (None,)
        audio: Blob = (None,)
        genre_tags: List[Text] = (None,)
        title: Text = (None,)

    new = Document(
        MMDoc(
            audio=old.uri,
            genre_tags=old.tags['genre_tags'],
            title=old.tags['name'],
            artist=old.tags['artist'],
        )
    )
    new.tags['track_id'] = old.tags['track_id']
    new.tags['location'] = old.tags.get('location', '')
    new.tags['sr'] = old.tags.get('sr', '')
    new.tags['album_cover_image_url'] = old.tags.get('album_cover_image_url', '')
    new.chunks[1].blob = b''  # set the blob to None
    return new


def _convert_text_doc(old: Document):
    if old.tags['content_type'] != 'text':
        return

    @dataclass
    class MMDoc:
        lyrics: Text = (None,)
        title: Text = (None,)

    new = Document(MMDoc(lyrics=old.text, title=old.tags['additional_info']))
    # There is no additional meaningful tags
    return new


def _convert_image_doc_only(old: Document):
    if old.mime_type != 'image':
        return

    @dataclass
    class MMDoc:
        image: Image = (None,)

    new = Document(MMDoc())
    for k, v in old.tags.items():
        new.tags[k] = v
    new.embedding = old.embedding
    return new


def _convert_image_doc_with_label(old: Document):
    if old.tags['content_type'] != 'image':
        return

    @dataclass
    class MMDoc:
        label: Text = (None,)
        image: Blob = (None,)

    new = Document(
        MMDoc(image=old.blob or old.uri, label=old.tags.get('finetuner_label', ''))
    )
    # for k, v in old.tags.items():
    #     new.tags[k] = v
    return new


def _convert_doc_geolocation_geoguessr(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_bird_species(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_tll(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_nft_monkey(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_nih_chest_xrays(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_stanford_cars(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_deepfashion(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_best_artworks(old: Document):
    return _convert_image_doc_with_label(old)


def _convert_doc_music_genres_mid(old: Document):
    return _convert_music_doc(old)


def _convert_doc_music_genres_mix(old: Document):
    return _convert_music_doc(old)


def _convert_doc_rock_lyrics(old: Document):
    return _convert_text_doc(old)


def _convert_doc_pop_lyrics(old: Document):
    return _convert_text_doc(old)


def _convert_doc_rap_lyrics(old: Document):
    return _convert_text_doc(old)


def _convert_doc_indie_lyrics(old: Document):
    return _convert_text_doc(old)


def _convert_doc_metal_lyrics(old: Document):
    return _convert_text_doc(old)


def _convert_doc_tumblr_gifs(old: Document):
    return _convert_gif_doc(old)


def _convert_doc_tumblr_gifs_10k(old: Document):
    return _convert_gif_doc(old)


def convert_dataset(dataset: DemoDataset, modality: Modalities, num_workers: int = 8):
    path = f'{dataset}.bin'

    print(f'===> {dataset}')
    print(f'  Loading {dataset.name} dataset from Google Storage ...')
    url = get_dataset_url(dataset.name, modality)
    old_docs = fetch_da_from_url(url)
    print(f'  Old dataset {dataset.name} size: {len(old_docs)}')

    encode_func = getattr(
        current_module, f'_convert_doc_{dataset.name.replace("-", "_")}'
    )

    print(f'  Converting {dataset.name} ...')
    with mp.Pool(processes=num_workers) as pool:
        new_docs = [
            item for item in tqdm(pool.imap(encode_func, old_docs)) if item is not None
        ]

    new_docs = DocumentArray(new_docs)

    print(f'  New dataset {dataset.name} size: {len(new_docs)}')

    print(f'  Saving new docs {dataset.name} ...')
    new_docs.push(
        'totally-looks-like' if dataset.name == 'tll' else dataset.name,
        show_progress=True,
    )
    print(f'  Saved {dataset.name} docs to hubble ...')


def fetch_da_from_url(
    url: str, downloaded_path: str = '~/.cache/jina-now'
) -> DocumentArray:
    data_dir = os.path.expanduser(downloaded_path)
    if not os.path.exists(osp(data_dir, 'data/tmp')):
        os.makedirs(osp(data_dir, 'data/tmp'))
    data_path = (
        data_dir
        + f"/data/tmp/{base64.b64encode(bytes(url, 'utf-8')).decode('utf-8')}.bin"
    )
    if not os.path.exists(data_path):
        download(url, data_path)

    try:
        da = DocumentArray.load_binary(data_path)
    except pickle.UnpicklingError:
        path = pathlib.Path(data_path).expanduser().resolve()
        os.remove(path)
        download(url, data_path)
        da = DocumentArray.load_binary(data_path)
    return da


def get_dataset_url(dataset: str, output_modality: Modalities) -> str:
    data_folder = None
    docarray_version = DEMO_DATASET_DOCARRAY_VERSION
    if output_modality == Modalities.IMAGE:
        data_folder = 'jpeg'
    elif output_modality == Modalities.TEXT:
        data_folder = 'text'
    elif output_modality == Modalities.MUSIC:
        data_folder = 'music'
    elif output_modality == Modalities.VIDEO:
        data_folder = 'video'
    elif output_modality == Modalities.TEXT_AND_IMAGE:
        data_folder = 'text-image'
    if output_modality not in [
        Modalities.MUSIC,
        Modalities.VIDEO,
        Modalities.TEXT_AND_IMAGE,
    ]:
        model_name = 'ViT-B32'
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}.{model_name}-{docarray_version}.bin'
    else:
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}-{docarray_version}.bin'


def main():
    """
    Main method.
    """
    num_workers = 8

    for modality, datasets in AVAILABLE_DATASET.items():
        for dataset in datasets:
            convert_dataset(dataset, modality, num_workers)


if __name__ == '__main__':
    main()
