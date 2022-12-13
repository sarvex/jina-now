import multiprocessing as mp
import sys
from typing import List

from docarray import Document, DocumentArray, dataclass
from docarray.typing import Blob, Image, Text, Video
from tqdm import tqdm

from now.constants import Modalities
from now.data_loading.utils import fetch_da_from_url, get_dataset_url
from now.demo_data import AVAILABLE_DATASET, DemoDataset

current_module = sys.modules[__name__]


def _convert_gif_doc(old: Document):
    if old.mime_type != 'image':
        return

    @dataclass
    class MMDoc:
        description: Text = None
        video: Video = None

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
            audio=old.blob,
            genre_tags=old.tags['genre_tags'],
            title=old.tags['name'],
            artist=old.tags['artist'],
        )
    )
    new.tags['track_id'] = old.tags['track_id']
    new.tags['location'] = old.tags['location']
    new.tags['sr'] = old.tags['sr']
    new.tags['album_cover_image_uri'] = old.tags['album_cover_image_uri']
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
            item
            for item in tqdm(pool.imap(encode_func, old_docs[:10]))
            if item is not None
        ]

    new_docs = DocumentArray(new_docs)

    print(f'  New dataset {dataset.name} size: {len(new_docs)}')

    print(f'  Saving new docs {dataset.name} ...')
    new_docs[:10].push(
        'totally-looks-like' if dataset.name == 'tll' else dataset.name,
        show_progress=True,
    )
    print(f'  Saved {dataset.name} docs to hubble ...')


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
