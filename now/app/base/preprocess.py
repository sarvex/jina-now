import io

import numpy as np
from docarray import Document
from PIL import Image

from now.constants import Modalities
from now.data_loading.convert_datasets_to_jpeg import (
    ndarray_to_jpeg_bytes,
    to_thumbnail_jpg,
)

NUM_FRAMES_SAMPLED = 3


def preprocess_text(
    d: Document,
) -> Document:
    """Splits the text by sentences and puts each sentence into the chunk chunk level.

    Generates sentence chunks:
    Before
    Document(chunks=[Document(text='s1. s2. s3')])

    After
    Document(chunks=[Document(text=None, chunks=[Document('s1'), Document('s2')..])])
    """
    import nltk

    nltk.download('punkt', quiet=True)
    from nltk.tokenize import sent_tokenize

    # TODO HACK (needs to be provided as general feature
    d.text = 'loading' if d.text.lower() == 'loader' else d.text

    if not d.text and d.uri:
        d.load_uri_to_text(timeout=10)
        d.tags['additional_info'] = d.uri
    d.chunks = [
        Document(
            mime_type=Modalities.TEXT,
            modality=Modalities.TEXT,
            text=sentence,
            tags=d.tags,
        )
        for sentence in set(sent_tokenize(d.text.replace('\n', ' ')))
        if sentence
    ]
    d.text = None
    return d


def preprocess_image(d: Document):
    """loads document into memory and creates thumbnail."""
    # TODO move logic of downloading data away from preprocessing them
    if d.tensor is None:
        if d.blob != b'':
            d.convert_blob_to_image_tensor()
        elif d.uri:
            d.load_uri_to_image_tensor(timeout=10)
    if 'uri' in d.tags:
        d.uri = d.tags['uri']
    to_thumbnail_jpg(d)

    d.chunks.append(
        Document(
            uri=d.uri,
            blob=d.blob,
            tags=d.tags,
            modality=Modalities.IMAGE,
            mime_type='image/jpeg',
        )
    )
    d.blob = None
    d.uri = None


def preprocess_video(d: Document):
    if d.blob == b'':
        if d.uri:
            d.load_uri_to_blob(timeout=10)
        elif d.tensor is not None:
            d.convert_tensor_to_blob()
    _sample_video(d)


def preprocess_music(d: Document):
    from pydub import AudioSegment

    if d.blob == b'':
        if d.uri:
            if d.uri.startswith(f'data:{d.mime_type}'):
                d.load_uri_to_blob(timeout=10)
            else:
                AudioSegment.from_file(d.uri)  # checks if file is valid
                with open(d.uri, 'rb') as fh:
                    d.blob = fh.read()


def _select_frames(num_selected_frames, num_total_frames):
    partition_size = num_total_frames / (num_selected_frames + 1)
    return [round(partition_size * (i + 1)) for i in range(num_selected_frames)]


def _sample_video(d):
    video = d.blob
    video_io = io.BytesIO(video)
    gif = Image.open(video_io)
    frame_indices = _select_frames(NUM_FRAMES_SAMPLED, gif.n_frames)
    for i in frame_indices:
        gif.seek(i)
        frame = np.array(gif.convert("RGB"))
        image_bytes = ndarray_to_jpeg_bytes(frame)
        d.chunks.append(
            Document(
                uri=d.uri,
                blob=image_bytes,
                tags=d.tags,
                modality=Modalities.IMAGE,
                mime_type='image/jpeg',
            )
        )
    d.blob = None
    d.uri = None
    d.tensor = None
