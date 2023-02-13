import io
import math

import numpy as np
from docarray import Document
from PIL import Image

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
        # In case it is a json file, we need to get the right field
    d.chunks = [
        Document(
            mime_type='text',
            modality='text',
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
    strategy = 2
    # TODO move logic of downloading data away from preprocessing them
    if strategy == 1:
        if d.tensor is None:
            if d.blob != b'':
                d.convert_blob_to_image_tensor()
            elif d.uri:
                d.load_uri_to_image_tensor(timeout=10)
        to_thumbnail_jpg(d)

    elif strategy == 2:
        if 'uri' in d.tags:
            d.uri = d.tags['uri']
        if d.blob is None:
            if d.uri:
                d.load_uri_to_blob()
                downsample_image(d)
            d.convert_tensor_to_blob()

    d.chunks.append(
        Document(
            uri=d.uri,
            blob=d.blob,
            tags=d.tags,
            modality='image',
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
                modality='image',
                mime_type='image/jpeg',
            )
        )
    d.blob = None
    d.uri = None
    d.tensor = None


def ndarray_to_jpeg_bytes(arr) -> bytes:
    pil_img = Image.fromarray(arr)
    pil_img.thumbnail((224, 224))
    pil_img = pil_img.convert('RGB')
    img_byte_arr = io.BytesIO()
    pil_img.save(img_byte_arr, format="JPEG", quality=95)
    return img_byte_arr.getvalue()


def to_thumbnail_jpg(doc: Document):
    if doc.tensor is not None:
        doc.blob = ndarray_to_jpeg_bytes(doc.tensor)
    return doc


def preserve_aspect_ratio(source_size, target_size):
    def round_aspect(number, key):
        return max(min(math.floor(number), math.ceil(number), key=key), 1)

    width, height = source_size
    x, y = target_size
    if x >= width and y >= height:
        return

    aspect = width / height
    if x / y >= aspect:
        x = round_aspect(y * aspect, key=lambda n: abs(aspect - n / y))
    else:
        y = round_aspect(x / aspect, key=lambda n: 0 if n == 0 else abs(aspect - x / n))
    return x, y


def downsample_image(doc: Document):
    if doc.tensor is not None:
        width, height, _ = doc.tensor.shape
        doc.tensor.set_image_tensor_shape(
            shape=preserve_aspect_ratio((width, height), (224, 224))
        )
    return doc
