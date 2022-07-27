import base64
import io
import os
from os.path import join as osp
from typing import Optional

from docarray import DocumentArray

from now.constants import (
    BASE_STORAGE_URL,
    DEMO_DATASET_DOCARRAY_VERSION,
    IMAGE_MODEL_QUALITY_MAP,
    Modalities,
    Qualities,
)
from now.log import yaspin_extended
from now.utils import download, sigmap


def upload_to_gcloud_bucket(project: str, bucket: str, location: str, fname: str):
    """
    Upload local file to Google Cloud bucket.
    """
    # if TYPE_CHECKING:
    from google.cloud import storage

    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)

    with open(fname, 'rb') as f:
        content = io.BytesIO(f.read())

    tensor = bucket.blob(location + '/' + fname)
    tensor.upload_from_file(content, timeout=7200)


def _fetch_da_from_url(
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

    with yaspin_extended(
        sigmap=sigmap, text="Extracting dataset from DocArray", color="green"
    ) as spinner:
        da = DocumentArray.load_binary(data_path)
        spinner.ok("📂")
    return da


def get_dataset_url(
    dataset: str, model_quality: Optional[Qualities], output_modality: Modalities
) -> str:
    data_folder = None
    docarray_version = DEMO_DATASET_DOCARRAY_VERSION
    if output_modality == Modalities.IMAGE:
        data_folder = 'jpeg'
    elif output_modality == Modalities.TEXT:
        data_folder = 'text'
    elif output_modality == Modalities.MUSIC:
        data_folder = 'music'

    if output_modality != Modalities.MUSIC:
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}.{IMAGE_MODEL_QUALITY_MAP[model_quality][0]}-{docarray_version}.bin'
    else:
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}-{docarray_version}.bin'
