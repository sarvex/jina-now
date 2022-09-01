import base64
import os
from os.path import join as osp

from docarray import DocumentArray

from now.constants import BASE_STORAGE_URL, DEMO_DATASET_DOCARRAY_VERSION, Modalities
from now.log import yaspin_extended
from now.utils import download, sigmap


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

    if output_modality not in [Modalities.MUSIC, Modalities.VIDEO]:
        model_name = 'ViT-B32'
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}.{model_name}-{docarray_version}.bin'
    else:
        return f'{BASE_STORAGE_URL}/{data_folder}/{dataset}-{docarray_version}.bin'
