import base64
import os
from os.path import join as osp
from typing import Any, Dict, List, Optional

from docarray import Document, DocumentArray

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


def transform_es_data(data: DocumentArray) -> List[Dict[str, Any]]:
    """
    Transform data extracted from Elasticsearch to a more convenient form.

    :param data: Loaded `DocumentArray` containing ES data.
    :return: List of data examples as dictionaries.
    """
    transformed_data = []
    for document in data:
        attributes = {}
        transform_doc(document, attributes, [])
        transformed_data.append(attributes)
    return transformed_data


def transform_doc(document: Document, attributes: Dict[str, Any], names: List[str]):
    """
    Extract attributes from a `Document` and store it as a dictionary.

    Recursively iterates across different chunks of the `Document` and collects
    attributes with their corresponding values.

    :param document: `Document` we want to transform.
    :param attributes: Dictionary of attributes extracted from the document.
    :param names: Name of an attribute (attribute names may be nested, e.g.
        info.cars, and we need to store name(s) on every level of recursion).
    """
    if not document.chunks:
        names.append(document.tags['field_name'])
        attr_name = '.'.join(names)
        attr_val = (
            document.text if document.tags['modality'] == 'text' else document.uri
        )
        if attr_name not in attributes:
            attributes[attr_name] = []
        attributes[attr_name].append(attr_val)
    else:
        if 'field_name' in document.tags:
            names.append(document.tags['field_name'])
        for doc in document.chunks:
            transform_doc(doc, attributes, names[:])
