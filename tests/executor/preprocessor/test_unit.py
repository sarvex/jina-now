import os
import shutil
from unittest.mock import MagicMock, Mock

import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Image, Text, Video

import now.utils
from now.constants import DatasetTypes
from now.executor.preprocessor import NOWPreprocessor

curdir = os.path.dirname(os.path.abspath(__file__))


def download_mock(url, destfile):
    path = f'{curdir}/../../{url.replace("s3://", "")}'
    shutil.copyfile(path, destfile)


@dataclass
class MMImageDoc:
    uri: Image


@dataclass
class MMVideoDoc:
    uri: Video


@dataclass
class MMTextDoc:
    text: Text


@pytest.mark.parametrize(
    'file_path, modality, num_chunks',
    [
        (
            'image/b.jpg',
            'image',
            1,
        ),
        ('gif/folder1/file.gif', 'video', 3),
    ],
)
def test_ocr_with_bucket(file_path, modality, num_chunks):
    uri = f's3://bucket_name/resources/{file_path}'
    if modality == 'image':
        da_index = DocumentArray(
            [
                Document(MMImageDoc(uri=uri))
                for _ in range(
                    1
                )  # changing range here from 2 to 1 to fix threading issues
            ]
        )
    else:
        da_index = DocumentArray(
            [
                Document(MMVideoDoc(uri=uri))
                for _ in range(
                    1
                )  # changing range here from 2 to 1 to fix threading issues
            ]
        )
    preprocessor = NOWPreprocessor(
        user_input_dict={
            'dataset_type': DatasetTypes.S3_BUCKET,
            'index_fields': [],
            'aws_region_name': 'test',
        },
    )

    bucket_mock = Mock()
    bucket_mock.download_file = download_mock
    now.utils.get_bucket = MagicMock(return_value=bucket_mock)

    res_index = preprocessor.preprocess(
        da_index,
        return_results=True,
    )
    assert len(res_index) == 1
    for d in res_index:
        c = d.chunks[0]
        cc = c.chunks[0]
        assert len(cc.blob) > 0
        assert cc.uri == uri
        assert c.uri == uri
        assert len(c.chunks) == num_chunks


def test_text():
    da_search = DocumentArray(
        [
            Document(
                MMTextDoc(
                    text='This is the first Sentence. This is the second Sentence.'
                )
            )
        ]
    )
    preprocessor = NOWPreprocessor()
    res_search = preprocessor.preprocess(da_search)
    assert len(res_search) == 1
    assert len(res_search[0].chunks) == 1
    result_strings = res_search[0].chunks[0].chunks.texts
    expected_strings = ['This is the first Sentence.', 'This is the second Sentence.']
    assert sorted(result_strings) == sorted(expected_strings)
