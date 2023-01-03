import os
import shutil
from unittest.mock import MagicMock, Mock

import pytest
from docarray import Document, DocumentArray

import now.utils
from now.constants import TAG_OCR_DETECTOR_TEXT_IN_DOC, DatasetTypes
from now.executor.preprocessor import NOWPreprocessor

curdir = os.path.dirname(os.path.abspath(__file__))


def download_mock(url, destfile):
    path = f'{curdir}/../../{url.replace("s3://", "")}'
    shutil.copyfile(path, destfile)


@pytest.mark.parametrize(
    'file_path, num_chunks, ocr_text',
    [
        (
            'image/b.jpg',
            1,
            'RichavdE',  # with larger resolution it would be the following text: 'Richard Branson TotallyLooksLike.com Zaphod Beeblebrox',
        ),
        ('gif/folder1/file.gif', 3, 'e'),
    ],
)
def test_ocr_with_bucket(file_path, num_chunks, ocr_text):
    uri = f's3://bucket_name/resources/{file_path}'
    da_index = DocumentArray(
        [
            Document(
                uri=uri,
                tags={'tag_uri': 's3://bucket_name/resources/gif/folder1/meta.json'},
            )
            for _ in range(1)  # changing range here from 2 to 1 to fix threading issues
        ]
    )
    preprocessor = NOWPreprocessor()

    bucket_mock = Mock()
    bucket_mock.download_file = download_mock
    now.utils.get_bucket = MagicMock(return_value=bucket_mock)

    res_index = preprocessor.preprocess(
        da_index,
        parameters={
            'user_input': {
                'dataset_type': DatasetTypes.S3_BUCKET,
                'index_fields': [],
                'aws_region_name': 'test',
            }
        },
        return_results=True,
    )
    assert len(res_index) == 1  # change wrt to range above (line 40)
    for d in res_index:
        c = d.chunks[0]
        cc = c.chunks[0]
        assert len(cc.blob) > 0
        assert cc.uri == uri
        assert c.uri == uri
        assert len(c.chunks) == num_chunks
        tags = cc.tags
        assert tags['a1'] == 'v1'
        assert tags['a2'] == 'v2'
        assert cc.tags[TAG_OCR_DETECTOR_TEXT_IN_DOC] == ocr_text


def test_text():
    da_search = DocumentArray(
        [Document(text='This is the first Sentence. This is the second Sentence.')]
    )
    preprocessor = NOWPreprocessor()
    res_search = preprocessor.preprocess(da_search, parameters={})
    assert len(res_search) == 1
    assert len(res_search[0].chunks) == 1
    result_strings = res_search[0].chunks[0].chunks.texts
    expected_strings = ['This is the first Sentence.', 'This is the second Sentence.']
    assert sorted(result_strings) == sorted(expected_strings)
