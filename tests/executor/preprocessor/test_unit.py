import os
import shutil
from unittest.mock import MagicMock, Mock

from docarray import Document, DocumentArray

import now.utils
from now.constants import TAG_OCR_DETECTOR_TEXT_IN_DOC, Apps, DatasetTypes
from now.executor.preprocessor import NOWPreprocessor

curdir = os.path.dirname(os.path.abspath(__file__))


def download_mock(url, destfile):
    print('url:', url)
    print('destfile:', destfile)
    path = f'{curdir}/../../{url.replace("s3://", "")}'
    shutil.copyfile(path, destfile)


def test_s3_video():
    da_index = DocumentArray(
        [
            Document(
                uri='s3://bucket_name/resources/gif/folder1/file.gif',
                tags={'tag_uri': 's3://bucket_name/resources/gif/folder1/meta.json'},
            )
            for _ in range(2)
        ]
    )
    preprocessor = NOWPreprocessor(Apps.TEXT_TO_VIDEO)

    bucket_mock = Mock()
    bucket_mock.download_file = download_mock
    now.utils.get_bucket = MagicMock(return_value=bucket_mock)

    res_index = preprocessor.preprocess(
        da_index,
        parameters={
            'user_input': {
                'dataset_type': DatasetTypes.S3_BUCKET,
                'search_fields': [],
                'aws_region_name': 'test',
            }
        },
        return_results=True,
    )
    assert len(res_index) == 2
    for d in res_index:
        c = d.chunks[0]
        cc = c.chunks[0]
        assert len(cc.blob) > 0
        uri = 's3://bucket_name/resources/gif/folder1/file.gif'
        assert cc.uri == uri
        assert c.uri == uri
        assert len(c.chunks) == 3
        tags = cc.tags
        assert tags['a1'] == 'v1'
        assert tags['a2'] == 'v2'
        assert cc.tags[TAG_OCR_DETECTOR_TEXT_IN_DOC] == 'e '


def test_text():
    da_search = DocumentArray([Document(text='test')])
    preprocessor = NOWPreprocessor(Apps.TEXT_TO_VIDEO)
    res_search = preprocessor.preprocess(da_search, parameters={})
    assert len(res_search) == 1
    assert len(res_search[0].chunks) == 1
    assert res_search[0].chunks[0].chunks[0].text == 'test'
