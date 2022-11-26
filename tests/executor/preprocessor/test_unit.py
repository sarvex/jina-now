import os
from unittest.mock import MagicMock, Mock

from docarray import Document, DocumentArray

import now.utils
from now.constants import Apps, DatasetTypes
from now.executor.preprocessor import NOWPreprocessor

curdir = os.path.dirname(os.path.abspath(__file__))


def test_s3():
    # define dataset
    da_index = DocumentArray(
        [
            Document(
                uri='s3://bucket/folder1/file.gif',
                tags={'tag_uri': 'tag_file/path.json'},
            ),
            Document(
                uri='s3://bucket/folder2/file.gif',
                tags={'tag_uri': 'tag_file/path.json'},
            ),
        ]
    )
    da_search = DocumentArray(
        [
            Document(text='hi'),
        ]
    )
    # construct the preprocessor
    preprocessor = NOWPreprocessor(Apps.TEXT_TO_VIDEO)

    # patch method get_bucket()
    bucket_mock = Mock()
    bucket_mock.download_file = MagicMock(return_value=None)
    now.utils.get_bucket = MagicMock(return_value=bucket_mock)
    gif_path = f'{curdir}/../../resources/gif/folder1/file.gif'
    json_path = f'{curdir}/../../resources/gif/folder1/meta.json'
    now.utils.get_local_path = MagicMock(
        side_effect=[
            gif_path,
            json_path,
            gif_path,
            json_path,
        ]
        # return_value=f'{curdir}/../../resources/gif/folder1/file.gif'
    )

    res_index = preprocessor.index(
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
    assert len(res_index[0].chunks[0].chunks[0].blob) > 0
    assert len(res_index[0].chunks[0].chunks[0].uri) > 0
    assert len(res_index[0].chunks[0].chunks) == 3
    tags = res_index[0].chunks[0].chunks[0].tags
    assert tags['a1'] == 'v1'
    assert tags['a2'] == 'v2'
