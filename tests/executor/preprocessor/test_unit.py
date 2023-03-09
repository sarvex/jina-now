import os
import shutil

import pytest
from docarray import Document, DocumentArray

from now.constants import S3_CUSTOM_MM_DATA_PATH
from now.executor.preprocessor.executor import NOWPreprocessor
from now.executor.preprocessor.s3_download import (
    convert_fn,
    get_local_path,
    update_tags,
)

curdir = os.path.dirname(os.path.abspath(__file__))


def download_mock(url, destfile):
    path = f'{curdir}/../../{url.replace("s3://", "")}'
    shutil.copyfile(path, destfile)


def test_text(mm_dataclass):
    da_search = DocumentArray(
        [
            Document(
                mm_dataclass(
                    text_field='This is the first Sentence. This is the second Sentence.'
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


def test_update_tags():
    d = Document()
    d._metadata['_s3_uri_for_tags'] = f'{S3_CUSTOM_MM_DATA_PATH}folder0/manifest.json'
    update_tags(
        d=d,
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='eu-west-1',
    )

    assert len(d.tags) > 0
    for value in d.tags.values():
        assert not isinstance(value, dict)


def test_convert_fn(tmpdir):
    d = Document()
    d._metadata['_s3_uri_for_tags'] = f'{S3_CUSTOM_MM_DATA_PATH}folder0/manifest.json'
    d._metadata['field_name'] = 'tags__colors'
    d.uri = f'{S3_CUSTOM_MM_DATA_PATH}folder0/manifest.json'
    res = convert_fn(
        d,
        tmpdir,
        os.environ['AWS_ACCESS_KEY-ID'],
        os.environ['AWS_SECRET_ACCESS_KEY'],
        'eu-west-1',
    )
    assert res.text == {'name': '4A0044', 'slug': '4a0044'}
    assert res.uri == ''


def test_raise_exception():
    with pytest.raises(ValueError):
        preprocessor = NOWPreprocessor()
        preprocessor.preprocess(DocumentArray([Document()]))


def test_get_local_path(tmpdir):
    path = get_local_path(tmpdir, f'{S3_CUSTOM_MM_DATA_PATH}folder0/manifest.json')
    assert (
        isinstance(path, str)
        and path.startswith(str(tmpdir))
        and path.endswith('.json')
    )

    path = get_local_path(tmpdir, 'test')
    assert isinstance(path, str) and path.startswith(str(tmpdir)) and path.endswith('.')
