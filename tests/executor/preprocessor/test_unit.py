import os

import pytest
from docarray import Document, DocumentArray
from docarray.typing import Image, Text

from now.constants import S3_CUSTOM_MM_DATA_PATH, DatasetTypes
from now.data_loading.data_loading import load_data
from now.executor.preprocessor.executor import NOWPreprocessor, move_uri
from now.executor.preprocessor.s3_download import (
    convert_s3_to_local_uri,
    get_local_path,
    maybe_download_from_s3,
    update_tags,
)
from now.now_dataclasses import UserInput

curdir = os.path.dirname(os.path.abspath(__file__))


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


def test_convert_s3_to_local_uri(tmpdir):
    d = Document()
    d._metadata['_s3_uri_for_tags'] = f'{S3_CUSTOM_MM_DATA_PATH}folder0/manifest.json'
    d._metadata['field_name'] = 'tags__colors'
    d.uri = f'{S3_CUSTOM_MM_DATA_PATH}folder0/manifest.json'
    res = convert_s3_to_local_uri(
        d,
        tmpdir,
        os.environ['AWS_ACCESS_KEY_ID'],
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


def test_move_uri_not_s3():
    doc = Document(
        tags={'uri': 'test_uri'},
        chunks=[Document(chunks=[Document(), Document()])],
    )
    doc = move_uri(doc)

    assert doc.uri == ''
    assert doc.chunks[0].chunks[0].uri == ''
    assert doc.chunks[0].chunks[1].uri == ''


def test_move_uri_starts_with_s3():
    doc_starts_with_s3 = Document(
        tags={'uri': 's3://test_uri'},
        chunks=[Document(), Document()],
    )

    doc_starts_with_s3 = move_uri(doc_starts_with_s3)

    assert doc_starts_with_s3.uri == 's3://test_uri'
    assert doc_starts_with_s3.chunks[0].uri == 's3://test_uri'
    assert doc_starts_with_s3.chunks[1].uri == 's3://test_uri'


def test_maybe_download_from_s3(tmpdir, mm_dataclass, resources_folder_path):
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.S3_BUCKET
    user_input.dataset_path = S3_CUSTOM_MM_DATA_PATH
    user_input.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    user_input.aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    user_input.aws_region_name = 'eu-west-1'
    user_input.index_fields = ['image.png', 'test.txt']
    user_input.index_field_candidates_to_modalities = {
        'image.png': Image,
        'test.txt': Text,
    }
    da = load_data(user_input)
    maybe_download_from_s3(da, tmpdir, user_input, 2)

    assert len(da) == 10
    for doc in da:
        assert doc.chunks[0].uri.startswith(str(tmpdir))
        assert doc.chunks[0].uri.endswith('.png')
        assert doc.chunks[1].uri.startswith(str(tmpdir))
        assert doc.chunks[1].uri.endswith('.txt')
        assert doc.tags != {}
        assert '_s3_uri_for_tags' in doc._metadata


@pytest.mark.parametrize(
    'data',
    [
        'artworks_data',
        'pop_lyrics_data',
        'elastic_data',
        'local_folder_data',
        's3_bucket_data',
    ],
)
def test_all_cases(data, request):

    docs, user_input = request.getfixturevalue(data)

    preprocessor = NOWPreprocessor(user_input_dict=user_input.to_safe_dict())
    result = preprocessor.preprocess(docs)

    assert result
    for doc in result:
        assert len(doc.chunks) > 0 and len(doc.chunks[0].chunks) > 0
