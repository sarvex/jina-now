from docarray import Document

from now.apps.base.app import JinaNOWApp
from now.constants import DatasetTypes
from now.data_loading.data_loading import load_data
from now.now_dataclasses import UserInput


def test_elasticsearch_data_loading(setup_online_shop_db, es_connection_params):
    _, index_name = setup_online_shop_db
    connection_str, _ = es_connection_params
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.ELASTICSEARCH
    user_input.es_index_name = index_name
    user_input.es_image_fields = ['uris']
    user_input.es_text_fields = ['title', 'text']
    user_input.es_host_name = connection_str

    transformed_docs = load_data(app=JinaNOWApp(), user_input=user_input)

    assert len(transformed_docs) == 50
    assert isinstance(transformed_docs[0], Document)
    assert len(transformed_docs[0].chunks) == 3
    assert sorted(
        [doc.tags['field_name'] for doc in transformed_docs[0].chunks]
    ) == sorted(['title', 'text', 'uris'])
