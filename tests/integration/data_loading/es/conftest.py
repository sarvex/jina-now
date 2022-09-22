import json
import os
from warnings import filterwarnings, catch_warnings

import pytest
from urllib3.exceptions import InsecureRequestWarning, SecurityWarning
from now.data_loading.es import ElasticsearchConnector, ElasticsearchExtractor
from tests.integration.data_loading.es.example_dataset import ExampleDataset

from tests.integration.data_loading.es.utils import delete_es_index


@pytest.fixture
def setup_elastic_db(es_connection_params):
    connection_str, connection_args = es_connection_params
    with catch_warnings():
        filterwarnings('ignore', category=InsecureRequestWarning)
        filterwarnings('ignore', category=SecurityWarning)
        with ElasticsearchConnector(
            connection_str=connection_str, connection_args=connection_args
        ) as es_connector:
            # return connector to interact with the es database
            yield es_connector

            index_list = list(es_connector._es.indices.get(index='*').keys())
            for index in index_list:
                es_connector._es.indices.delete(index=str(index))


@pytest.fixture
def online_shop_resources(resources_folder_path):
    corpus_path = os.path.join(
        resources_folder_path, 'text+image/online_shop_corpus.jsonl.gz'
    )
    mapping_path = os.path.join(
        resources_folder_path, 'text+image/online_shop_mapping.json'
    )
    return corpus_path, mapping_path, 'online_shop_data'


@pytest.fixture()
def setup_online_shop_db(setup_elastic_db, es_connection_params, online_shop_resources):
    """
    This fixture loads data from Online shop data into an Elasticsearch instance.
    """
    es_connector = setup_elastic_db
    connection_str, connection_args = es_connection_params
    corpus_path, mapping_path, index_name = online_shop_resources

    # number of documents to import
    dataset_size = 50

    # load online shop data from some resource file
    dataset = ExampleDataset(corpus_path)
    dataset.import_to_elastic_search(
        connection_str=connection_str,
        connection_args=connection_args,
        index_name=index_name,
        mapping_path=mapping_path,
        size=dataset_size,
    )

    # return connector to interact with the es database
    yield es_connector, index_name

    # delete index
    delete_es_index(connector=es_connector, name=index_name)
