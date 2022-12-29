import pathlib
import time
from typing import Dict, Optional

from now.constants import (
    NOW_ELASTIC_INDEXER_VERSION,
    NOW_QDRANT_INDEXER_VERSION,
    TAG_INDEXER_DOC_HAS_TEXT,
    Modalities,
)
from now.deployment.deployment import cmd
from now.executor.name_to_id_map import name_to_id_map
from now_dataclasses import UserInput


def get_indexer_config(
    elastic: Optional[bool] = False,
    kubectl_path: str = None,
    deployment_type: str = None,
) -> Dict:
    """Depending on the number of samples, which will be indexed, indexer and its resources are determined.

    :param elastic: hack to use NOWElasticIndexer, should be changed in future.
    :param kubectl_path: path to kubectl binary
    :param deployment_type: deployment type, e.g. 'remote' or 'local'
    :return: dict with indexer and its resource config
    """

    if elastic and deployment_type == 'local':
        config = {
            'indexer_uses': f'{name_to_id_map.get("NOWElasticIndexer")}/{NOW_ELASTIC_INDEXER_VERSION}',
            'hosts': setup_elastic_service(kubectl_path),
        }
    elif elastic and deployment_type == 'remote':
        raise ValueError(
            'NOWElasticIndexer is currently not supported for remote deployment. Please use local deployment.'
        )
    else:
        config = {
            'indexer_uses': f'{name_to_id_map.get("NOWQdrantIndexer16")}/{NOW_QDRANT_INDEXER_VERSION}'
        }
    config['indexer_resources'] = {'INDEXER_CPU': 0.5, 'INDEXER_MEM': '4G'}

    return config


def _extract_tags_for_indexer(user_input: UserInput):
    final_tags = []
    for tag in user_input.filter_fields:
        final_tags.append([tag, user_input.filter_field_candidates_to_modalities[tag]])
    if any(
        user_input.search_field_candidates_to_modalities[search_field]
        in [Modalities.IMAGE, Modalities.VIDEO]
        for search_field in user_input.search_fields
    ):
        final_tags.append([TAG_INDEXER_DOC_HAS_TEXT, str(bool.__name__)])
    return final_tags


def setup_elastic_service(
    kubectl_path: str,
) -> str:
    """Setup ElasticSearch service and return a connection string to connect to the service with.

    :param kubectl_path: path to kubectl binary
    :return: connection string for connecting to the ElasticSearch service.
    """
    MAX_RETRIES = 20
    cur_dir = pathlib.Path(__file__).parent.resolve()
    cmd(
        f'{kubectl_path} create -f https://download.elastic.co/downloads/eck/2.4.0/crds.yaml'
    )
    cmd(
        f'{kubectl_path} apply -f https://download.elastic.co/downloads/eck/2.4.0/operator.yaml'
    )
    cmd(f'{kubectl_path} create ns nowapi')
    cmd(f'{kubectl_path} apply -f {cur_dir}/../deployment/elastic_kind.yml')
    num_retries = 0
    es_password, error_msg = '', b''
    while num_retries < MAX_RETRIES:
        es_password, error_msg = cmd(
            [
                kubectl_path,
                "get",
                "secret",
                "quickstart-es-elastic-user",
                "-o",
                "go-template='{{.data.elastic | base64decode}}'",
            ]
        )
        if es_password:
            es_password = es_password.decode("utf-8")[1:-1]
            break
        else:
            num_retries += 1
            time.sleep(2)
    if not es_password:
        raise Exception(error_msg.decode("utf-8"))
    host = f"https://elastic:{es_password}@quickstart-es-http.default:9200"
    return host
