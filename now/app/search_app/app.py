import os
from typing import Dict, List, Tuple

from jina.helper import random_port

from now.app.base.app import JinaNOWApp
from now.common.utils import _extract_tags_for_indexer, get_email, get_indexer_config
from now.constants import (
    CLIP_USES,
    EXECUTOR_PREFIX,
    EXTERNAL_CLIP_HOST,
    NOW_AUTOCOMPLETE_VERSION,
    NOW_PREPROCESSOR_VERSION,
    Apps,
    Modalities,
)
from now.demo_data import AVAILABLE_DATASETS, DemoDataset, DemoDatasetNames
from now.executor.name_to_id_map import name_to_id_map


class SearchApp(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.SEARCH_APP

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Search app'

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 8

    @property
    def demo_datasets(self) -> Dict[Modalities, List[DemoDataset]]:
        return AVAILABLE_DATASETS

    @property
    def finetune_datasets(self) -> [Tuple]:
        return DemoDatasetNames.DEEP_FASHION, DemoDatasetNames.BIRD_SPECIES

    @staticmethod
    def autocomplete_stub() -> Dict:
        return {
            'name': 'autocomplete_executor',
            'uses': f'{EXECUTOR_PREFIX}{name_to_id_map.get("NOWAutoCompleteExecutor2")}/{NOW_AUTOCOMPLETE_VERSION}',
            'needs': 'gateway',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
        }

    @staticmethod
    def preprocessor_stub(use_high_performance_flow: bool) -> Dict:
        return {
            'name': 'preprocessor',
            'needs': 'gateway',
            'replicas': 15
            if use_high_performance_flow
            and not any(_var in os.environ for _var in ['NOW_CI_RUN', 'NOW_TESTING'])
            else 1,
            'uses': f'{EXECUTOR_PREFIX}{name_to_id_map.get("NOWPreprocessor")}/{NOW_PREPROCESSOR_VERSION}',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'jcloud': {
                'resources': {
                    'memory': '1G',
                    'cpu': '0.5',
                    'capacity': 'on-demand',
                }
            },
        }

    @staticmethod
    def clip_encoder_stub(user_input) -> Dict:
        is_remote = user_input.deployment_type == 'remote'
        clip_uses = CLIP_USES[user_input.deployment_type]
        return {
            'name': 'clip_encoder',
            'uses': f'{EXECUTOR_PREFIX}{clip_uses[0]}',
            'host': EXTERNAL_CLIP_HOST if is_remote else '0.0.0.0',
            'port': 443 if is_remote else random_port(),
            'tls': is_remote,
            'external': is_remote,
            'uses_with': {'name': clip_uses[1]},
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'needs': 'preprocessor',
        }

    @staticmethod
    def sbert_encoder_stub() -> Dict:
        return {
            'name': 'sbert_encoder',
            'uses': f'{EXECUTOR_PREFIX}SentenceTransformerEncoder',
            'uses_with': {'name': 'msmarco-distilbert-base-v3'},
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'needs': 'preprocessor',
        }

    @staticmethod
    def indexer_stub(user_input, encoders_list: List[str]) -> Dict:
        """Creates indexer stub.

        :param user_input: User input
        :param encoders_list: List of encoders for data
        """
        indexer_config = get_indexer_config()
        tags = _extract_tags_for_indexer(user_input)
        if len(encoders_list) != 1:
            raise ValueError(
                f'Indexer can only be created for one encoder but have encoders: {encoders_list}'
            )
        else:
            dim = (
                CLIP_USES[user_input.deployment_type][2]
                if encoders_list[0] == 'clip'
                else 768
            )
        return {
            'name': 'indexer',
            'needs': encoders_list,
            'uses': f'{EXECUTOR_PREFIX}{indexer_config["indexer_uses"]}',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'uses_with': {
                'dim': dim,
                'columns': tags,
            },
            'jcloud': {
                'resources': {
                    'memory': indexer_config['indexer_resources']['INDEXER_MEM'],
                    'cpu': indexer_config['indexer_resources']['INDEXER_CPU'],
                    'capacity': 'on-demand',
                }
            },
        }

    def get_executor_stubs(
        self, dataset, user_input, flow_yaml_content, **kwargs
    ) -> Dict:
        """
        Returns a dictionary of executors to be added in the flow along with their env vars and its values
        :param dataset: DocumentArray of the dataset
        :param user_input: user input
        :param flow_yaml_content: initial flow yaml content
        :param kwargs: additional arguments
        :return: executors stubs with filled-in env vars
        """
        if not flow_yaml_content['executors']:
            flow_yaml_content['executors'] = []
        encoders_list = []

        flow_yaml_content['executors'].append(self.autocomplete_stub())

        flow_yaml_content['executors'].append(
            self.preprocessor_stub(
                use_high_performance_flow=get_email().split('@')[-1] == 'jina.ai'
                and user_input.deployment_type == 'remote'
            )
        )

        if Modalities.TEXT in user_input.search_mods.values():
            sbert_encoder = self.sbert_encoder_stub()
            encoders_list.append(sbert_encoder['name'])
            flow_yaml_content['executors'].append(sbert_encoder)
        if any(
            _mod in user_input.search_mods.values()
            for _mod in [Modalities.IMAGE, Modalities.VIDEO]
        ):
            clip_encoder = self.clip_encoder_stub(user_input)
            encoders_list.append(clip_encoder['name'])
            flow_yaml_content['executors'].append(clip_encoder)

        indexer_stub = self.indexer_stub(user_input)
        indexer_stub['needs'] = encoders_list
        flow_yaml_content['executors'].append(indexer_stub)

        # append api_keys to all executors except the remote executors
        for executor in flow_yaml_content['executors']:
            if not (
                executor.get('external', False)
                and user_input.deployment_type == 'remote'
            ):
                if not executor.get('uses_with', None):
                    executor['uses_with'] = {}
                executor['uses_with']['api_keys'] = '${{ ENV.API_KEY }}'
                executor['uses_with']['user_emails'] = '${{ ENV.USER_EMAILS }}'
                executor['uses_with']['admin_emails'] = '${{ ENV.ADMIN_EMAILS }}'

        return flow_yaml_content

    @property
    def max_request_size(self) -> int:
        """Max number of documents in one request"""
        return 10
