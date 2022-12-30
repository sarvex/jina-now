import os
from typing import Dict, List, Tuple, TypeVar

from docarray.typing import Image, Text, Video
from jina import Client
from jina.helper import random_port

from now.app.base.app import JinaNOWApp
from now.app.search_app.indexer_utils import (
    _extract_tags_for_indexer,
    get_indexer_config,
)
from now.constants import (
    CLIP_USES,
    EXECUTOR_PREFIX,
    EXTERNAL_CLIP_HOST,
    NOW_AUTOCOMPLETE_VERSION,
    NOW_PREPROCESSOR_VERSION,
    Apps,
)
from now.demo_data import (
    AVAILABLE_DATASETS,
    DEFAULT_EXAMPLE_HOSTED,
    DemoDataset,
    DemoDatasetNames,
)
from now.executor.name_to_id_map import name_to_id_map
from now.now_dataclasses import UserInput
from now.utils import get_email


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
    def demo_datasets(self) -> Dict[TypeVar, List[DemoDataset]]:
        return AVAILABLE_DATASETS

    @property
    def finetune_datasets(self) -> [Tuple]:
        return DemoDatasetNames.DEEP_FASHION, DemoDatasetNames.BIRD_SPECIES

    def is_demo_available(self, user_input) -> bool:
        if (
            DEFAULT_EXAMPLE_HOSTED
            and user_input.dataset_name in DEFAULT_EXAMPLE_HOSTED
            and user_input.deployment_type == 'remote'
            and 'NOW_EXAMPLES' not in os.environ
            and 'NOW_CI_RUN' not in os.environ
        ):
            client = Client(
                host=f'grpcs://now-example-{self.app_name}-{user_input.dataset_name}.dev.jina.ai'.replace(
                    '_', '-'
                )
            )
            try:
                client.post('/dry_run', timeout=2)
            except Exception:
                return False
            return True
        return False

    @staticmethod
    def autocomplete_stub() -> Dict:
        return {
            'name': 'autocomplete_executor',
            'uses': f'{EXECUTOR_PREFIX}{name_to_id_map.get("NOWAutoCompleteExecutor2")}/{NOW_AUTOCOMPLETE_VERSION}',
            'needs': 'gateway',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
        }

    @staticmethod
    def preprocessor_stub(use_high_perf_flow: bool) -> Dict:
        return {
            'name': 'preprocessor',
            'needs': 'gateway',
            'replicas': 15 if use_high_perf_flow else 1,
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
            'uses': f'{EXECUTOR_PREFIX}TransformerSentenceEncoder',
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

    def get_executor_stubs(self, dataset, user_input: UserInput) -> Dict:
        """Returns a dictionary of executors to be added in the flow

        :param dataset: DocumentArray of the dataset
        :param user_input: user input
        :return: executors stubs with filled-in env vars
        """
        flow_yaml_executors = [
            self.autocomplete_stub(),
            self.preprocessor_stub(
                use_high_perf_flow=get_email().split('@')[-1] == 'jina.ai'
                and user_input.deployment_type == 'remote'
                and not any(
                    _var in os.environ for _var in ['NOW_CI_RUN', 'NOW_TESTING']
                )
            ),
        ]

        encoders_list = []
        if any(
            user_input.index_field_candidates_to_modalities[field] == Text
            for field in user_input.index_fields
        ):
            sbert_encoder = self.sbert_encoder_stub()
            encoders_list.append(sbert_encoder['name'])
            flow_yaml_executors.append(sbert_encoder)
        if any(
            user_input.index_field_candidates_to_modalities[field] in [Image, Video]
            for field in user_input.index_fields
        ):
            clip_encoder = self.clip_encoder_stub(user_input)
            encoders_list.append(clip_encoder['name'])
            flow_yaml_executors.append(clip_encoder)

        flow_yaml_executors.append(
            self.indexer_stub(user_input, encoders_list=encoders_list)
        )

        return flow_yaml_executors

    @property
    def max_request_size(self) -> int:
        """Max number of documents in one request"""
        return 10
