import base64
from typing import Dict, List, Tuple, Union

from docarray import Document, DocumentArray
from jina.helper import random_port
from jina.serve.runtimes.gateway.http.models import JinaRequestModel, JinaResponseModel
from pydantic import BaseModel

from deployment.bff.app.v1.models.text import NowTextSearchRequestModel
from deployment.bff.app.v1.models.video import (
    NowVideoIndexRequestModel,
    NowVideoListResponseModel,
    NowVideoResponseModel,
)
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
from now.demo_data import DemoDatasetNames
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
        return 'Image-text search app'

    @property
    def input_modality(self) -> Union[Modalities, List[Modalities]]:
        return [Modalities.IMAGE, Modalities.TEXT]

    @property
    def output_modality(self) -> Union[Modalities, List[Modalities]]:
        return [Modalities.IMAGE, Modalities.TEXT]

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 8

    @property
    def finetune_datasets(self) -> [Tuple]:
        return DemoDatasetNames.DEEP_FASHION, DemoDatasetNames.BIRD_SPECIES

    @property
    def samples_frames_video(self) -> int:
        """Number of frames to sample from a video"""
        return 3

    @staticmethod
    def autocomplete_stub() -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of autocomplete executors to be added in the flow
        """
        exec_stub = {
            'name': 'autocomplete_executor',
            'uses': '${{ ENV.AUTOCOMPLETE_EXECUTOR_NAME }}',
            'needs': 'gateway',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
        }
        exec_env = {
            'AUTOCOMPLETE_EXECUTOR_NAME': f'{EXECUTOR_PREFIX}{name_to_id_map.get("NOWAutoCompleteExecutor2")}'
            f'/{NOW_AUTOCOMPLETE_VERSION}',
        }
        return exec_stub, exec_env

    @staticmethod
    def preprocessor_stub() -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of preprocessor executors to be added in the flow
        """
        exec_stub = {
            'name': 'preprocessor',
            'replicas': '${{ ENV.PREPROCESSOR_REPLICAS }}',
            'uses': '${{ ENV.PREPROCESSOR_NAME }}',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'uses_with': {
                'app': '${{ ENV.APP }}',
            },
            'jcloud': {
                'resources': {
                    'memory': '1G',
                    'cpu': '${{ ENV.PREPROCESSOR_CPU }}',
                    'capacity': 'on-demand',
                }
            },
        }
        exec_env = {
            'PREPROCESSOR_NAME': f'{EXECUTOR_PREFIX}{name_to_id_map.get("NOWPreprocessor")}/{NOW_PREPROCESSOR_VERSION}',
            'PREPROCESSOR_REPLICAS': 1,
            'PREPROCESSOR_CPU': 1,
            'APP': Apps.SEARCH_APP,
        }
        return exec_stub, exec_env

    @staticmethod
    def clip_encoder_stub(user_input) -> Tuple[Dict, Dict]:
        is_remote = user_input.deployment_type == 'remote'
        # Define the clip encoder stub
        exec_stub = {
            'name': 'clip_encoder',
            'replicas': '${{ ENV.CLIP_ENCODER_REPLICAS }}',
            'uses': f'{EXECUTOR_PREFIX}{CLIP_USES[user_input.deployment_type][0]}',
            'host': EXTERNAL_CLIP_HOST if is_remote else '0.0.0.0',
            'port': 443 if is_remote else random_port(),
            'tls': is_remote,
            'external': is_remote,
            'uses_with': {
                'name': 'ViT-B/32',
            },
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'needs': 'preprocessor',
        }
        # Define the clip encoder env vars
        encoder_env = {
            'CLIP_ENCODER_REPLICAS': 1,
        }
        return exec_stub, encoder_env

    @staticmethod
    def sbert_encoder_stub() -> Tuple[Dict, Dict]:
        exec_stub = {
            'name': 'sbert_encoder',
            'replicas': '${{ ENV.SBERT_ENCODER_REPLICAS }}',
            'uses': '${{ ENV.ENCODER_NAME }}',
            'host': '${{ ENV.ENCODER_HOST }}',
            'port': '${{ ENV.ENCODER_PORT }}',
            'tls': '${{ ENV.IS_REMOTE_DEPLOYMENT }}',
            'external': '${{ ENV.IS_REMOTE_DEPLOYMENT }}',
            'uses_with': {
                'name': '${{ ENV.PRE_TRAINED_MODEL_NAME }}',
            },
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'needs': 'preprocessor',
        }
        exec_env = {
            'SBERT_ENCODER_REPLICAS': 1,
            'ENCODER_NAME': 'jinahub+docker://SentenceTransformerEncoder',
            'ENCODER_HOST': '',
            'ENCODER_PORT': 443,
            'IS_REMOTE_DEPLOYMENT': True,
            'PRE_TRAINED_MODEL_NAME': 'paraphrase-MiniLM-L6-v2',
        }
        return exec_stub, exec_env

    @staticmethod
    def indexer_stub(user_input) -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of indexers to be added in the flow
        """
        # Get indexer configuration
        indexer_config = get_indexer_config()
        # Get the filter fields to be passed to the indexer
        tags = _extract_tags_for_indexer(user_input)
        exec_stub = {
            'name': 'indexer',
            'uses': '${{ ENV.INDEXER_NAME }}',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
            'uses_with': {
                'dim': '${{ENV.N_DIM}}',
                'columns': '${{ENV.COLUMNS}}',
            },
            'jcloud': {
                'resources': {
                    'memory': '${{ENV.INDEXER_MEM}}',
                    'cpu': '1.0',
                    'capacity': 'on-demand',
                }
            },
        }
        exec_env = {
            **indexer_config['indexer_resources'],
            'INDEXER_NAME': f'{EXECUTOR_PREFIX}{indexer_config["indexer_uses"]}',
            'N_DIM': CLIP_USES[user_input.deployment_type][2],
            'COLUMNS': tags,
        }
        return exec_stub, exec_env

    def get_executor_stubs(
        self, dataset, is_finetuned, user_input, flow_yaml_content, **kwargs
    ) -> Tuple[Dict, Dict]:
        """
        Returns a dictionary of executors to be added in the flow along with their env vars and its values
        :param dataset: DocumentArray of the dataset
        :param is_finetuned: Boolean indicating if this app is finetuned
        :param user_input: user input
        :param flow_yaml_content: initial flow yaml content
        :param kwargs: additional arguments
        :return: executors stubs and env vars
        """
        if not flow_yaml_content['executors']:
            flow_yaml_content['executors'] = []
        encoders_list = []
        init_execs_list = []
        exec_env_dict = {}

        # 1. append autocomplete executor to the flow if output modality is text
        if Modalities.TEXT in self.input_modality:
            if not any(
                exec_dict['name'] == 'autocomplete_executor'
                for exec_dict in flow_yaml_content['executors']
            ):
                autocomplete, exec_env = self.autocomplete_stub()
                init_execs_list.append(autocomplete['name'])
                flow_yaml_content['executors'].append(autocomplete)
                exec_env_dict.update(exec_env)

        # 2. append preprocessors to the flow
        preprocessor, exec_env = self.preprocessor_stub()
        preprocessor['needs'] = init_execs_list[-1] if init_execs_list else 'gateway'
        init_execs_list.append(preprocessor['name'])
        flow_yaml_content['executors'].append(preprocessor)
        exec_env_dict.update(exec_env)

        # TODO: add support for finetuned models
        # 3a. append sbert encoder to the flow if output modalities are texts
        if Modalities.TEXT in user_input.search_mods.values():
            sbert_encoder, exec_env = self.sbert_encoder_stub()
            encoders_list.append(sbert_encoder['name'])
            sbert_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(sbert_encoder)
            exec_env_dict.update(exec_env)

        # 3b. append clip encoder to the flow if output modalities are images
        if Modalities.IMAGE in user_input.search_mods.values():
            clip_encoder, exec_env = self.clip_encoder_stub(user_input)
            encoders_list.append(clip_encoder['name'])
            clip_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(clip_encoder)
            exec_env_dict.update(exec_env)

        # 3c. append clip encoder if the output modality is video
        if Modalities.VIDEO in user_input.search_mods.values():
            clip_encoder, exec_env = self.clip_encoder_stub()
            encoders_list.append(clip_encoder['name'])
            clip_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(clip_encoder)
            exec_env_dict.update(exec_env)

        # 4. append indexer to the flow
        if not any(
            exec_dict['name'] == 'indexer'
            for exec_dict in flow_yaml_content['executors']
        ):
            indexer_stub, exec_env = self.indexer_stub()
            # skip connection to indexer + from all encoders
            indexer_stub['needs'] = encoders_list
            flow_yaml_content['executors'].append(indexer_stub)
            exec_env_dict.update(exec_env)

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

        # Override env vars here if needed.
        is_jina_email = get_email().split('@')[-1] == 'jina.ai'
        if len(dataset) > 200_000 and is_jina_email:
            exec_env_dict.update(
                {
                    'PREPROCESSOR_REPLICAS': '20',
                }
            )
        exec_env_dict.update(
            {
                'PRETRAINED_MODEL_NAME_OR_PATH': CLIP_USES[user_input.deployment_type][
                    1
                ],
                'PRE_TRAINED_EMBEDDINGS_SIZE': CLIP_USES[user_input.deployment_type][2],
                'CAST_CONVERT_NAME': f'{EXECUTOR_PREFIX}CastNMoveNowExecutor/v0.0.3',
            }
        )
        return flow_yaml_content, exec_env_dict

    @property
    def bff_mapping_fns(self):
        def search_text_to_video_request_mapping_fn(
            request: NowTextSearchRequestModel,
        ) -> JinaRequestModel:
            jina_request_model = JinaRequestModel()
            jina_request_model.data = [Document(chunks=[Document(text=request.text)])]
            jina_request_model.parameters = {
                'limit': request.limit,
                'api_key': request.api_key,
                'jwt': request.jwt,
            }
            return jina_request_model

        def search_video_response_mapping_fn(
            request: NowTextSearchRequestModel, response: JinaResponseModel
        ) -> List[NowVideoResponseModel]:
            docs = response.data
            limit = request.limit
            return docs[0].matches[:limit].to_dict()

        def index_text_to_video_request_mapping_fn(
            request: NowVideoIndexRequestModel,
        ) -> JinaRequestModel:
            index_docs = DocumentArray()
            for video, uri, tags in zip(request.videos, request.uris, request.tags):
                if bool(video) + bool(uri) != 1:
                    raise ValueError(
                        f'Can only set one value but have video={video}, uri={uri}'
                    )
                if video:
                    base64_bytes = video.encode('utf-8')
                    message = base64.decodebytes(base64_bytes)
                    index_docs.append(Document(blob=message, tags=tags))
                else:
                    index_docs.append(Document(uri=uri, tags=tags))
            return JinaRequestModel(data=index_docs)

        def no_response_mapping_fn(_: JinaResponseModel) -> BaseModel:
            return BaseModel()

        return {
            '/search': (
                NowTextSearchRequestModel,
                NowVideoListResponseModel,
                search_text_to_video_request_mapping_fn,
                search_video_response_mapping_fn,
            ),
            '/index': (
                NowVideoIndexRequestModel,
                BaseModel,
                index_text_to_video_request_mapping_fn,
                no_response_mapping_fn,
            ),
        }

    @property
    def max_request_size(self) -> int:
        """Max number of documents in one request"""
        return 10
