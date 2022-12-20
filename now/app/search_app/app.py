import base64
from typing import Dict, List, Tuple, Union

from docarray import Document, DocumentArray
from jina.serve.runtimes.gateway.http.models import JinaRequestModel, JinaResponseModel
from pydantic import BaseModel

from deployment.bff.app.v1.models.text import NowTextSearchRequestModel
from deployment.bff.app.v1.models.video import (
    NowVideoIndexRequestModel,
    NowVideoListResponseModel,
    NowVideoResponseModel,
)
from now.app.base.app import JinaNOWApp
from now.common.utils import _get_clip_apps_with_dict, common_setup, get_indexer_config
from now.constants import CLIP_USES, Apps, Modalities
from now.demo_data import DemoDatasetNames
from now.now_dataclasses import UserInput


class ImageTextRetrieval(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.IMAGE_TEXT_RETRIEVAL

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
    def autocomplete_stub() -> Dict:
        """
        Returns a dictionary of autocomplete executors to be added in the flow
        """
        return {
            'name': 'autocomplete_executor',
            'uses': '${{ ENV.AUTOCOMPLETE_EXECUTOR_NAME }}',
            'needs': 'gateway',
            'env': {'JINA_LOG_LEVEL': 'DEBUG'},
        }

    @staticmethod
    def preprocessor_stub() -> Dict:
        """
        Returns a dictionary of preprocessor executors to be added in the flow
        """
        return {
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

    @staticmethod
    def clip_encoder_stub() -> Dict:
        return {
            'name': 'clip_encoder',
            'replicas': '${{ ENV.CLIP_ENCODER_REPLICAS }}',
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

    @staticmethod
    def sbert_encoder_stub() -> Dict:
        return {
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

    @staticmethod
    def indexer_stub() -> Dict:
        """
        Returns a dictionary of indexers to be added in the flow
        """
        return {
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

    def get_executor_stubs(self, user_input, flow_yaml_content) -> Dict:
        """
        Returns a dictionary of executors to be added in the flow
        """
        if not flow_yaml_content['executors']:
            flow_yaml_content['executors'] = []
        encoders_list = []
        init_execs_list = []

        # 1. append autocomplete executor to the flow if output modality is text
        if Modalities.TEXT in self.input_modality:
            if not any(
                exec_dict['name'] == 'autocomplete_executor'
                for exec_dict in flow_yaml_content['executors']
            ):
                autocomplete = self.autocomplete_stub()
                init_execs_list.append(autocomplete['name'])
                flow_yaml_content['executors'].append(autocomplete)

        # 2. append preprocessors to the flow
        preprocessor = self.preprocessor_stub()
        preprocessor['needs'] = init_execs_list[-1] if init_execs_list else 'gateway'
        init_execs_list.append(preprocessor['name'])
        flow_yaml_content['executors'].append(preprocessor)

        # 3a. append sbert encoder to the flow if output modalities are texts
        if Modalities.TEXT in user_input.output_modality:
            sbert_encoder = self.sbert_encoder_stub()
            encoders_list.append(sbert_encoder['name'])
            sbert_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(sbert_encoder)

        # 3b. append clip encoder to the flow if output modalities are images
        if Modalities.IMAGE in user_input.output_modality:
            clip_encoder = self.clip_encoder_stub()
            encoders_list.append(clip_encoder['name'])
            clip_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(clip_encoder)

        # 3c. append clip encoder if the output modality is video
        if Modalities.VIDEO in user_input.output_modality:
            clip_encoder = self.clip_encoder_stub()
            encoders_list.append(clip_encoder['name'])
            clip_encoder['needs'] = init_execs_list[-1]
            flow_yaml_content['executors'].append(clip_encoder)

        # 4. append indexer to the flow
        if not any(
            exec_dict['name'] == 'indexer'
            for exec_dict in flow_yaml_content['executors']
        ):
            indexer_stub = self.indexer_stub()
            # skip connection to indexer + from all encoders
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

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        # Get the output modality to determine the executors to be added in the flow
        _search_modality = user_input.search_fields_modalities[
            user_input.search_fields[0]
        ]
        if _search_modality == 'video':
            indexer_config = get_indexer_config(
                len(dataset) * self.samples_frames_video
            )
        else:
            indexer_config = get_indexer_config(len(dataset))
        encoder_with = _get_clip_apps_with_dict(user_input)
        env_dict = common_setup(
            app_instance=self,
            user_input=user_input,
            dataset=dataset,
            encoder_uses=CLIP_USES[user_input.deployment_type][0],
            encoder_with=encoder_with,
            encoder_uses_with={
                'pretrained_model_name_or_path': CLIP_USES[user_input.deployment_type][
                    1
                ]
            },
            pre_trained_embedding_size=CLIP_USES[user_input.deployment_type][2],
            indexer_uses=indexer_config['indexer_uses'],
            kubectl_path=kubectl_path,
            indexer_resources=indexer_config['indexer_resources'],
        )
        super().setup(dataset=dataset, user_input=user_input, kubectl_path=kubectl_path)
        return env_dict

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
