import base64
import os
from typing import Dict, List

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
from now.common.utils import (
    _get_clip_apps_with_dict,
    common_setup,
    get_email,
    get_indexer_config,
)
from now.constants import CLIP_USES, Apps, Modalities
from now.now_dataclasses import UserInput

NUM_FRAMES_SAMPLED = 3


class TextToVideo(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.TEXT_TO_VIDEO

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Text to video search app'

    @property
    def input_modality(self) -> List[Modalities]:
        return [Modalities.TEXT]

    @property
    def output_modality(self) -> List[Modalities]:
        return [Modalities.VIDEO]

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 12

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)
        dataset_len = kwargs.get('dataset_len', 0) * NUM_FRAMES_SAMPLED
        is_jina_email = get_email().split('@')[-1] == 'jina.ai'

        flow_dir = os.path.abspath(os.path.join(__file__, '..'))

        if finetuning:
            if dataset_len > 200_000 and is_jina_email:
                print(f"🚀🚀🚀 You are using high performance flow")
                self.flow_yaml = os.path.join(
                    flow_dir, 'ft-flow-video-clip-high-performance.yml'
                )
            else:
                self.flow_yaml = os.path.join(flow_dir, 'ft-flow-video-clip.yml')
        else:
            if dataset_len > 200_000 and is_jina_email:
                print(f"🚀🚀🚀 You are using high performance flow")
                self.flow_yaml = os.path.join(
                    flow_dir, 'flow-video-clip-high-performance.yml'
                )
            else:
                self.flow_yaml = os.path.join(flow_dir, 'flow-video-clip.yml')

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        indexer_config = get_indexer_config(len(dataset) * NUM_FRAMES_SAMPLED)
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
        super().setup(dataset, user_input, kubectl_path)
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
