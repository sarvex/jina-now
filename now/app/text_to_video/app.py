import io
import os
from typing import Dict, List

import numpy as np
import PIL
from docarray import Document, DocumentArray
from jina.helper import random_port

from now.app.base.app import JinaNOWApp
from now.common.utils import common_setup, get_email, get_indexer_config
from now.constants import CLIP_USES, EXTERNAL_CLIP_HOST, Apps, Modalities
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
    def input_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def output_modality(self) -> Modalities:
        return Modalities.VIDEO

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

    @property
    def supported_file_types(self) -> List[str]:
        return ['gif', 'mp4', 'mov']

    @property
    def index_query_access_paths(self) -> str:
        return '@c'

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        indexer_config = get_indexer_config(len(dataset) * NUM_FRAMES_SAMPLED)
        is_remote = user_input.deployment_type == 'remote'
        encoder_with = {
            'ENCODER_HOST': EXTERNAL_CLIP_HOST if is_remote else '0.0.0.0',
            'ENCODER_PORT': 443 if is_remote else random_port(),
            'ENCODER_USES_TLS': True if is_remote else False,
            'ENCODER_IS_EXTERNAL': True if is_remote else False,
        }
        return common_setup(
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

    def preprocess(
        self, da: DocumentArray, user_input: UserInput, is_indexing=False
    ) -> DocumentArray:
        if is_indexing:

            def convert_fn(d: Document):
                try:
                    if d.blob == b'':
                        if d.uri:
                            d.load_uri_to_blob()
                        elif d.tensor is not None:
                            d.convert_tensor_to_blob()
                    sample_video(d)
                except Exception as e:
                    print(f'Failed to process {d.id}, error: {e}')
                return d

            for d in da:
                convert_fn(d)

            return DocumentArray(d for d in da if d.blob != b'')
        else:

            def convert_fn(d: Document):
                d.chunks = DocumentArray(d for d in d.chunks if d.text)
                return d

            for d in da:
                convert_fn(d)

            return DocumentArray(d for d in da if d.chunks)

    @property
    def max_request_size(self) -> int:
        """Max number of documents in one request"""
        return 2


def select_frames(num_selected_frames, num_total_frames):
    partition_size = num_total_frames / (num_selected_frames + 1)
    return [round(partition_size * (i + 1)) for i in range(num_selected_frames)]


def sample_video(d):
    video = d.blob
    video_io = io.BytesIO(video)
    gif = PIL.Image.open(video_io)
    frame_indices = select_frames(NUM_FRAMES_SAMPLED, gif.n_frames)
    frames = []
    for i in frame_indices:
        gif.seek(i)
        frame = np.array(gif.convert("RGB"))
        frame_pil = PIL.Image.fromarray(frame)
        frame_pil_resized = frame_pil.resize((224, 224))
        frames.append(frame_pil_resized)
        frame_bytes = io.BytesIO()
        frame_pil_resized.save(frame_bytes, format="JPEG", quality=70)
        d.chunks.append(Document(uri=d.uri, blob=frame_bytes.getvalue(), tags=d.tags))
