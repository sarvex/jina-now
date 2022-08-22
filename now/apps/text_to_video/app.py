import io
import os
from typing import Dict, List

import numpy as np
import PIL
from docarray import Document, DocumentArray
from now_common import options
from now_common.utils import _get_email, get_indexer_config, setup_clip_music_apps

from now.apps.base.app import JinaNOWApp
from now.constants import (
    CLIP_USES,
    IMAGE_MODEL_QUALITY_MAP,
    Apps,
    Modalities,
    Qualities,
)
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

    @property
    def options(self) -> List[Dict]:
        return [options.QUALITY_CLIP]

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)
        dataset_len = kwargs.get('dataset_len', 0) * NUM_FRAMES_SAMPLED
        is_jina_email = _get_email().split('@')[-1] == 'jina.ai'

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
    def pre_trained_embedding_size(self) -> Dict[Qualities, int]:
        return {
            Qualities.MEDIUM: 512,
            Qualities.GOOD: 512,
            Qualities.EXCELLENT: 768,
        }

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        indexer_config = get_indexer_config(len(dataset) * NUM_FRAMES_SAMPLED)
        return setup_clip_music_apps(
            app_instance=self,
            user_input=user_input,
            dataset=dataset,
            encoder_uses=CLIP_USES,
            encoder_uses_with={
                'pretrained_model_name_or_path': IMAGE_MODEL_QUALITY_MAP[
                    user_input.quality
                ][1]
            },
            indexer_uses=indexer_config['indexer_uses'],
            indexer_resources=indexer_config['indexer_resources'],
            kubectl_path=kubectl_path,
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
                except:
                    pass
                return d

            da.apply(convert_fn)

            return DocumentArray(d for d in da if d.blob != b'')
        else:

            def convert_fn(d: Document):
                d.chunks = d.chunks.find(query={'text': {'$exists': True}})
                return d

            da.apply(convert_fn)

            return DocumentArray(d for d in da if d.chunks)


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
        d.chunks.append(Document(uri=d.uri, blob=frame_bytes.getvalue()))
