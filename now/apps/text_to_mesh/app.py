import os
from typing import Dict, List

from docarray import DocumentArray
from now_common import options

from now.apps.base.app import JinaNOWApp
from now.constants import (
    CLIP_USES,
    IMAGE_MODEL_QUALITY_MAP,
    Apps,
    DemoDatasets,
    Modalities,
    Qualities,
)
from now.dataclasses import UserInput
from now.run_backend import finetune_flow_setup


class TextToMesh(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app(self) -> str:
        return Apps.TEXT_TO_MESH

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Text to mesh search app'

    @property
    def input_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def output_modality(self) -> Modalities:
        return Modalities.MESH

    def set_flow_yaml(self, **kwargs):
        flow_dir = os.path.join(os.path.join(__file__, '..'))
        self.flow_yaml = os.path.join(flow_dir, 'flow-clip.yml')

    @property
    def pre_trained_embedding_size(self) -> Dict[Qualities, int]:
        return {
            Qualities.MEDIUM: 512,
            Qualities.GOOD: 512,
            Qualities.EXCELLENT: 768,
        }

    def setup(self, da: DocumentArray, user_config: UserInput, kubectl_path) -> Dict:
        return finetune_flow_setup(
            self,
            da,
            user_config,
            kubectl_path,
            encoder_uses=CLIP_USES,
            encoder_uses_with={
                'pretrained_model_name_or_path': IMAGE_MODEL_QUALITY_MAP[
                    user_config.quality
                ][1]
            },
            finetune_datasets=(DemoDatasets.DEEP_FASHION, DemoDatasets.BIRD_SPECIES),
            indexer_uses='DocarrayIndexer',
        )
