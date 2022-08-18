import os
from typing import Dict, List, Tuple

from docarray import DocumentArray
from now_common import options
from now_common.utils import (
    get_indexer_config,
    preprocess_images,
    setup_clip_music_apps,
)

from now.apps.base.app import JinaNOWApp
from now.constants import (
    CLIP_USES,
    IMAGE_MODEL_QUALITY_MAP,
    Apps,
    DemoDatasets,
    Modalities,
    Qualities,
)
from now.now_dataclasses import UserInput


class ImageToImage(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.IMAGE_TO_IMAGE

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Image to image search app'

    @property
    def input_modality(self) -> Modalities:
        return Modalities.IMAGE

    @property
    def output_modality(self) -> Modalities:
        return Modalities.IMAGE

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 8

    @property
    def finetune_datasets(self) -> [Tuple]:
        return (DemoDatasets.DEEP_FASHION, DemoDatasets.BIRD_SPECIES)

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)

        now_package_dir = os.path.abspath(
            os.path.join(__file__, '..', '..', '..', '..')
        )
        flow_dir = os.path.join(now_package_dir, 'now_common', 'flow')

        if finetuning:
            self.flow_yaml = os.path.join(flow_dir, 'ft-flow-clip.yml')
        else:
            self.flow_yaml = os.path.join(flow_dir, 'flow-clip.yml')

    @property
    def options(self) -> List[Dict]:
        return [options.QUALITY_CLIP]

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
        indexer_config = get_indexer_config(len(dataset))
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
        return preprocess_images(da=da)
