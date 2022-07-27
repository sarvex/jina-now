import os
from typing import Dict, List

from docarray import DocumentArray
from now_common import options
from now_common.utils import preprocess_text, setup_clip_music_apps

from now.apps.base.app import JinaNOWApp
from now.constants import (
    CLIP_USES,
    IMAGE_MODEL_QUALITY_MAP,
    Apps,
    DatasetTypes,
    Modalities,
    Qualities,
)
from now.now_dataclasses import UserInput


class ImageToText(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.IMAGE_TO_TEXT

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Image to text search app'

    @property
    def input_modality(self) -> Modalities:
        return Modalities.IMAGE

    @property
    def output_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 8

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)
        encode = kwargs.get('encode', False)
        if finetuning + encode > 1:
            raise ValueError(
                f"Can't set flow to more than one mode but have encode={encode}, finetuning={finetuning}"
            )

        now_package_dir = os.path.abspath(
            os.path.join(__file__, '..', '..', '..', '..')
        )
        flow_dir = os.path.join(now_package_dir, 'now_common', 'flow')

        if finetuning:
            self.flow_yaml = os.path.join(flow_dir, 'ft-flow-clip.yml')
        elif encode:
            self.flow_yaml = os.path.join(flow_dir, 'encode-flow-clip.yml')
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
            indexer_uses='DocarrayIndexerV2',
            kubectl_path=kubectl_path,
        )

    def load_from_folder(self, path: str) -> DocumentArray:
        return DocumentArray.from_files(path + '/*.txt')

    def preprocess(self, da: DocumentArray, user_input: UserInput) -> DocumentArray:
        split_by_sentences = False
        if (
            user_input.is_custom_dataset
            and user_input.custom_dataset_type == DatasetTypes.PATH
            and user_input.dataset_path
            and os.path.isdir(user_input.dataset_path)
        ):
            # for text loaded from folder can't assume it is split by sentences
            split_by_sentences = True
        return preprocess_text(da=da, split_by_sentences=split_by_sentences)
