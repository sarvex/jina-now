import os
from typing import Dict, List, Tuple

from docarray import DocumentArray
from jina.helper import random_port
from now_common.preprocess import preprocess_images, preprocess_text
from now_common.utils import common_setup, get_indexer_config

from now.apps.base.app import JinaNOWApp
from now.constants import CLIP_USES, EXTERNAL_CLIP_HOST, Apps, DatasetTypes, Modalities
from now.demo_data import DemoDatasetNames
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

    @property
    def finetune_datasets(self) -> [Tuple]:
        return (DemoDatasetNames.DEEP_FASHION, DemoDatasetNames.BIRD_SPECIES)

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
    def supported_file_types(self) -> List[str]:
        return ['txt']

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        indexer_config = get_indexer_config(len(dataset))
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
            split_by_sentences = False
            if (
                user_input
                and user_input.dataset_type == DatasetTypes.PATH
                and user_input.dataset_path
                and os.path.isdir(user_input.dataset_path)
            ):
                # for text loaded from folder can't assume it is split by sentences
                split_by_sentences = True
            return preprocess_text(da=da, split_by_sentences=split_by_sentences)
        else:
            return preprocess_images(da=da)
