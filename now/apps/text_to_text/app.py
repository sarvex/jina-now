import os
from typing import Dict, List

from docarray import DocumentArray
from now_common.utils import get_indexer_config, preprocess_text, setup_clip_music_apps

from now.apps.base.app import JinaNOWApp
from now.constants import CLIP_USES, Apps, DatasetTypes, Modalities, Qualities
from now.now_dataclasses import UserInput


class TextToText(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.TEXT_TO_TEXT

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Text to text search app'

    @property
    def input_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def output_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 8

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)

        flow_dir = os.path.abspath(os.path.join(__file__, '..'))

        if finetuning:
            self.flow_yaml = os.path.join(flow_dir, 'ft-flow-sbert.yml')
        else:
            self.flow_yaml = os.path.join(flow_dir, 'flow-sbert.yml')

    @property
    def supported_wildcards(self) -> List[str]:
        return ['*.txt']

    @property
    def pre_trained_embedding_size(self) -> Dict[Qualities, int]:
        return {Qualities.MEDIUM: 512, Qualities.EXCELLENT: 768}

    @property
    def options(self) -> List[Dict]:
        return [
            {
                'name': 'quality',
                'choices': [
                    {'name': '🦊 medium', 'value': Qualities.MEDIUM},
                    {'name': '🦄 excellent', 'value': Qualities.EXCELLENT},
                ],
                'prompt_message': 'What quality do you expect?',
                'prompt_type': 'list',
                'description': 'Choose the quality of the model that you would like to finetune',
            }
        ]

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path: str
    ) -> Dict:
        quality_pretrained_model_map = {
            Qualities.MEDIUM: 'openai/clip-vit-base-patch32',
            Qualities.EXCELLENT: 'sentence-transformers/msmarco-distilbert-base-v4',
        }
        pretrained_model_name_or_path = quality_pretrained_model_map[user_input.quality]
        if pretrained_model_name_or_path.startswith('openai'):
            encoder_uses = CLIP_USES
        else:
            encoder_uses = 'TransformerSentenceEncoder/v0.4'
        indexer_config = get_indexer_config(len(dataset))
        return setup_clip_music_apps(
            app_instance=self,
            user_input=user_input,
            dataset=dataset,
            encoder_uses=encoder_uses,
            encoder_uses_with={
                'pretrained_model_name_or_path': pretrained_model_name_or_path
            },
            indexer_uses=indexer_config['indexer_uses'],
            indexer_resources=indexer_config['indexer_resources'],
            kubectl_path=kubectl_path,
        )

    def preprocess(
        self, da: DocumentArray, user_input: UserInput, is_indexing=False
    ) -> DocumentArray:
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
