import os
from typing import Dict, List

from docarray import DocumentArray
from now_common.preprocess import preprocess_text
from now_common.utils import common_setup, get_indexer_config

from now.apps.base.app import JinaNOWApp
from now.constants import Apps, DatasetTypes, Modalities
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
    def supported_file_types(self) -> List[str]:
        return ['txt']

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path: str
    ) -> Dict:
        indexer_config = get_indexer_config(len(dataset))
        return common_setup(
            app_instance=self,
            user_input=user_input,
            dataset=dataset,
            encoder_uses='TransformerSentenceEncoder/v0.4',
            encoder_uses_with={
                'pretrained_model_name_or_path': 'sentence-transformers/msmarco-distilbert-base-v4'
            },
            pre_trained_embedding_size=768,
            indexer_uses=indexer_config['indexer_uses'],
            kubectl_path=kubectl_path,
            indexer_resources=indexer_config['indexer_resources'],
        )

    def preprocess(
        self, da: DocumentArray, user_input: UserInput, is_indexing=False
    ) -> DocumentArray:
        split_by_sentences = False
        if (
            is_indexing
            and user_input.data == 'custom'
            and user_input.custom_dataset_type == DatasetTypes.PATH
            and user_input.dataset_path
            and os.path.isdir(user_input.dataset_path)
        ):
            # for text loaded from folder can't assume it is split by sentences
            split_by_sentences = True
        return preprocess_text(da=da, split_by_sentences=split_by_sentences)
