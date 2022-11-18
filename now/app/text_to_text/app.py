import os
from typing import Dict

from docarray import DocumentArray

from now.app.base.app import JinaNOWApp
from now.common.preprocess import preprocess_text, filter_data
from now.common.utils import common_setup, get_indexer_config
from now.constants import Apps, DatasetTypes, Modalities, ModelDimensions
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

    def get_index_query_access_paths(self, **kwargs) -> str:
        """If `split_by_sentences` is set to True, the structure of the data
        will have 2 level chunks. (That's the puspose of @cc)
        Otherwise, we access documents on chunk level. (@c)
        """
        return '@c,cc'

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)

        flow_dir = os.path.abspath(os.path.join(__file__, '..'))

        if finetuning:
            self.flow_yaml = os.path.join(flow_dir, 'ft-flow-sbert.yml')
        else:
            self.flow_yaml = os.path.join(flow_dir, 'flow-sbert.yml')

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path: str
    ) -> Dict:
        indexer_config = get_indexer_config(
            len(dataset),
            kubectl_path=kubectl_path,
            deployment_type=user_input.deployment_type,
        )
        env_dict = common_setup(
            app_instance=self,
            user_input=user_input,
            dataset=dataset,
            encoder_uses='TransformerSentenceEncoder/v0.4',
            encoder_uses_with={
                'pretrained_model_name_or_path': 'sentence-transformers/msmarco-distilbert-base-v4'
            },
            pre_trained_embedding_size=ModelDimensions.SBERT,
            indexer_uses=indexer_config['indexer_uses'],
            kubectl_path=kubectl_path,
            indexer_resources=indexer_config['indexer_resources'],
        )
        env_dict['HOSTS'] = indexer_config.get('hosts', None)
        super().setup(dataset, user_input, kubectl_path)
        return env_dict

    def preprocess(
        self,
        da: DocumentArray,
        user_input: UserInput,
        process_index: bool = False,
        process_query: bool = True,
    ) -> DocumentArray:
        if not process_query and not process_index:
            raise Exception(
                'Either `process_query` or `process_index` must be set to True.'
            )

        split_by_sentences = False
        if (
            process_index
            and user_input.dataset_type == DatasetTypes.PATH
            and user_input.dataset_path
            and os.path.isdir(user_input.dataset_path)
        ):
            # for text loaded from folder can't assume it is split by sentences
            split_by_sentences = True
        da = preprocess_text(da=da, split_by_sentences=split_by_sentences)
        return filter_data(da, modalities=[Modalities.TEXT])
