import os
from typing import Dict, Optional

from docarray import DocumentArray

from now.finetuning.run_finetuning import finetune
from now.finetuning.settings import parse_finetune_settings
from now_common.preprocess import preprocess_nested_docs, preprocess_text

from now.apps.base.app import JinaNOWApp
from now.constants import Apps, Modalities
from now.finetuning.data_builder import DataBuilder
from now.now_dataclasses import UserInput


class TextToTextAndImage(JinaNOWApp):
    """
    Hybrid text to text+image search combining symbolic and neural IR approaches.
    """

    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> Apps:
        return Apps.TEXT_TO_TEXT_AND_IMAGE

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return (
            'Text to text+image search app combining symbolic and neural IR approaches.'
        )

    def set_flow_yaml(self, **kwargs):
        """configure the flow yaml in the Jina NOW app."""
        flow_dir = os.path.abspath(os.path.join(__file__, '..'))
        self.flow_yaml = os.path.join(flow_dir, 'flow.yml')

    @property
    def input_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def output_modality(self) -> Modalities:
        return Modalities.TEXT_AND_IMAGE

    def preprocess(
            self,
            da: DocumentArray,
            user_input: UserInput,
            is_indexing: Optional[bool] = False,
    ) -> DocumentArray:
        # Indexing
        if is_indexing:
            return preprocess_nested_docs(da=da, user_input=user_input)
        # Query
        else:
            return preprocess_text(da=da, split_by_sentences=False)

    def setup(
            self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        # only implements data generation
        data = DataBuilder(dataset=dataset, config=user_input.task_config).build()

        # for encoder_data, encoder_type in data:
        encoder_data, encoder_type = data[1]
        finetune_settings = parse_finetune_settings(user_input=user_input,
                                           dataset=dataset,
                                           encoder_type=encoder_type)

        artifact_id, token = finetune(
            finetune_settings=finetune_settings,
            app_instance=self,
            dataset=encoder_data,
            user_input=user_input,
            env_dict={},
            kubectl_path=kubectl_path,
        )
        import time
        time.sleep(10)
        exit(0)

        return {}

    @property
    def finetuning_model_name(self, encoder_type: Optional[str] = None) -> str:
        """Name of the model used in case of fine-tuning."""
        if encoder_type == 'text_to_text':
            return 'sentence-transformers/msmarco-distilbert-base-v3'
        elif encoder_type == 'text_to_image':
            return 'openai/clip-vit-base-patch32'

    @property
    def loss_function(self, encoder_type: Optional[str] = None) -> str:
        """Loss function used during fine-tuning."""
        if encoder_type == 'text_to_image':
            return 'CLIPLoss'
        return 'TripletMarginLoss'

    @property
    def add_embeddings(self) -> bool:
        """Whether we need to calculate embeddings before fine-tuning or not."""
        raise False
