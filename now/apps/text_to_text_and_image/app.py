import json
import os
from typing import Dict, Optional, List

from docarray import DocumentArray

from now.finetuning.run_finetuning import finetune
from now.finetuning.settings import parse_finetune_settings, FinetuneSettings
from now_common.preprocess import preprocess_nested_docs, preprocess_text

from now.apps.base.app import JinaNOWApp
from now.constants import Apps, Modalities, NOW_PREPROCESSOR_VERSION, ModelNames
from now.finetuning.data_builder import DataBuilder
from now.now_dataclasses import UserInput, DialogOptions, Task


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

    @property
    def options(self) -> List[DialogOptions]:
        task_config_option = DialogOptions(
            name='task_config',
            prompt_message='Please enter the path to your task configuration:',
            prompt_type='input',
            post_func=lambda user_input, **kwargs: self._read_task_config(
                kwargs['task_config'], user_input
            ),
        )
        return [task_config_option]

    @staticmethod
    def _read_task_config(task_config_path: str, user_input: UserInput):
        with open(task_config_path) as f:
            dct = json.load(f)
            user_input.task_config = Task(**dct)

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
        data = DataBuilder(dataset=dataset, config=user_input.task_config).build()
        env_dict = {}
        for encoder_data, encoder_type in data:
            finetune_settings = self._construct_finetune_settings(
                user_input=user_input,
                dataset=dataset,
                encoder_type=encoder_type,
            )
            artifact_id, token = finetune(
                finetune_settings=finetune_settings,
                app_instance=self,
                dataset=encoder_data,
                user_input=user_input,
                env_dict={},
                kubectl_path=kubectl_path,
            )

            env_dict['JINA_TOKEN'] = token
            if finetune_settings.model_name == ModelNames.CLIP:
                env_dict['CLIP_ARTIFACT'] = artifact_id
            elif finetune_settings.model_name == ModelNames.SBERT:
                env_dict['SBERT_ARTIFACT'] = artifact_id
            else:
                print(f'{self.app_name} only expects CLIP or SBERT models.')
                raise

        env_dict[
            'PREPROCESSOR_NAME'
        ] = f'jinahub+docker://NOWPreprocessor/v{NOW_PREPROCESSOR_VERSION}'
        env_dict['APP'] = self.app_name
        self.set_flow_yaml()

        return env_dict

    def _construct_finetune_settings(
        self,
        user_input: UserInput,
        dataset: DocumentArray,
        encoder_type: str,
    ) -> FinetuneSettings:
        finetune_settings = parse_finetune_settings(
            user_input=user_input,
            dataset=dataset,
            model_name=self._model_name(encoder_type),
            loss=self._loss_function(encoder_type),
            add_embeddings=False,
        )
        # temporary adjustments to work with small text+image dataset
        finetune_settings.epochs = 2
        finetune_settings.num_val_queries = 5
        finetune_settings.train_val_split_ration = 0.8
        return finetune_settings

    @staticmethod
    def _model_name(encoder_type: Optional[str] = None) -> str:
        """Name of the model used in case of fine-tuning."""
        if encoder_type == 'text-to-text':
            return ModelNames.SBERT
        elif encoder_type == 'text-to-image':
            return ModelNames.CLIP

    @staticmethod
    def _loss_function(encoder_type: Optional[str] = None) -> str:
        """Loss function used during fine-tuning."""
        if encoder_type == 'text-to-image':
            return 'CLIPLoss'
        return 'TripletMarginLoss'
