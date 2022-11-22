import os
from typing import Dict, Tuple

from docarray import DocumentArray

from now.app.base.app import JinaNOWApp
from now.common.preprocess import preprocess_images, preprocess_text, filter_data
from now.common.utils import _get_clip_apps_with_dict, common_setup, get_indexer_config
from now.constants import CLIP_USES, Apps, DatasetTypes, Modalities
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

    def get_index_query_access_paths(self, **kwargs) -> str:
        """If `split_by_sentences` is set to True, the structure of the data
        will have 2 level chunks. (That's the puspose of @cc)
        Otherwise, we access documents on chunk level. (@c)
        """
        return '@c,cc'

    @property
    def finetune_datasets(self) -> [Tuple]:
        return (DemoDatasetNames.DEEP_FASHION, DemoDatasetNames.BIRD_SPECIES)

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)

        now_package_dir = os.path.abspath(
            os.path.join(__file__, '..', '..', '..', '..')
        )
        flow_dir = os.path.join(now_package_dir, 'now', 'common', 'flow')

        if finetuning:
            self.flow_yaml = os.path.join(flow_dir, 'ft-flow-clip.yml')
        else:
            self.flow_yaml = os.path.join(flow_dir, 'flow-clip.yml')

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        indexer_config = get_indexer_config(len(dataset))
        encoder_with = _get_clip_apps_with_dict(user_input)
        env_dict = common_setup(
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
        super().setup(dataset=dataset, user_input=user_input, kubectl_path=kubectl_path)
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
        modalities = []
        if process_index:
            split_by_sentences = False
            if (
                user_input
                and user_input.dataset_type == DatasetTypes.PATH
                and user_input.dataset_path
                and os.path.isdir(user_input.dataset_path)
            ):
                # for text loaded from folder can't assume it is split by sentences
                split_by_sentences = True
            da = preprocess_text(da=da, split_by_sentences=split_by_sentences)
            modalities.append(Modalities.TEXT)
        if process_query:
            da = preprocess_images(da=da)
            modalities.append(Modalities.IMAGE)

        return filter_data(da, modalities)
