import os
from typing import Dict, Tuple

from docarray import DocumentArray

from now.app.base.app import JinaNOWApp
from now.common.utils import _get_clip_apps_with_dict, common_setup, get_indexer_config
from now.constants import CLIP_USES, Apps
from now.demo_data import DemoDatasetNames
from now.now_dataclasses import UserInput


class SearchApp(JinaNOWApp):
    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.SEARCH_APP

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Image-text search app'

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 8

    @property
    def finetune_datasets(self) -> [Tuple]:
        return (DemoDatasetNames.DEEP_FASHION, DemoDatasetNames.BIRD_SPECIES)

    def set_flow_yaml(self, **kwargs):
        now_package_dir = os.path.abspath(
            os.path.join(__file__, '..', '..', '..', '..')
        )
        flow_dir = os.path.join(now_package_dir, 'now', 'common', 'flow')
        self.flow_yaml = os.path.join(flow_dir, 'flow-clip.yml')

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path, data_class
    ) -> Dict:
        indexer_config = get_indexer_config()
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
            data_class=data_class,
        )
        super().setup(
            dataset=dataset,
            user_input=user_input,
            kubectl_path=kubectl_path,
            data_class=data_class,
        )
        return env_dict
