from typing import Dict, List

from docarray import DocumentArray
from now_common import options

from now.apps.base.app import JinaNOWApp
from now.constants import Modalities
from now.run_backend import finetune_and_push_if_possible


class text_to_image(JinaNOWApp):
    def __init__(self):
        self._flow_yaml = None

    @property
    def description(self) -> str:
        return 'Text to image app'

    @property
    def input_modality(self) -> str:
        return Modalities.TEXT

    @property
    def output_modality(self) -> str:
        return Modalities.IMAGE

    @property
    def flow_yaml(self) -> str:
        """Created in the setup function"""
        return self._flow_yaml

    @flow_yaml.setter
    def flow_yaml(self, value: str):
        self._flow_yaml = value

    @property
    def options(self) -> List[Dict]:
        return [options.QUALITY_CLIP]

    def setup(self, da: DocumentArray, user_config: Dict, kubectl_path) -> Dict:
        return finetune_and_push_if_possible(self, da, user_config, kubectl_path)

    def cleanup(self, app_config: dict) -> None:
        """
        Runs after the flow is terminated.
        Cleans up the resources created during setup.
        Common examples are:
            - delete a database
            - remove artifact
            - notify other services
        :param app_config: contains all information needed to clean up the allocated resources
        """
        pass
