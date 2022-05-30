from typing import Dict, List

from docarray import DocumentArray
from now_common import options

from now.apps.base.app import JinaNOWApp
from now.constants import Modalities
from now.run_backend import finetune_flow_setup


class text_to_image(JinaNOWApp):
    @property
    def description(self) -> str:
        return 'Text to image search'

    @property
    def input_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def output_modality(self) -> Modalities:
        return Modalities.IMAGE

    @property
    def options(self) -> List[Dict]:
        return [options.QUALITY_CLIP]

    def setup(self, da: DocumentArray, user_config: Dict, kubectl_path) -> Dict:
        return finetune_flow_setup(self, da, user_config, kubectl_path)
