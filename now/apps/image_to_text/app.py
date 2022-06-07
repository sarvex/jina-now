from typing import Dict, List

from docarray import DocumentArray
from now_common import options

from now.apps.base.app import JinaNOWApp
from now.constants import CLIP_USES, IMAGE_MODEL_QUALITY_MAP, Apps, Modalities
from now.dataclasses import UserInput
from now.run_backend import finetune_flow_setup


class ImageToText(JinaNOWApp):
    @property
    def description(self) -> str:
        return 'Image to text search'

    @property
    def input_modality(self) -> Modalities:
        return Modalities.IMAGE

    @property
    def output_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def options(self) -> List[Dict]:
        return [options.QUALITY_CLIP]

    def set_app_parser(self, parser, formatter):
        parser = parser.add_parser(
            Apps.IMAGE_TO_TEXT,
            help='Image To Text App.',
            description='Create an `Image To Text` app.',
            formatter_class=formatter,
        )
        super().set_app_parser(parser, formatter)

    def setup(self, da: DocumentArray, user_config: UserInput, kubectl_path) -> Dict:
        return finetune_flow_setup(
            self,
            da,
            user_config,
            kubectl_path,
            encoder_uses=CLIP_USES,
            artifact=IMAGE_MODEL_QUALITY_MAP[user_config.quality][1],
        )
