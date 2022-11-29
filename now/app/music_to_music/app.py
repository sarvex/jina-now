import os
from typing import Dict, List

import cowsay
from docarray import DocumentArray

from now.app.base.app import JinaNOWApp
from now.common.utils import common_setup
from now.constants import (
    EXECUTOR_PREFIX,
    NOW_QDRANT_INDEXER_VERSION,
    Apps,
    Modalities,
    ModelDimensions,
)
from now.demo_data import DemoDatasetNames
from now.deployment.deployment import which
from now.now_dataclasses import UserInput


class MusicToMusic(JinaNOWApp):
    """
    Music2Music search.

    Pre-trained head weights are stored here
    https://console.cloud.google.com/storage/browser/jina-fashion-data/model/music?project=jina-simpsons-florian
    To re-built, go in now/hub/head_encoder with place the model weights in this folder
    and run "jina hub push --private . -t linear_head_encoder_music_2k"
    """

    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> str:
        return Apps.MUSIC_TO_MUSIC

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def description(self) -> str:
        return 'Music to music search app'

    @property
    def input_modality(self) -> List[Modalities]:
        return [Modalities.MUSIC]

    @property
    def output_modality(self) -> List[Modalities]:
        return [Modalities.MUSIC]

    @property
    def required_docker_memory_in_gb(self) -> int:
        return 10

    def set_flow_yaml(self, **kwargs):
        finetuning = kwargs.get('finetuning', False)
        demo_data = kwargs.get('demo_data', False)
        if finetuning + demo_data > 1:
            raise ValueError(
                f"Can't set flow to more than one mode but have "
                f"demo_data={demo_data}, finetuning={finetuning}"
            )

        flow_dir = os.path.abspath(os.path.join(__file__, '..'))

        if demo_data:
            self.flow_yaml = os.path.join(flow_dir, 'demo-data-flow-music.yml')
        else:
            self.flow_yaml = os.path.join(flow_dir, 'ft-flow-music.yml')

    def _check_requirements(self) -> bool:
        if not ffmpeg_is_installed():
            _handle_ffmpeg_install_required()
            return False
        return True

    def setup(
        self, dataset: DocumentArray, user_input: UserInput, kubectl_path
    ) -> Dict:
        # needed to write a custom solution for music 2 music app as we need to allow to integrate
        # externally pretrained executors for the demo datasets
        pre_trained_head_map = {
            DemoDatasetNames.MUSIC_GENRES_ROCK: 'FinetunedLinearHeadEncoderMusicRock',
            DemoDatasetNames.MUSIC_GENRES_MIX: 'FineTunedLinearHeadEncoderMusicMix',
        }

        # will execute finetuning on custom datasets (if possible) but not for demo datasets
        env_dict = common_setup(
            app_instance=self,
            user_input=user_input,
            dataset=dataset,
            encoder_uses='BiModalMusicTextEncoderV2',
            encoder_uses_with={},
            indexer_uses=f'NOWQdrantIndexer16/{NOW_QDRANT_INDEXER_VERSION}',
            kubectl_path=kubectl_path,
            indexer_resources={},
            pre_trained_embedding_size=ModelDimensions.CLIP,
        )

        # can reuse large part of other code but need to make some adjustments
        if user_input.dataset_name in pre_trained_head_map:
            print(f'⚡️ Using cached hub model for speed')

            env_dict[
                'LINEAR_HEAD_NAME'
            ] = f"{EXECUTOR_PREFIX}{pre_trained_head_map[user_input.dataset_name]}"

            self.set_flow_yaml(demo_data=True)
        super().setup(dataset=dataset, user_input=user_input, kubectl_path=kubectl_path)

        return env_dict


def ffmpeg_is_installed():
    return which("ffmpeg")


def _handle_ffmpeg_install_required():
    bc_red = '\033[91m'
    bc_end = '\033[0m'
    print()
    print(
        f"{bc_red}To use the audio modality you need the ffmpeg audio processing"
        f" library installed on your system.{bc_end}"
    )
    print(
        f"{bc_red}For MacOS please run 'brew install ffmpeg' and on"
        f" Linux 'apt-get install ffmpeg libavcodec-extra'.{bc_end}"
    )
    print(
        f"{bc_red}After the installation, restart Jina Now and have fun with music search 🎸!{bc_end}"
    )
    cowsay.cow('see you soon 👋')
    exit(1)
