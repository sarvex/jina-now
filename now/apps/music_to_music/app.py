from typing import Dict, List

import cowsay
from docarray import DocumentArray
from now_common import options

from now.apps.base.app import JinaNOWApp
from now.deployment.deployment import which


class music_to_music(JinaNOWApp):
    @property
    def description(self) -> str:
        return 'Music to music app'

    @property
    def input_modality(self) -> str:
        raise NotImplementedError()

    @property
    def output_modality(self) -> str:
        raise NotImplementedError()

    @property
    def flow_yaml(self) -> str:
        pass

    @property
    def options(self) -> List[Dict]:
        return [options.QUALITY_CLIP]

    def check_requirements(self) -> bool:
        if not ffmpeg_is_installed():
            _handle_ffmpeg_install_required()
            return False
        return True

    def setup(self, da: DocumentArray, user_config: Dict) -> Dict:

        raise NotImplementedError()

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
