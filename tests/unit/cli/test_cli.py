import os
import subprocess

import pytest


def test_start_with_parameters():
    """Checks if now can be started from cli"""
    dataset_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'resources', 'image'
    )
    bash_command = (
        f"python main.py start --app image_text_retrieval --output_modality image --dataset_path "
        f"{dataset_path} --dataset_type path --deployment_type remote --flow_name test --secured false "
        f"--search_fields a,b,c"
    )
    print(bash_command)
    with pytest.raises(subprocess.TimeoutExpired):
        # timeout means that the parsing was successful and the process is running
        subprocess.run(bash_command.split(' '), timeout=10)
