import os
import subprocess

import pytest


def test_start_with_parameters():
    """Checks if now can be started from cli"""
    dataset_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'resources', 'image'
    )
    python_file = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'main.py')
    bash_command = (
        f"python {python_file} start --app search_app --search_fields .jpg --dataset_path "
        f"{dataset_path} --dataset_type path --deployment_type remote --flow_name test --secured false"
    )
    with pytest.raises(subprocess.TimeoutExpired):
        # timeout means that the parsing was successful and the process is running
        process = subprocess.Popen(bash_command.split(' '))
        process.wait(timeout=10)
