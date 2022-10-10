import os
import subprocess
from time import sleep

import yaml

QDRANT_CONFIG_PATH = '/qdrant/config/production.yaml'


def setup_qdrant_server(workspace, logger):
    if workspace and os.path.exists(QDRANT_CONFIG_PATH):
        logger.info('set new storage to network file system location in WOLF')
        qdrant_config = yaml.safe_load(open(QDRANT_CONFIG_PATH))
        qdrant_config['storage'] = {
            'storage_path': os.path.join(workspace, 'user_input.json')
        }
        yaml.safe_dump(qdrant_config, open(QDRANT_CONFIG_PATH, 'w'))
        logger.info('if qdrant exists, then start it')
    try:
        subprocess.Popen(['./run-qdrant.sh'])
        sleep(3)
        logger.info('Qdrant server started')
    except FileNotFoundError:
        logger.info('Qdrant not found, locally. So it won\'t be started.')
