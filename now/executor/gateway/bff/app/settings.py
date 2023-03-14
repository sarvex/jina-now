import json
import logging
import os

from now.now_dataclasses import UserInput

logger = logging.getLogger(__name__)

ENV_PREFIX = 'JINA_NOW_'

# server
DEFAULT_WORKERS = 1
DEFAULT_PORT = 8080
DEFAULT_BACKLOG = 2048

# debug flag
DEFAULT_DEBUG = True


user_input_in_bff = UserInput()


def init_user_input_in_bff():
    global user_input_in_bff
    try:
        with open(os.path.join(os.path.expanduser('~'), 'user_input.json'), 'r') as f:
            user_input_dict = json.load(f)
        for attr_name, prev_value in user_input_in_bff.__dict__.items():
            setattr(
                user_input_in_bff,
                attr_name,
                user_input_dict.get(attr_name, prev_value),
            )
    except FileNotFoundError:
        print('Could not find user input file in BFF')
        print(f'used path: {os.path.join(os.path.expanduser("~"), "user_input.json")}')
        print('but this can be okay')


init_user_input_in_bff()
