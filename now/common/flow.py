import os


def get_executor_prefix():
    if os.environ.get('NOW_TESTING', False):
        return 'jinahub+docker://'
    return f'jinahub://'
