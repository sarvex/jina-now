import signal
import sys
from collections.abc import MutableMapping
from inspect import stack
from typing import Any

from pyfiglet import Figlet


def debug(msg: Any):
    """
    Prints a message along with the details of the caller, ie filename and line number.
    """
    msg = str(msg) if msg is not None else ''
    frameinfo = stack()[1]
    print(f"{frameinfo.filename}:{frameinfo.lineno}: {msg}")


def flatten_dict(d, parent_key='', sep='__'):
    """
    This function converts a nested dictionary into a dictionary of attirbutes using '__' as a separator.
    Example:
        {'a': {'b': {'c': 1, 'd': 2}}} -> {'a__b__c': 1, 'a__b__c': 2}
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            # TODO for now, we just have string values, str(v) should be removed once we support numeric values
            items.append((new_key, str(v)))
    return dict(items)


def hide_string_chars(s):
    return ''.join(['*' for _ in range(len(s) - 4)]) + s[len(s) - 4 :] if s else None


def to_camel_case(text):
    """Convert text to camel case in great coding style"""
    s = text.replace("_", " ")
    s = s.split()
    return ''.join(i.capitalize() for i in s)


def my_handler(signum, frame, spinner):
    with spinner.hidden():
        sys.stdout.write("Program terminated!\n")
    spinner.stop()
    exit(0)


class BetterEnum:
    def __iter__(self):
        return [getattr(self, x) for x in dir(self) if ('__' not in x)].__iter__()


sigmap = {signal.SIGINT: my_handler, signal.SIGTERM: my_handler}


def print_headline():
    f = Figlet(font='slant')
    print('Welcome to:')
    print(f.renderText('Jina NOW'))
    print('Get your search use case up and running - end to end.\n')
    print(
        'You can choose between image and text search. \nJina NOW trains a model, pushes it to Jina AI Cloud '
        'and deploys a Flow and playground app in the cloud or locally. \nCheck out our demos or bring '
        'your own data.\n'
    )
    print('Visit docs.jina.ai to learn more about our framework')
    print(
        '💡 Make sure you give enough memory to your Docker daemon. '
        '5GB - 8GB should be okay.'
    )
    print()
