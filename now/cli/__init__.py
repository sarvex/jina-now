import os
import pathlib
import sys
import warnings
from argparse import Namespace

from now import __version__
from now import __version__ as version
from now import run_all_k8s

warnings.filterwarnings("ignore")

cur_dir = pathlib.Path(__file__).parents[1].resolve()

os.environ['JINA_CHECK_VERSION'] = 'False'
os.environ['JCLOUD_LOGLEVEL'] = 'ERROR'
# fix


def get_run_args():
    from now.cli.parser import get_main_parser

    parser = get_main_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        exit()
    args, unknown = parser.parse_known_args()

    # clean up the args with None values
    args = {k: v for k, v in vars(args).items() if v is not None}
    # Convert args back to Namespace
    args = Namespace(**args)

    if unknown:
        raise Exception('unknown args: ', unknown)

    return args


def get_task(kwargs):
    for x in ['cli', 'now']:
        if x in kwargs:
            return kwargs[x]
    raise Exception('kwargs do not contain a task')


def cli(args=None):
    """The main entrypoint of the CLI"""
    os.environ['JINA_LOG_LEVEL'] = 'CRITICAL'
    print_version_line()
    kwargs = parse_args(args)
    task = get_task(kwargs)
    if '--version' in sys.argv[1:]:
        print(__version__)
        exit(0)
    if task == 'start':
        return run_all_k8s.start_now(**kwargs)
    elif task == 'stop':
        run_all_k8s.stop_now(**kwargs)
    elif task == 'compare':
        run_all_k8s.compare_flows(**kwargs)


def parse_args(args):
    if not args:
        args = get_run_args()
    args = vars(args)  # Make it a dict from Namespace
    return args


def print_version_line():
    if len(sys.argv) != 1 and not ('-h' in sys.argv[1:] or '--help' in sys.argv[1:]):
        print(f'Initialising Jina NOW v{version} ...')


if __name__ == '__main__':
    cli()
