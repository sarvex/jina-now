import argparse

from jina.parsers.helper import _ColoredHelpFormatter

from now import __version__
from now.common import options
from now.common.options import construct_app
from now.constants import Apps


def set_base_parser():
    """Set the base parser
    :return: the parser
    """

    # create the top-level parser
    urls = {
        'Code': ('💻', 'https://github.com/jina-ai/now'),
        'Jina Docs': ('📖', 'https://docs.jina.ai'),
        'Help': ('💬', 'https://slack.jina.ai'),
        'Hiring!': ('🙌', 'https://career.jina.ai'),
    }
    url_str = '\n'.join(f'- {v[0]:<10} {k:10.10}\t{v[1]}' for k, v in urls.items())

    parser = argparse.ArgumentParser(
        epilog=f'Jina NOW - get your neural search case up in minutes. \n\n{url_str}',
        formatter_class=_chf,
        description='Command Line Interface of `%(prog)s`',
    )
    parser.add_argument(
        '-v',
        '--version',
        action='version',
        version=__version__,
        help='Show Jina version',
    )
    return parser


def set_help_parser(parser=None):
    """Set the parser for the jina help lookup
    :param parser: an optional existing parser to build upon
    :return: the parser
    """

    if not parser:
        from jina.parsers.base import set_base_parser

        parser = set_base_parser()

    parser.add_argument(
        'query',
        type=str,
        help='Lookup the usage & mention of the argument name in Jina NOW',
    )
    return parser


def set_start_parser(sp):
    """Add the arguments for the jina now to the parser
    :param parser: an optional existing parser to build upon
    :return: the parser
    """

    parser = sp.add_parser(
        'start',
        help='Start jina now and create or reuse a cluster.',
        description='Start jina now and create or reuse a cluster.',
        formatter_class=_chf,
    )

    # Get list of enabled apps
    enabled_apps, enabled_apps_instance = [], []
    for app in Apps():
        app_instance = construct_app(app)
        if app_instance.is_enabled:
            enabled_apps.append(app)
            enabled_apps_instance.append(app_instance)

    parser.add_argument(
        '--app',
        help='Select the app you would like to use. Do not use this argument when'
        ' using the `%(prog)-8s [sub-command]`',
        choices=enabled_apps,
        type=str,
    )

    # Add common app options
    for option in options.base_options:
        if getattr(option, 'is_terminal_command', False):
            _kwargs = {
                'help': option.description,
                'type': str,
            }
            _kwargs.update(option.argparse_kwargs)
            parser.add_argument(
                f'--{option.name}',
                **_kwargs,
            )

    # Add app sub-command and its options
    sub_parser = parser.add_subparsers(
        dest='app',
        description='use `%(prog)-8s [sub-command] --help` '
        'to get additional arguments to be used with each sub-command',
        required=False,
    )

    # Set parser args for the enabled apps
    for app_instance in enabled_apps_instance:
        app_instance.set_app_parser(sub_parser, formatter=_chf)


def set_stop_parser(sp):
    sp.add_parser(
        'stop',
        help='Stop jina now and remove local cluster.',
        description='Stop jina now and remove local cluster.',
        formatter_class=_chf,
    )


def set_survey_parser(sp):
    sp.add_parser(
        'survey',
        help='Opens a survey in the browser. Thanks for providing us feedback.',
        description='Opens a survey in the browser. Thanks for providing us feedback.',
        formatter_class=_chf,
    )


def set_logs_parser(sp):
    sp.add_parser(
        'logs',
        help='Fetch logs from a running pod.',
        description='Fetch logs from a running pod to help troubleshooting remote workflow.',
        formatter_class=_chf,
    )


def set_compare_parser(sp):
    parser = sp.add_parser(
        'compare',
        help='Compare the performance of different flows.',
        description='Compare the performance of different flows.',
        formatter_class=_chf,
    )

    parser.add_argument(
        '--path_semantic_scores',
        help='Path to json file mapping flow IDs to a list of semantic scores configurations',
        type=str,
    )

    parser.add_argument(
        '--flow_ids',
        help='Flow IDs to compare',
        type=str,
    )

    parser.add_argument(
        '--dataset',
        help='Path to local or name of pushed DocArray with the queries in multi-modal format',
        type=str,
    )

    parser.add_argument(
        '--limit',
        help='Number of results to return',
        type=int,
        default=9,
    )

    parser.add_argument(
        '--disable_to_datauri',
        help='Disable converting images to datauri; makes the files smaller but also not self-contained',
        action='store_true',
        default=False,
    )

    parser.add_argument(
        '--results_per_table',
        help='Number of results shown per table (default is 20)',
        type=int,
        default=20,
    )


def get_main_parser():
    """The main parser for Jina NOW
    :return: the parser
    """
    # create the top-level parser
    parser = set_base_parser()
    sp = parser.add_subparsers(
        dest='cli',
        description='',
        required=True,
    )

    set_start_parser(sp)
    set_stop_parser(sp)
    set_survey_parser(sp)
    set_logs_parser(sp)
    set_compare_parser(sp)

    return parser


_chf = _ColoredHelpFormatter
