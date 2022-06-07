import argparse

from jina.parsers.helper import _ColoredHelpFormatter

from now import __version__
from now.apps.image_to_image.app import ImageToImage
from now.apps.image_to_text.app import ImageToText
from now.apps.text_to_image.app import TextToImage
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
    url_str = '\n'.join(f'- {v[0]:<15} {k:10.10}\t{v[1]}' for k, v in urls.items())

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


def set_default_start_args(parser):
    parser.add_argument(
        '--app',
        help='Select the app you would like to use. Do not use this argument when'
        ' using the `%(prog)-8s [sub-command]`',
        choices=[Apps.IMAGE_TO_TEXT, Apps.TEXT_TO_IMAGE, Apps.IMAGE_TO_IMAGE],
        type=str,
    )

    parser.add_argument(
        '--data',
        help='Select one of the available datasets or provide local filepath, '
        'docarray url, or docarray secret to use your own dataset',
        type=str,
    )

    parser.add_argument(
        '--cluster',
        help='Reference an existing `local` cluster or select `new` to create a new one.',
        type=str,
    )

    parser.add_argument(
        '--deployment-type',
        help='Option is `local` and `remote`. Select `local` if you want search engine to be deployed on local '
        'cluster. Select `remote` to deploy it on Jina Cloud',
        type=str,
    )


def set_start_parser(parser=None):
    """Add the arguments for the jina now to the parser
    :param parser: an optional existing parser to build upon
    :return: the parser
    """

    set_default_start_args(parser)

    sub_parser = parser.add_subparsers(
        dest='app',
        description='use `%(prog)-8s [sub-command] --help` '
        'to get additional arguments to be used with each sub-command',
        required=False,
    )

    # Set parser args for the Image to Image app
    ImageToImage().set_app_parser(sub_parser, formatter=_chf)
    ImageToText().set_app_parser(sub_parser, formatter=_chf)
    TextToImage().set_app_parser(sub_parser, formatter=_chf)


def set_stop_parser(sp):
    pass


def set_survey_parser(sp):
    pass


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
    set_start_parser(
        sp.add_parser(
            'start',
            help='Start jina now and create or reuse a cluster.',
            description='Start jina now and create or reuse a cluster.',
            formatter_class=_chf,
        )
    )
    set_stop_parser(
        sp.add_parser(
            'stop',
            help='Stop jina now and remove local cluster.',
            description='Stop jina now and remove local cluster.',
            formatter_class=_chf,
        )
    )
    set_survey_parser(
        sp.add_parser(
            'survey',
            help='Opens a survey in the browser. Thanks for providing us feedback.',
            description='Opens a survey in the browser. Thanks for providing us feedback.',
            formatter_class=_chf,
        )
    )

    return parser


_chf = _ColoredHelpFormatter
