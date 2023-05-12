# -*- coding: utf-8 -*-
import json

__version__ = '0.1.2'


def format_json(data):
    return json.dumps(data, sort_keys=True, indent=4)


def colorize_json(data):
    try:
        from pygments import formatters, highlight, lexers

        if isinstance(data, bytes):
            data = data.decode('UTF-8')
        return highlight(
            data, lexers.JsonLexer(), formatters.TerminalFormatter()
        )
    except ModuleNotFoundError:
        return data


def print_json(data):
    # colorful_json = highlight(unicode(format_json(data), 'UTF-8'),
    #                          lexers.JsonLexer(),
    #                          formatters.TerminalFormatter())
    print(colorize_json(format_json(data)))
