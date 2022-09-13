"""
This module implements a command-line dialog with the user.
Its goal is to configure a UserInput object with users specifications.
Optionally, values can be passed from the command-line when jina-now is launched. In that case,
the dialog won't ask for the value.
"""
from __future__ import annotations, print_function, unicode_literals

import inspect
import pathlib

from now_common import options

import now.utils
from now.apps.base.app import JinaNOWApp
from now.now_dataclasses import DialogOptions, UserInput

cur_dir = pathlib.Path(__file__).parent.resolve()


def configure_app(**kwargs) -> JinaNOWApp:
    now.utils.print_headline()
    app_name = configure_option(options.APP, None, **kwargs)
    return options._construct_app(app_name)


def configure_user_input(app_instance: JinaNOWApp, **kwargs) -> UserInput:
    user_input = UserInput()
    user_input.app_instance = app_instance
    # Ask the base/common options
    for option in options.base_options:
        configure_option(option, user_input, **kwargs)
    # Ask app specific options
    for option in user_input.app_instance.options:
        configure_option(option, user_input, **kwargs)

    return user_input


def configure_option(option: DialogOptions, user_input: UserInput, **kwargs):
    # If there is any pre function then invoke that - commonly used to fill choices if needed
    # if inspect.isfunction(option.pre_func):
    #     option.post_func(user_input, option, **kwargs)

    # Check if it is dependent on some other dialog options
    if option.depends_on and not option.conditional_check(user_input):
        return

    # Populate choices if needed
    if option.choices and inspect.isfunction(option.choices):
        option.choices = option.choices(user_input, **kwargs)

    val = now.utils._prompt_value(
        **option.__dict__,
        **kwargs,
    )

    if hasattr(user_input, option.name):
        setattr(user_input, option.name, val)

    # If there is any post function then invoke that
    kwargs[option.name] = val
    if inspect.isfunction(option.post_func):
        option.post_func(user_input, **kwargs)

    return val
