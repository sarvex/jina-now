"""
This module implements a command-line dialog with the user.
Its goal is to configure a UserInput object with users specifications.
Optionally, values can be passed from the command-line when jina-now is launched. In that case,
the dialog won't ask for the value.
"""
from __future__ import annotations, print_function, unicode_literals

import inspect
import pathlib

from now_common.options import UserInput

from now.apps.base.app import JinaNOWApp
from now.utils import _prompt_value, print_headline

cur_dir = pathlib.Path(__file__).parent.resolve()

AVAILABLE_SOON = 'will be available in upcoming versions'


def configure_user_input(**kwargs) -> [JinaNOWApp, UserInput]:
    print_headline()

    user_input = UserInput()
    _configure_app_options(user_input, **kwargs)

    return user_input.app_instance, user_input


def _configure_app_options(user_input, **kwargs):
    # First we configure the base app options by calling it on the base class
    for option in JinaNOWApp.options():
        configure_option(option, user_input, **kwargs)

    # Now we call app specific options
    for option in user_input.app_instance.options():
        configure_option(option, user_input, **kwargs)


def configure_option(option, user_input, **kwargs):
    # check if it is dependent on some other dialog options
    if option.depends_on:
        # # ii) Check if the parent option is set first else raise Exception
        # if getattr(user_input, option.depends_on.name) is None:
        #     raise ValueError(
        #         f'The `{option.name}` option depends on `{option.depends_on.name}` option but it is not set. Please '
        #         f'either change the order of dialog options or remove dependency'
        #     )
        # iii) Check if it should be triggered for the selected value of the parent
        if option.trigger_option_value != getattr(user_input, option.depends_on.name):
            return

    # 1) Populate choices if needed
    if option.choices and inspect.isfunction(option.choices):
        option.choices = option.choices(user_input, **kwargs)

    val = _prompt_value(
        **option.__dict__,
        **kwargs,
    )
    setattr(user_input, option.name, val)

    # If there is any post function then invoke that
    if inspect.isfunction(option.post_func):
        option.post_func(user_input, **kwargs)
