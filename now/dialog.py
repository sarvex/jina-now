"""
This module implements a command-line dialog with the user.
Its goal is to configure a UserInput object with users specifications.
Optionally, values can be passed from the command-line when jina-now is launched. In that case,
the dialog won't ask for the value.
"""
from __future__ import annotations, print_function, unicode_literals

import inspect
import pathlib

import now.utils
from now.common import options
from now.common.options import construct_app
from now.constants import MODALITY_TO_MODELS, Apps, DialogStatus
from now.now_dataclasses import DialogOptions, UserInput
from now.utils import DemoAvailableException, RetryException

cur_dir = pathlib.Path(__file__).parent.resolve()


def configure_user_input(**kwargs) -> UserInput:
    user_input = UserInput()
    now.utils.print_headline()
    # Create the search app.
    # TODO: refactor this when more apps are added
    user_input.app_instance = construct_app(Apps.SEARCH_APP)
    # Ask the options
    for option in options.base_options + user_input.app_instance.options:
        if configure_option(option, user_input, **kwargs) == DialogStatus.BREAK:
            break

    return user_input


def configure_option(
    option: DialogOptions, user_input: UserInput, **kwargs
) -> DialogStatus:
    # Check if it is dynamic. If it is then spawn multiple dialogs for each option and return with continue
    if option.dynamic_func:
        if option.name in kwargs:
            # Expand dynamic options from parent option, expect a dict (supports only model_selection)
            for user_selection in kwargs[option.name].split(","):
                if ":" in user_selection:
                    option_name, option_values = user_selection.split(":")
                    kwargs[f"{option_name}_model"] = []
                    if (
                        not option_name
                        in user_input.index_field_candidates_to_modalities
                    ):
                        raise ValueError(
                            f"Error with --{option.name}: `{option_name}` is not an index field."
                        )
                    for option_value in option_values.split("+"):
                        model_selection = [
                            model
                            for model in MODALITY_TO_MODELS[
                                user_input.index_field_candidates_to_modalities[
                                    option_name
                                ]
                            ]
                            if model["name"] == option_value
                        ]
                        if model_selection:
                            kwargs[f"{option_name}_model"].append(
                                model_selection[0]["value"]
                            )
                        else:
                            model_choices = [
                                model["name"]
                                for model in MODALITY_TO_MODELS[
                                    user_input.index_field_candidates_to_modalities[
                                        option_name
                                    ]
                                ]
                            ]
                            raise ValueError(
                                f"Error with --{option.name}: `{option_value}` is not available. "
                                f"for index field `{option_name}`. Choices are: {','. join(model_choices)}."
                            )

        for result in option.dynamic_func(user_input):
            configure_option(result, user_input, **kwargs)
        return DialogStatus.CONTINUE

    # Check if it is dependent on some other dialog options
    if option.depends_on and not option.conditional_check(user_input):
        return DialogStatus.SKIP

    # Populate choices if needed
    if option.choices and inspect.isfunction(option.choices):
        option.choices = option.choices(user_input, **kwargs)

    while True:
        val = now.utils.prompt_value(
            **option.__dict__,
            **kwargs,
        )

        if val:
            kwargs[option.name] = val
            if hasattr(user_input, option.name):
                setattr(user_input, option.name, val)

        try:
            # If there is any post function then invoke that
            if inspect.isfunction(option.post_func):
                option.post_func(user_input, **kwargs)
        except RetryException as e:
            print(e)
            continue
        except DemoAvailableException:
            return DialogStatus.BREAK
        break

    return DialogStatus.CONTINUE
