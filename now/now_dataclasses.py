"""
This module implements a command-line dialog with the user.
Its goal is to configure a UserInput object with users specifications.
Optionally, values can be passed from the command-line when jina-now is launched. In that case,
the dialog won't ask for the value.
"""
from __future__ import annotations, print_function, unicode_literals

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from now.constants import Apps, DatasetTypes


@dataclass
class UserInput:
    app: Optional[Apps] = None
    app_instance: Optional['JinaNOWApp'] = None  # noqa: F821

    # data related
    data: Optional[str] = None
    custom_dataset_type: Optional[DatasetTypes] = None
    dataset_name: Optional[str] = None
    dataset_url: Optional[str] = None
    dataset_path: Optional[str] = None

    # AWS related
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region_name: Optional[str] = None

    # cluster related
    cluster: Optional[str] = None
    deployment_type: Optional[str] = None
    secured: Optional[bool] = None
    jwt: Optional[Dict[str, str]] = None
    admin_emails: Optional[List[str]] = None
    user_emails: Optional[List[str]] = None
    additional_user: Optional[bool] = None


@dataclass
class DialogOptions:
    name: str
    prompt_message: str
    prompt_type: str
    choices: Union[
        List[Dict[str, Union[str, bool]]],
        Callable[[Any], List[Dict[str, str]]],
    ] = None
    is_terminal_command: bool = False  # set when this dialog is required as a cli param
    description: str = None  # Description to show on terminal when used as a cli param
    depends_on: Optional['DialogOptions'] = None
    conditional_check: Callable[[Any], bool] = None
    post_func: Callable[[Any], None] = None
