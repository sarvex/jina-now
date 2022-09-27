"""
This module implements a command-line dialog with the user.
Its goal is to configure a UserInput object with users specifications.
Optionally, values can be passed from the command-line when jina-now is launched. In that case,
the dialog won't ask for the value.
"""
from __future__ import annotations, print_function, unicode_literals

import dataclasses
from typing import Any, Callable, Dict, List, Optional, Union

from pydantic import BaseModel, StrictBool
from pydantic.dataclasses import dataclass

from now.constants import DatasetTypes


@dataclass
class TrainDataGeneratorConfig:
    """
    Configuration of a specific data generation method for an encoder model.

    Fields
    ------
    method : Method to generate "artificial queries".
    parameters : Parameters to pass to the training data generation method.
    scope : Fields to apply the method on.
    """

    method: str
    parameters: Dict[str, Any]
    scope: List[str]


@dataclass
class TrainDataGenerationConfig:
    """
    Configuration of the training data generation for a bi-encoder
    which encodes queries and targets.

    Fields
    ------
    query : Method to generate "artificial queries".
    target : Method to generate training targets and do preprocessing before
        encoding documents.
    """

    query: TrainDataGeneratorConfig
    target: TrainDataGeneratorConfig


@dataclass
class EncoderConfig:
    """
    Configuration of an encoder to encode queries and targets (text+image).

    Fields
    ------
    name : Encoder name.
    encoder_type : Datatypes which are matched by this encoder pair - in the first
        version either "text-to-text" or "text-to-image".
    train_dataset_name: Name of a dataset generated for this encoder.
    training_data_generation_methods: Configuration of methods to generate training data.
    """

    name: str
    encoder_type: str
    train_dataset_name: str
    training_data_generation_methods: List[TrainDataGenerationConfig]


@dataclass
class Task:
    """
    Task configuration for text to text+image apps.

    Fields
    ------
    name : Unique name for the task.
    encoders : Configuration of the models to encode queries and
        elastic search documents.
    indexer_scope: Fields to consider during indexing for each modality.
    """

    name: str
    encoders: List[EncoderConfig]
    indexer_scope: Dict[str, str]


class UserInput(BaseModel):
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

    # ES related
    task_config: Optional[Task] = None
    es_text_fields: Optional[List] = None
    es_image_fields: Optional[List] = None
    es_index_name: Optional[str] = None
    es_host_name: Optional[str] = None
    es_additional_args: Optional[Dict] = None

    # cluster related
    cluster: Optional[str] = None
    deployment_type: Optional[str] = None
    secured: Optional[StrictBool] = None
    jwt: Optional[Dict[str, str]] = None
    admin_emails: Optional[List[str]] = None
    user_emails: Optional[List[str]] = None
    additional_user: Optional[StrictBool] = None

    class Config:
        arbitrary_types_allowed = True


@dataclasses.dataclass
class DialogOptions:
    name: str
    prompt_message: str
    prompt_type: str
    choices: Union[
        List[Dict[str, Union[str, bool]]],
        Callable[[Any], List[Dict[str, str]]],
    ] = None
    is_terminal_command: StrictBool = (
        False  # set when this dialog is required as a cli param
    )
    description: str = None  # Description to show on terminal when used as a cli param
    depends_on: Optional['DialogOptions'] = None
    conditional_check: Callable[[Any], bool] = None
    post_func: Callable[[Any], None] = None
