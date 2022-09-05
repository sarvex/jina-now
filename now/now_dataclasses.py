from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from now.constants import Apps, DatasetTypes, Qualities


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
    target_fields : Properties of an elastic search document encoded by this encoder.
    train_dataset_name: Name of a dataset generated for this encoder.
    training_data_generation_methods: Configuration of methods to generate training data.
    """

    name: str
    encoder_type: str
    target_fields: List[str]
    train_dataset_name: str
    training_data_generation_methods: List[TrainDataGenerationConfig]


@dataclass
class Task:
    """
    Task configuration for text to text+image apps.

    Fields
    ------
    name : Unique name for the task.
    data : Name of the dataset.
    encoders : Configuration of the models to encode queries and
        elastic search documents.
    indexer_scope: Fields to consider during indexing for each modality.
    """

    name: str
    data: str
    encoders: List[EncoderConfig]
    indexer_scope: Dict[str, str]


class UserInput(BaseModel):
    app: Optional[Apps] = None

    # data related
    data: Optional[str] = None
    is_custom_dataset: Optional[bool] = None

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

    # model related
    quality: Optional[Qualities] = Qualities.MEDIUM

    # cluster related
    cluster: Optional[str] = None
    create_new_cluster: Optional[bool] = False
    deployment_type: Optional[str] = None
    secured: Optional[bool] = False
    jwt: Optional[Dict[str, str]] = None
    admin_emails: Optional[List[str]] = None
    user_emails: Optional[List[str]] = None
    additional_user: Optional[bool] = False

    class Config:
        arbitrary_types_allowed = True
