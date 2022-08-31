from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic.dataclasses import dataclass as pydantic_dataclass

from now.constants import Apps, DatasetTypes, Qualities


@pydantic_dataclass
class TrainDataGeneratorConfig:
    method: str
    parameters: Dict[str, Any]
    scope: List[str]


@pydantic_dataclass
class TrainDataGenerationConfig:
    query: TrainDataGeneratorConfig
    target: TrainDataGeneratorConfig


@pydantic_dataclass
class EncoderConfig:
    name: str
    encoder_type: str
    target_fields: List[str]
    train_dataset_name: str
    training_data_generation_methods: List[TrainDataGenerationConfig]


@pydantic_dataclass
class Task:
    name: str
    data: str
    encoders: List[EncoderConfig]
    indexer_scope: Dict[str, str]


@dataclass
class UserInput:
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
