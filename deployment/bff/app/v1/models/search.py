from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, root_validator

from deployment.bff.app.v1.models.shared import BaseRequestModel, ModalityModel


class _NamedScore(BaseModel):
    value: Optional[float] = None


class IndexRequestModel(BaseRequestModel):
    data: List[Tuple[Dict[str, ModalityModel], Dict[str, Any]]] = Field(
        default=[({}, {})],
        description='List of tuples where each tuple contains a dictionary of data and a dictionary of tags. '
        'The data dictionary maps the field name to its value. ',
    )


class SearchRequestModel(BaseRequestModel):
    limit: int = Field(default=10, description='Number of matching results to return')
    filters: Optional[Dict[str, str]] = Field(
        default={},
        description='dictionary with filters for search results  {"tag_name" : "tag_value"}',
    )
    query: List[Dict] = Field(
        default={},
        description='List of dictionaries with query fields {"name": "field_name", "modality": "modality", "value": '
        '"value"}. Each dictionary represents a field in the query. Each dictionary must have a name, '
        'modality and value. The name is the name of the field in the data. The modality is the modality '
        'of the field. The value is the value of the field in the query. '
        'Example: [{"name": "title", "modality": "text", "value": "hello world"},'
        '          {"name": "image", "modality": "image", "value": "https://example.com/image.jpg"}]',
    )
    create_temp_link: bool = Field(
        default=False,
        description='If true, a temporary link to the file is created. '
        'This is useful if the file is stored in a cloud bucket.',
    )
    semantic_scores: List[Tuple] = Field(
        default=[],
        description='List of tuples where each tuple contains a query_field, index_field, encoder_name and weight.'
        ' This defines how scores should be calculated for documents.',
    )


class SearchResponseModel(BaseModel):
    id: str = Field(
        default=..., nullable=False, description='Id of the matching result.'
    )
    scores: Optional[Dict[str, '_NamedScore']] = Field(
        description='Similarity score with respect to the query.'
    )
    tags: Optional[
        Dict[
            str,
            Union[
                Optional[Union[str, bool, float]],
                List[Optional[Union[str, bool, float]]],
                Dict[str, Optional[Union[str, bool, float]]],
            ],
        ]
    ] = Field(description='Additional tags associated with the file.')
    fields: Dict[str, ModalityModel] = Field(
        default={}, description='Dictionary which maps the field name to its value. '
    )

    def __init__(
        self,
        id: str,
        scores: Optional[Dict[str, '_NamedScore']],
        tags: Optional[
            Dict[
                str,
                Union[
                    Optional[Union[str, bool, int, float]],
                    List[Optional[Union[str, bool, int, float]]],
                    Dict[str, Optional[Union[str, bool, int, float]]],
                ],
            ]
        ],
        fields: Dict[str, ModalityModel],
    ) -> None:
        super().__init__(id=id, scores=scores, fields=fields, tags=tags)
        self.tags = tags

    @root_validator(pre=True)
    def validate_tags(cls, values):
        tags = values.get('tags')
        if tags:
            for key, value in tags.items():
                if isinstance(value, list):
                    for item in value:
                        if not isinstance(item, (str, bool, int, float)):
                            raise ValueError(
                                f"Invalid type '{type(item)}' of value '{item}' for key '{key}' in tags"
                            )
                elif isinstance(value, dict):
                    for item in value.values():
                        if not isinstance(item, (str, bool, int, float)):
                            raise ValueError(
                                f"Invalid type '{type(item)}' of value '{item}' for key '{key}' in tags"
                            )
                elif not isinstance(value, (str, bool, int, float)):
                    raise ValueError(
                        f"Invalid type '{type(item)}' of value '{item}' for key '{key}' in tags"
                    )
        return values

    class Config:
        case_sensitive = False
        arbitrary_types_allowed = True


class SuggestionRequestModel(BaseRequestModel):
    text: Optional[str] = Field(default=None, description='Text')


IndexRequestModel.update_forward_refs()
SearchRequestModel.update_forward_refs()
SearchResponseModel.update_forward_refs()
