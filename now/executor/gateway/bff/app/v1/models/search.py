from math import ceil, sqrt
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field

from now.executor.gateway.bff.app.v1.models.shared import (
    BaseRequestModel,
    ModalityModel,
)


class _NamedScore(BaseModel):
    value: Optional[float] = None


class IndexRequestModel(BaseRequestModel):
    data: List[Tuple[Dict[str, ModalityModel], Dict[str, Any]]] = Field(
        default=[({}, {})],
        description='List of tuples where each tuple contains a dictionary of data and a dictionary of tags. '
        'The data dictionary maps the field name to its value. ',
        example=[({'title': ModalityModel(text='this title')}, {'color': 'red'})],
    )


class SearchRequestModel(BaseRequestModel):
    limit: int = Field(
        default=10, description='Number of matching results to return', example=10
    )
    filters: Optional[Dict[str, Union[List, Dict[str, Union[int, float]]]]] = Field(
        default={},
        description='dictionary with filters for search results',
        example={'color': ['blue'], 'price': {'lt': 50.0}},
    )
    query: List[Dict] = Field(
        default={},
        description='List of dictionaries with query fields. Each dictionary represents a field in the query.',
        example=[
            {'name': 'title', 'modality': 'text', 'value': 'cute cats'},
            {
                'name': 'image',
                'modality': 'image',
                'value': 'https://example.com/image.jpg',
            },
        ],
    )
    create_temp_link: bool = Field(
        default=False,
        description='If true, a temporary link to the file is created. '
        'This is useful if the file is stored in a cloud bucket.',
        example=False,
    )
    score_calculation: List[List] = Field(
        default=[],
        description='List of lists, where each nested list contains a query_field, index_field, matching_method and weight.'
        ' This defines how scores should be calculated for documents. The matching_method can be an encoder name or '
        'bm25. The weight is a float which is used to scale the score.',
        example=[['query_text', 'title', 'encoderclip', 1.0]],
    )
    get_score_breakdown: bool = Field(
        default=False,
        description='If true, the score breakdown is returned in the response tags.',
        example=True,
    )


class SearchResponseModel(BaseModel):
    id: str = Field(
        default=...,
        nullable=False,
        description='Id of the matching result.',
        example='123',
    )
    scores: Optional[Dict[str, '_NamedScore']] = Field(
        description='Similarity score with respect to the query.',
        example={'score': {'value': 0.5}},
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
    ] = Field(
        description='Additional tags associated with the file.',
        example={'price': {'lt': 50.0}},
    )
    fields: Dict[str, ModalityModel] = Field(
        default={},
        description='Dictionary which maps the field name to its value.',
        example={
            'title': {'text': 'hello world'},
            'image': {'uri': 'https://example.com/image.jpg'},
        },
    )

    def __init__(
        self,
        id: str,
        scores: Optional[Dict[str, '_NamedScore']] = {},
        tags: Optional[
            Dict[
                str,
                Union[
                    Optional[Union[str, bool, int, float]],
                    List[Optional[Union[str, bool, int, float]]],
                    Dict[str, Optional[Union[str, bool, int, float]]],
                ],
            ]
        ] = {},
        fields: Dict[str, ModalityModel] = {},
    ) -> None:
        super().__init__(id=id, scores=scores, fields=fields)
        self.validate_tags(tags)
        self.tags = tags

    def validate_tags(self, tags):
        for key, value in tags.items():
            if isinstance(value, list):
                for item in value:
                    self.validate_tags({'': item})
            elif isinstance(value, dict):
                for _key, _value in value.items():
                    self.validate_tags({_key: _value})
            elif not isinstance(value, (str, bool, int, float)):
                raise ValueError(
                    f"Invalid type '{type(item)}' of value '{item}' for key '{key}' in tags"
                )

    def to_html(self, disable_to_datauri: bool = False) -> str:
        """Converts the SearchResponseModel to HTML. This is used to display the a single multi-modal result as HTML.

        :param disable_to_datauri: If True, the image is not converted to datauri.
        """
        # sort dictionary by keys, to have the same order in displaying elements
        single_fields_in_html = [
            mm.to_html(title, disable_to_datauri)
            for title, mm in dict(sorted(self.fields.items())).items()
        ]
        mm_in_html = ''.join(single_fields_in_html)
        return mm_in_html

    @classmethod
    def responses_to_html(
        cls, responses: List['SearchResponseModel'], disable_to_datauri: bool = False
    ) -> str:
        """Converts a list of SearchResponseModel to HTML. This is used to display the multi-modal results as HTML."""
        html_list = [r.to_html(disable_to_datauri) for r in responses]

        num_html = len(html_list)
        side_length = ceil(sqrt(num_html))
        output_html = "<div style='display: grid; grid-template-columns: repeat({0}, 1fr); grid-gap: 10px;'>".format(
            side_length
        )

        for i in range(num_html):
            output_html += (
                "<div style='border: 1px solid black; padding: 5px;'>{0}</div>".format(
                    html_list[i]
                )
            )

        output_html += "</div>"

        return output_html

    class Config:
        case_sensitive = False
        arbitrary_types_allowed = True


class SuggestionRequestModel(BaseRequestModel):
    text: Optional[str] = Field(default=None, description='Text', example='cute cats')


IndexRequestModel.update_forward_refs()
SearchRequestModel.update_forward_refs()
SearchResponseModel.update_forward_refs()
