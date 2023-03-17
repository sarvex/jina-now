import base64
import logging
import os
from typing import Any, Dict, List

from docarray import Document
from fastapi import APIRouter, Body

from now.data_loading.create_dataclass import create_dataclass
from now.executor.gateway.bff.app.settings import user_input_in_bff
from now.executor.gateway.bff.app.v1.models.search import (
    SearchRequestModel,
    SearchResponseModel,
    SuggestionRequestModel,
)
from now.executor.gateway.bff.app.v1.routers.helper import (
    field_dict_to_mm_doc,
    jina_client_post,
)
from now.executor.gateway.hubble_report import report_search_usage
from now.utils.docarray.helpers import (
    get_chunk_by_field_name,
    modality_string_to_docarray_typing,
)

logger = logging.getLogger(__file__)
logger.setLevel(os.environ.get('JINA_LOG_LEVEL', 'INFO'))

search_examples = {
    'working_text': {
        'summary': 'A working example: search with text',
        'description': 'A working example which can be tried out. Search with text on the best artworks dataset.',
        'value': {
            'limit': 10,
            'query': [
                {
                    'name': 'query_text_0',
                    'modality': 'text',
                    'value': 'cute cats',
                }
            ],
            'create_temp_link': False,
            'get_score_breakdown': True,
        },
    },
    'working_text_image': {
        'summary': 'A working example: search with text and image',
        'description': 'A working example which can be tried out. Search with text and image on the best artworks dataset.',
        'value': {
            'limit': 10,
            'query': [
                {
                    'name': 'query_text_0',
                    'modality': 'text',
                    'value': 'cute cats',
                },
                {
                    'name': 'query_image_0',
                    'modality': 'image',
                    'value': 'https://images.pexels.com/photos/45201/kitty-cat-kitten-pet-45201.jpeg',
                },
            ],
            'create_temp_link': False,
            'get_score_breakdown': True,
        },
    },
    'dummy': {
        'summary': 'A dummy example',
        'description': 'A dummy example,  do not run. For parameter reference only.',
        'value': {
            'limit': 10,
            'filters': {
                'color': ['blue', 'red'],
                'price': {'lte': 100, 'gte': 50},
            },
            'query': [
                {
                    'name': 'query_text_0',
                    'modality': 'text',
                    'value': 'cute cats',
                }
            ],
            'create_temp_link': False,
            'score_calculation': [['query_text_0', 'title', 'encoderclip', 1.0]],
        },
    },
}

suggestion_examples = {
    'working_text': {
        'summary': 'A working example: get suggestions for a text query',
        'description': 'A working example which can be tried out. Get autocomplete suggestions for a text query.',
        'value': {
            'text': 'cute ca',
        },
    },
    'dummy': {
        'summary': 'A dummy example',
        'description': 'A dummy example,  do not run. For parameter reference only.',
        'value': {
            'jwt': {'token': '<your token>'},
            'api_key': '<your api key>',
            'text': 'cute cats',
        },
    },
}

router = APIRouter()


@router.post(
    '/search',
    response_model=List[SearchResponseModel],
    summary='Search data via query',
)
async def search(
    data: SearchRequestModel = Body(examples=search_examples),
):
    logger.info(f'Got search request: {data}')
    fields_modalities_mapping = {}
    fields_values_mapping = {}
    if len(data.query) == 0:
        raise ValueError('Query cannot be empty')

    for field in data.query:
        fields_modalities_mapping[field['name']] = modality_string_to_docarray_typing(
            field['modality']
        )
        fields_values_mapping[field['name']] = field['value']
    data_class, field_names_to_dataclass_fields = create_dataclass(
        fields=list(fields_modalities_mapping.keys()),
        fields_modalities=fields_modalities_mapping,
    )
    query_doc = field_dict_to_mm_doc(
        fields_values_mapping,
        data_class=data_class,
        modalities_dict=fields_modalities_mapping,
        field_names_to_dataclass_fields=field_names_to_dataclass_fields,
    )
    score_calculation = get_score_calculation(data, field_names_to_dataclass_fields)

    query_filter = {}
    for key, value in data.filters.items():
        key = 'tags__' + key
        query_filter[key] = value

    docs = await jina_client_post(
        endpoint='/search',
        docs=query_doc,
        parameters={
            'limit': data.limit,
            'filter': query_filter,
            'create_temp_link': data.create_temp_link,
            'score_calculation': score_calculation,
            'get_score_breakdown': data.get_score_breakdown,
        },
        request_model=data,
    )
    matches = []
    for doc in docs[0].matches:
        # todo: use multimodal doc in the future!
        scores = {}
        for score_name, named_score in doc.scores.items():
            scores[score_name] = named_score.to_dict()
        # since multimodal doc is not supported, we take the first chunk
        if doc.chunks:
            field_names_and_chunks = [
                [field_name, get_chunk_by_field_name(doc, field_name)]
                for field_name in doc._metadata['multi_modal_schema'].keys()
            ]
        else:
            # TODO remove else path. It is only used to support the inmemory indexer since that one is operating on chunks while elastic responds with root documents
            field_names_and_chunks = [['result_field', doc]]
        results = {}
        for field_name, chunk in field_names_and_chunks:
            if chunk.blob:
                result = {'blob': base64.b64encode(chunk.blob).decode('utf-8')}
            elif chunk.text:
                result = {'text': chunk.text}
            elif chunk.uri:
                # in case we have content and uri, the content is preferred
                result = {'uri': chunk.uri}
            else:
                # We should not raise exception else it breaks the playground if a single chunk has no content
                # irrespective of what other chunks hold. We should just log it and move on.
                logger.info('Result without content', doc.id, doc.tags)
                result = {'text': ''}
            results[field_name] = result
        match = SearchResponseModel(
            id=doc.id,
            scores=scores,
            tags=doc.tags,
            fields=results,
        )
        matches.append(match)
    # reporting the usage at the end to make sure the request was successful

    logger.info(
        f'Reporting search usage after successful search request for user {data.jwt.get("token")}'
    )
    report_search_usage(user_token=data.jwt.get('token'))
    return matches


def get_score_calculation(
    data: SearchRequestModel, field_names_to_dataclass_fields: Dict[str, str]
) -> List[List[Any]]:
    """
    Extract and process the score calculation from the request model to the format expected by the indexer.
    This includes converting the field names to the dataclass field names, for the query and for the index fields.

    :param data: the request model
    :param field_names_to_dataclass_fields: a mapping from the field names in the request model to the field names in the dataclass
    :return: the score calculation in the format expected by the indexer. Example:
        [['query_text', 'my_product_image', 'encoderclip', 1], ['query_text', 'my_product_text', 'bm25', 1]]
    """
    score_calculation = []
    for scr_calc in data.score_calculation:
        scr_calc[0] = field_names_to_dataclass_fields[scr_calc[0]]
        try:
            scr_calc[1] = user_input_in_bff.field_names_to_dataclass_fields[scr_calc[1]]
        except KeyError:
            raise KeyError(
                f'Field {scr_calc[1]} not found in dataclass. Please select possible values: {user_input_in_bff.field_names_to_dataclass_fields.keys()}'
            )
        score_calculation.append(scr_calc)
    return score_calculation


@router.post(
    '/suggestion',
    summary='Get auto complete suggestion for query',
)
async def suggestion(data: SuggestionRequestModel = Body(examples=suggestion_examples)):
    suggest_doc = Document(text=data.text)
    docs = await jina_client_post(
        endpoint='/suggestion',
        docs=suggest_doc,
        request_model=data,
        target_executor=r'\Aautocomplete_executor\Z',
    )
    return docs.to_dict()
