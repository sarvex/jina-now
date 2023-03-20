from collections import defaultdict

from docarray import Document
from fastapi import APIRouter, Body

from now.executor.gateway.bff.app.settings import user_input_in_bff
from now.executor.gateway.bff.app.v1.models.info import (
    CountRequestModel,
    CountResponseModel,
    EncoderToDataclassFieldsModsResponseModel,
    FiltersResponseModel,
)
from now.executor.gateway.bff.app.v1.models.search import SuggestionRequestModel
from now.executor.gateway.bff.app.v1.models.shared import BaseRequestModel
from now.executor.gateway.bff.app.v1.routers.helper import jina_client_post

router = APIRouter()

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


@router.post(
    '/filters', summary='Get all filters in the indexer and their possible values'
)
async def get_tags(data: BaseRequestModel) -> FiltersResponseModel:
    response = await jina_client_post(
        request_model=data,
        docs=Document(),
        endpoint='/filters',
        target_executor=r'\Aindexer\Z',
    )
    return FiltersResponseModel(filters=response[0].tags['filters'])


@router.post(
    '/count', summary='Get the count of the total number of documents in the indexer'
)
async def get_count(data: CountRequestModel) -> CountResponseModel:
    response = await jina_client_post(
        request_model=data,
        docs=Document(),
        endpoint='/count',
        target_executor=r'\Aindexer\Z',
        parameters={'limit': data.limit},
    )
    return CountResponseModel(number_of_docs=response[0].tags['count'])


@router.post('/encoder_to_dataclass_fields_mods', include_in_schema=False)
async def get_index_fields_dict() -> EncoderToDataclassFieldsModsResponseModel:
    index_fields_dict = defaultdict(dict)
    for index_field_raw, encoders in user_input_in_bff.model_choices.items():
        index_field = index_field_raw.replace('_model', '')
        dataclass_field = user_input_in_bff.field_names_to_dataclass_fields[index_field]
        modality = user_input_in_bff.index_field_candidates_to_modalities[index_field]
        for encoder in encoders:
            index_fields_dict[encoder][dataclass_field] = modality
    return EncoderToDataclassFieldsModsResponseModel(
        encoder_to_dataclass_fields_mods=index_fields_dict
    )
