from collections import defaultdict

from docarray import Document
from fastapi import APIRouter, Request

from now.executor.gateway.bff.app.settings import user_input_in_bff
from now.executor.gateway.bff.app.v1.models.info import (
    CountRequestModel,
    CountResponseModel,
    EncoderToDataclassFieldsModsResponseModel,
    FieldNamesToDataclassFieldsResponseModel,
    TagsResponseModel,
)
from now.executor.gateway.bff.app.v1.models.shared import BaseRequestModel
from now.executor.gateway.bff.app.v1.routers.helper import jina_client_post

router = APIRouter()


@router.post('/tags')
def get_tags(request: Request, data: BaseRequestModel) -> TagsResponseModel:
    auth_token = None
    if request.headers.get('Authorization'):
        auth_token = request.headers.get('Authorization').replace('token ', '')
    # if jwt not set in data, use the one from header
    if not data.jwt and auth_token:
        data.jwt['token'] = auth_token
    response = jina_client_post(
        request_model=data,
        docs=Document(),
        endpoint='/tags',
        target_executor=r'\Aindexer\Z',
    )
    return TagsResponseModel(tags=response[0].tags['tags'])


@router.post('/count')
def get_count(request: Request, data: CountRequestModel) -> CountResponseModel:
    auth_token = None
    if request.headers.get('Authorization'):
        auth_token = request.headers.get('Authorization').replace('token ', '')
    # if jwt not set in data, use the one from header
    if not data.jwt and auth_token:
        data.jwt['token'] = auth_token
    response = jina_client_post(
        request_model=data,
        docs=Document(),
        endpoint='/count',
        target_executor=r'\Aindexer\Z',
        parameters={'limit': data.limit},
    )
    return CountResponseModel(number_of_docs=response[0].tags['count'])


@router.post('/field_names_to_dataclass_fields')
def get_field_names_to_dataclass_fields() -> FieldNamesToDataclassFieldsResponseModel:
    return FieldNamesToDataclassFieldsResponseModel(
        field_names_to_dataclass_fields=user_input_in_bff.field_names_to_dataclass_fields
    )


@router.post('/encoder_to_dataclass_fields_mods')
def get_index_fields_dict() -> EncoderToDataclassFieldsModsResponseModel:
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
