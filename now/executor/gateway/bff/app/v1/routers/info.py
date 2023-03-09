from collections import defaultdict

from docarray import Document
from fastapi import APIRouter

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
async def get_tags(data: BaseRequestModel) -> TagsResponseModel:
    response = await jina_client_post(
        request_model=data,
        docs=Document(),
        endpoint='/tags',
        target_executor=r'\Aindexer\Z',
    )
    return TagsResponseModel(tags=response[0].tags['tags'])


@router.post('/count')
async def get_count(data: CountRequestModel) -> CountResponseModel:
    response = await jina_client_post(
        request_model=data,
        docs=Document(),
        endpoint='/count',
        target_executor=r'\Aindexer\Z',
        parameters={'limit': data.limit},
    )
    return CountResponseModel(number_of_docs=response[0].tags['count'])


@router.post('/field_names_to_dataclass_fields')
async def get_field_names_to_dataclass_fields() -> FieldNamesToDataclassFieldsResponseModel:
    return FieldNamesToDataclassFieldsResponseModel(
        field_names_to_dataclass_fields=user_input_in_bff.field_names_to_dataclass_fields
    )


@router.post('/encoder_to_dataclass_fields_mods')
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
