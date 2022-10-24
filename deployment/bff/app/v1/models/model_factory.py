from typing import List, Optional

from pydantic import create_model

from deployment.bff.app.v1.models.base import (
    BaseIndexRequestModel,
    BaseIndexResponseModel,
    BaseSearchRequestModel,
    BaseSearchResponseModel,
)


def create_model_x(**kwargs):
    print(kwargs)
    return create_model(**kwargs)


def get_parent_model(is_request, endpoint_name):
    """Select base model based on endpoint and request/response type"""
    if endpoint_name == 'index':
        return BaseIndexRequestModel if is_request else BaseIndexResponseModel
    elif endpoint_name == 'search':
        return BaseSearchRequestModel if is_request else BaseSearchResponseModel
    else:
        raise ValueError(f'Endpoint {endpoint_name} not supported')


def get_modality_specific_fields(input_modality, is_request, endpoint_name):
    parent_model = get_parent_model(is_request, endpoint_name)

    if endpoint_name == 'index':
        if is_request:
            return {f'{input_modality}_list': (List[str], ...)}
        else:
            return {}
    elif endpoint_name == 'search':
        if is_request:
            return {f'{input_modality}': (str, ...)}
        else:
            return {f'{input_modality}_list': (List[str], ...)}

    modality_type = Optional[List[str]] if endpoint_name == 'index' else Optional[str]
    return parent_model, {
        input_modality: (modality_type, ...),
    }


def get_pydantic_model(input_modality, is_request, endpoint_name):
    parent_model, modality_specific_fields = get_modality_specific_fields(
        input_modality, is_request, endpoint_name
    )

    CustomModel = create_model_x(
        __model_name=f'{str(endpoint_name).capitalize()}{"Request" if is_request else "Response"}Model',
        __base__=parent_model,
        **modality_specific_fields,
    )

    return CustomModel


# def create_model_x(**kwargs):
#     print(kwargs)
#     return create_model(**kwargs)
#
#
# def get_pydantic_model(input_modality, is_request, endpoint_name):
#     base_model = BaseRequestModel if is_request else NowBaseModel
#     modality_type = Optional[List[str]] if endpoint_name == 'index' else Optional[str]
#     custom_model = create_model_x(
#         __model_name=f'{str(input_modality).capitalize()}{"Request" if is_request else "Response"}Model',
#         **{
#             input_modality: modality_type,
#         },
#         **({'uri': str} if input_modality != 'text' else {}),
#     )
#
#     class ResultModel(base_model, custom_model):
#         pass
#
#     return ResultModel
