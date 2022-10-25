from typing import List

from aioice.utils import random_string
from pydantic import BaseModel, create_model

from deployment.bff.app.v1.models.base import (
    BaseIndexRequestModel,
    BaseIndexResponseModel,
    BaseRequestModel,
    BaseResponseModel,
    BaseSearchRequestModel,
    BaseSearchResponseModel,
    NowBaseModel,
    TagsMixin,
    UriMixin,
)


def get_parent_model(is_request, endpoint_name):
    """Select base model based on endpoint and request/response type"""
    if endpoint_name == 'index':
        return BaseIndexRequestModel if is_request else BaseIndexResponseModel
    elif endpoint_name == 'search':
        return BaseSearchRequestModel if is_request else BaseSearchResponseModel
    else:
        raise ValueError(f'Endpoint {endpoint_name} not supported')


def get_tag_mixin(is_request, endpoint_name):
    if (endpoint_name == 'index' and is_request) or (
        endpoint_name == 'search' and not is_request
    ):
        return TagsMixin
    return BaseModel


def get_modality_mixin(input_modality):
    modality_specific_fields = {
        input_modality: (str, ...),
    }
    TmpModel = create_model(
        __model_name=random_string(10),
        __base__=BaseModel,
        **modality_specific_fields,
    )
    return TmpModel


def extend_as_list_if_necessary(model, is_request, endpoint_name):
    """Wraps the result in a list in case of search responses and index requests"""
    if (endpoint_name == 'index' and is_request) or (
        endpoint_name == 'search' and not is_request
    ):

        TmpModel = create_model(
            __model_name=random_string(10),
            __base__=NowBaseModel,
            __root__=(List[model], ...),
        )

    else:
        TmpModel = model
    return TmpModel


def get_uri_mixin(is_request, endpoint_name):
    if (endpoint_name == 'index' and is_request) or (endpoint_name == 'search'):
        return UriMixin
    return BaseModel


def combine_mixins(*mixins):
    """Combine mixins to a single model"""
    unique_mixins = set(mixin for mixin in mixins if mixin is not BaseModel)

    TmpModel = create_model(
        __model_name=random_string(10), __base__=tuple(unique_mixins)
    )

    return TmpModel


def create_final_model(parent, is_request, endpoint_name):
    """In case of search responses, the final model is has the parent model as root. In all other cases, the final model inherits from the parent model"""
    model_name = f'{str(endpoint_name).capitalize()}{"Request" if is_request else "Response"}Model'
    if endpoint_name == 'search' and not is_request:
        FinalModel = create_model(
            __model_name=model_name,
            __root__=(parent, ...),
        )
    else:
        FinalModel = create_model(
            __model_name=model_name,
            __base__=BaseRequestModel if is_request else BaseResponseModel,
            image_list=(parent, ...),
        )
    return FinalModel


def get_pydantic_model(input_modality, is_request, endpoint_name):
    parent = get_parent_model(is_request, endpoint_name)
    tag_mixin = get_tag_mixin(is_request, endpoint_name)
    modality_mixin = get_modality_mixin(input_modality)
    uri_mixin = get_uri_mixin(is_request, endpoint_name)
    model = combine_mixins(parent, tag_mixin, modality_mixin, uri_mixin)
    model = extend_as_list_if_necessary(model, is_request, endpoint_name)
    model = create_final_model(model, is_request, endpoint_name)
    return model
