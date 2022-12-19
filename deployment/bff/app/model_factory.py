import random
import string
from typing import Dict, List

from pydantic import BaseModel, Field, create_model

from deployment.bff.app.models import (
    BaseRequestModel,
    BaseResponseModel,
    MultiModalModel,
    NowBaseModel,
    TagsMixin,
)


def get_tag_mixin(endpoint, is_request):
    if endpoint.has_tags(is_request):
        return TagsMixin
    return BaseModel


def get_modality_description(modality, is_request, endpoint_name):
    return (
        f'{str(modality).capitalize()} {"input" if is_request else "output"} for endpoint {endpoint_name}. '
        f'{("Base64 encoded string representing the binary of the " + modality) if modality != "text" else ""}.'
    )


def random_model_name(name):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(5)) + name


def get_modality_mixin(endpoint, modalities, is_request, endpoint_name):
    if endpoint.has_modality(is_request):
        field_name = 'fields'
        field_value = Field(
            default={},
            title=field_name,
            description='\n'.join(
                get_modality_description(modality, is_request, endpoint_name)
                for modality in modalities
            ),
        )

        class TmpModel(BaseModel):
            fields: Dict[str, MultiModalModel] = field_value

    else:
        TmpModel = BaseModel
    return TmpModel


def extend_as_list_if_necessary(endpoint, model, is_request):
    """Wraps the result in a list in case of search responses and index requests"""
    if endpoint.is_list(is_request):
        TmpModel = create_model(
            __model_name=random_model_name('_list_extended'),
            __base__=NowBaseModel,
            __root__=(List[model], ...),
        )
    else:
        TmpModel = model
    return TmpModel


def combine_mixins(*mixins):
    """Combine mixins to a single model"""
    unique_mixins = set(mixin for mixin in mixins if mixin is not BaseModel)

    TmpModel = create_model(
        __model_name=random_model_name('_combined'), __base__=tuple(unique_mixins)
    )

    return TmpModel


def create_final_model(endpoint, model, is_request):
    """In case of search responses, the final model is has the parent model as root. In all other cases, the final model inherits from the parent model"""
    model_name = f'{str(endpoint.name).capitalize()}{"Request" if is_request else "Response"}Model'
    if endpoint.is_list(is_request) and not endpoint.is_list_root(is_request):
        FinalModel = create_model(
            __model_name=model_name,
            __base__=BaseRequestModel if is_request else BaseResponseModel,
            **{'document_list': (model, ...)},
        )
    else:
        FinalModel = create_model(
            __model_name=model_name,
            __root__=(model, ...),
        )
    return FinalModel


def get_pydantic_model(endpoint, modalities, is_request):
    if is_request:
        base_model = endpoint.base_request_model()
    else:
        base_model = endpoint.base_response_model()
    if endpoint.is_list(is_request):
        base_model = type('TmpClass', (), {})
    tag_mixin = get_tag_mixin(endpoint, is_request)
    modality_mixin = get_modality_mixin(endpoint, modalities, is_request, endpoint.name)
    model = combine_mixins(base_model, tag_mixin, modality_mixin)
    model = extend_as_list_if_necessary(endpoint, model, is_request)
    model = create_final_model(endpoint, model, is_request)
    return model
