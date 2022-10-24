from typing import List, Optional

from pydantic import BaseModel, create_model

from deployment.bff.app.v1.models.base import BaseRequestModel


def get_pydantic_model(input_modality, is_request, endpoint_name):
    base_model = BaseRequestModel if is_request else BaseModel

    class Config(base_model.Config):
        fields = {
            input_modality: {
                'description': f'base64 encoded string of {input_modality} in `utf-8` format'
            },
            **(
                {'uri': {'description': 'uri of the file'}}
                if input_modality != 'text'
                else {}
            ),
        }

    modality_type = Optional[List[str]] if endpoint_name == 'index' else Optional[str]
    return create_model(
        f'{str(input_modality).capitalize()}{"Request" if is_request else "Response"}Model',
        __config__=Config,
        **{
            input_modality: modality_type,
        },
        **({'uri': str} if input_modality != 'text' else {}),
    )
