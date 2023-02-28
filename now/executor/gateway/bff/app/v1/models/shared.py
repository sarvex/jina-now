import base64
from time import sleep
from typing import Any, Dict, Optional

import filetype
from docarray import Document
from docarray.document.mixins.helper import _is_datauri, _to_datauri, _uri_to_blob
from pydantic import BaseModel, Field, root_validator


class BaseRequestModel(BaseModel):
    jwt: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Dictionary with key 'token' which maps to Jina Cloud token value."
        " To be passed when the flow is secure",
        example={'token': '<your jina cloud token>'},
    )
    api_key: Optional[str] = Field(
        default=None,
        description='Used to authenticate machines',
        example='<your api key>',
    )

    class Config:
        allow_mutation = False
        case_sensitive = False
        arbitrary_types_allowed = True


class ModalityModel(BaseModel):
    uri: Optional[str] = Field(
        default=None,
        description='URI of the file or data URI',
        example='https://example.com/image.jpg',
    )
    text: Optional[str] = Field(default=None, description='Text', example='cute cats')
    blob: Optional[str] = Field(
        default=None, description='Base64 encoded `utf-8` str format', example='xxx'
    )

    @root_validator(pre=True)
    def validate_only_one_exists(cls, values):
        # Get the names of all fields that are set (i.e. have a non-None value)
        set_fields = [name for name, value in values.items() if value]
        if len(set_fields) != 1:
            raise ValueError(f"Only one of {set_fields} can be set.")
        return values

    @property
    def content(self):
        for field_name in self.__fields_set__:
            field_value = getattr(self, field_name)
            if field_value:
                return field_value

    def to_html(self, title: str = '', disable_to_datauri: bool = False) -> str:
        """Converts the ModalityModel to HTML.

        :param title: Title of the figure (field name usually).
        :param disable_to_datauri: If True, the image is not converted to datauri.
        """
        if self.uri or self.blob:
            if self.uri:
                if _is_datauri(self.uri) or disable_to_datauri:
                    src = self.uri
                else:
                    # try downloading 5 times
                    src = None
                    for _ in range(5):
                        try:
                            _blob = _uri_to_blob(self.uri, timeout=10)
                            src = _to_datauri(
                                Document(uri=self.uri).mime_type,
                                _blob,
                                'utf-8',
                                False,
                                binary=True,
                            )
                            break
                        except:
                            sleep(1)
                            continue
                    if src is None:
                        src = self.uri
            elif self.blob:
                base64_decoded = base64.b64decode(self.blob.encode('utf-8'))
                file_ending = filetype.guess(base64_decoded)
                if not file_ending:
                    raise ValueError(
                        f'Could not guess file type of blob {self.blob}. '
                        f'Please provide a valid file type.'
                    )
                src = f'data:{file_ending.mime}/{file_ending.extension};base64,{self.blob}'
            html = f'<img src="{src}" alt="missing" style="max-width: 100px; max-height: 100px;">'

            if title:
                html = (
                    '<figure>' + html + f'<figcaption>({title})</figcaption></figure>'
                )
        elif self.text is not None:
            html = f'<p><b>{self.text}</b>' + (f' ({title})' if title else '') + '</p>'
        return html
