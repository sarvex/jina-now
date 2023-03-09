from typing import TypeVar

import docarray


def docarray_typing_to_modality_string(T: TypeVar) -> str:
    """E.g. docarray.typing.Image -> image"""
    return T.__name__.lower()


def modality_string_to_docarray_typing(s: str) -> TypeVar:
    """E.g. image -> docarray.typing.Image"""
    return getattr(docarray.typing, s.capitalize())


def get_chunk_by_field_name(doc, field_name):
    """
    Gets a specific chunk by field name, using its position instead of getting the attribute directly.
    This solves the getattr problem when there are conflicting attributes with Document.
    :param doc: Document to get the chunk from.
    :param field_name: Field needed to extract the position.
    :return: Specific chunk by field.
    """
    try:
        field_position = int(
            doc._metadata['multi_modal_schema'][field_name]['position']
        )
        return doc.chunks[field_position]
    except Exception as e:
        raise e
