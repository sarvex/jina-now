from collections import defaultdict
from typing import Dict, Hashable, List, TypeVar

from docarray import Document, dataclass, field
from docarray.typing import Image, Text, Video

from now.constants import AVAILABLE_MODALITIES_FOR_SEARCH, DatasetTypes
from now.now_dataclasses import UserInput
from now.utils import docarray_typing_to_modality_string


def update_dict_with_no_overwrite(dict1: Dict, dict2: Dict):
    """
    Update dict1 with dict2, but only if the key does not exist in dict1.

    :param dict1: dict to be updated
    :param dict2: dict to be used for updating
    """
    for key, value in dict2.items():
        if key not in dict1:
            dict1[key] = value


def create_dataclass(
    fields: List = None,
    fields_modalities: Dict = None,
    dataset_type: DatasetTypes = None,
    user_input: UserInput = None,
):
    """
    Create a dataclass from the selected index fields
    and their corresponding modalities or directly from the user input which should
    contain that information. If both are provided, the user input will be used.

    for example:
    the index fields modalities can be:
    {'test.txt': Text , 'image.png': Image}

    the dataclass will be:

    @dataclass
    class DataClass:
        text_0: Text
        image_0: Image
        price: float
        description: str

    :param fields: list of fields
    :param fields_modalities: dict of fields and their modalities
    :param dataset_type: dataset type
    :param user_input: user inputs

    :return: dataclass object
    """

    if user_input:
        fields_modalities = user_input.index_field_candidates_to_modalities
        dataset_type = user_input.dataset_type
        fields = user_input.index_fields

    field_names_to_dataclass_fields = create_dataclass_fields_file_mappings(
        fields,
        fields_modalities,
    )
    (all_annotations, all_class_attributes,) = create_annotations_and_class_attributes(
        fields,
        fields_modalities,
        field_names_to_dataclass_fields,
        dataset_type,
    )
    mm_doc = type("MMDoc", (object,), all_class_attributes)
    setattr(mm_doc, '__annotations__', all_annotations)
    mm_doc = dataclass(mm_doc)

    return mm_doc, field_names_to_dataclass_fields


def create_annotations_and_class_attributes(
    fields: List,
    fields_modalities: Dict[str, TypeVar],
    field_names_to_dataclass_fields: Dict,
    dataset_type: DatasetTypes = None,
):
    """
    Create annotations and class attributes for the dataclass
    In case of S3 bucket, new field is created to prevent uri loading


    :param fields: list of fields
    :param fields_modalities: dict of fields and their modalities
    :param field_names_to_dataclass_fields: dict of selected field names and their corresponding fields in dataclass
    :param dataset_type: dataset type
    """
    annotations = {}
    class_attributes = {}
    ImageType, image_setter, image_getter = create_blob_type('Image')
    VideoType, video_setter, video_getter = create_blob_type('Video')
    LocalTextType, local_text_setter, local_text_getter = create_local_text_type()

    for f in fields:
        if not isinstance(f, Hashable):
            continue
        if dataset_type == DatasetTypes.S3_BUCKET:
            S3Type, s3_setter, s3_getter = create_s3_type(str(fields_modalities[f])[1:])
            annotations[field_names_to_dataclass_fields[f]] = S3Type
            class_attributes[field_names_to_dataclass_fields[f]] = field(
                setter=s3_setter, getter=s3_getter, default=''
            )
        else:
            annotations[field_names_to_dataclass_fields[f]] = fields_modalities[f]
            if fields_modalities[f] == Image:
                class_attributes[field_names_to_dataclass_fields[f]] = field(
                    setter=image_setter, getter=image_getter, default=''
                )
            elif fields_modalities[f] == Video:
                class_attributes[field_names_to_dataclass_fields[f]] = field(
                    setter=video_setter, getter=video_getter, default=''
                )
            elif fields_modalities[f] == Text and dataset_type == DatasetTypes.PATH:
                class_attributes[field_names_to_dataclass_fields[f]] = field(
                    setter=local_text_setter, getter=local_text_getter, default=''
                )
            else:
                class_attributes[field_names_to_dataclass_fields[f]] = None
    return annotations, class_attributes


def create_s3_type(modality: str):
    """Create a new type for S3 bucket which sets the right modality"""
    S3Object = TypeVar('S3Object', bound=str)

    def my_setter(value) -> 'Document':
        """
        Custom setter for the S3Object type that doesn't load the content from the URI
        """
        doc = Document(uri=value)
        doc.modality = modality.lower()
        return doc

    def my_getter(doc: 'Document'):
        return doc.uri

    return S3Object, my_setter, my_getter


def create_local_text_type():
    """Create a new type for local text which sets the right modality and loads from URI"""
    TextObject = TypeVar('Text', bound=str)

    def my_setter(value) -> 'Document':
        """
        Custom setter for the TextObject type that loads the content from the URI
        """
        doc = Document(uri=value)
        doc.modality = 'text'
        doc.load_uri_to_text()
        return doc

    def my_getter(doc: 'Document'):
        return doc.uri

    return TextObject, my_setter, my_getter


def create_blob_type(modality: str):
    """Creates a new type which loads into blob instead of tensor"""
    BlobObject = TypeVar(modality, bound=str)

    def my_setter(value) -> 'Document':
        """Custom setter for the BlobObject type that loads the content from the URI"""
        doc = Document(uri=value).load_uri_to_blob(timeout=10)
        doc.modality = modality.lower()
        return doc

    def my_getter(doc: 'Document'):
        return doc.uri

    return BlobObject, my_setter, my_getter


def create_dataclass_fields_file_mappings(fields: List, fields_modalities: Dict):
    """
    Create a mapping between the dataclass fields and the file fields

    :param fields: list of fields
    :param fields_modalities: dict of fields and their modalities
    """

    modalities_count = defaultdict(int)

    file_mapping_to_dataclass_fields = {}
    for f in fields:
        if not isinstance(f, Hashable):
            continue
        field_modality = fields_modalities[f]
        if field_modality in AVAILABLE_MODALITIES_FOR_SEARCH:
            file_mapping_to_dataclass_fields[
                f
            ] = f'{docarray_typing_to_modality_string(field_modality)}_{modalities_count[field_modality]}'
            modalities_count[fields_modalities[f]] += 1
    return file_mapping_to_dataclass_fields
