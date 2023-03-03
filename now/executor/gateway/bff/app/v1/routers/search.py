import base64
from typing import Any, Dict, List

from docarray import Document
from fastapi import APIRouter, Body

from now.data_loading.create_dataclass import create_dataclass
from now.executor.gateway.bff.app.settings import user_input_in_bff
from now.executor.gateway.bff.app.v1.models.search import (
    SearchRequestModel,
    SearchResponseModel,
    SuggestionRequestModel,
)
from now.executor.gateway.bff.app.v1.routers.helper import (
    field_dict_to_mm_doc,
    jina_client_post,
)
from now.utils import get_chunk_by_field_name, modality_string_to_docarray_typing

search_examples = {
    'working_text': {
        'summary': 'A working example: search with text',
        'description': 'A working example which can be tried out. Search with text on the best artworks dataset.',
        'value': {
            'limit': 10,
            'query': [
                {
                    'name': 'query_text_0',
                    'modality': 'text',
                    'value': 'cute cats',
                }
            ],
            'create_temp_link': False,
            'get_score_breakdown': True,
        },
    },
    'working_text_image': {
        'summary': 'A working example: search with text and image',
        'description': 'A working example which can be tried out. Search with text and image on the best artworks dataset.',
        'value': {
            'limit': 10,
            'query': [
                {
                    'name': 'query_text_0',
                    'modality': 'text',
                    'value': 'cute cats',
                },
                {
                    'name': 'query_image_0',
                    'modality': 'image',
                    'value': 'https://images.pexels.com/photos/45201/kitty-cat-kitten-pet-45201.jpeg',
                },
            ],
            'create_temp_link': False,
            'get_score_breakdown': True,
        },
    },
    'dummy': {
        'summary': 'A dummy example',
        'description': 'A dummy example,  do not run. For parameter reference only.',
        'value': {
            'limit': 10,
            'filters': {
                'tags__color': {'$eq': 'blue'},
                'tags__price': {'$lte': 100, '$gte': 50},
            },
            'query': [
                {
                    'name': 'query_text_0',
                    'modality': 'text',
                    'value': 'cute cats',
                }
            ],
            'create_temp_link': False,
            'semantic_scores': [('query_text_0', 'title', 'encoderclip', 1.0)],
        },
    },
}

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

router = APIRouter()


@router.post(
    '/search',
    response_model=List[SearchResponseModel],
    summary='Search data via query',
)
async def search(
    data: SearchRequestModel = Body(examples=search_examples),
):
    fields_modalities_mapping = {}
    fields_values_mapping = {}

    if len(data.query) == 0:
        raise ValueError('Query cannot be empty')

    for field in data.query:
        fields_modalities_mapping[field['name']] = modality_string_to_docarray_typing(
            field['modality']
        )
        fields_values_mapping[field['name']] = field['value']
    data_class, field_names_to_dataclass_fields = create_dataclass(
        fields=list(fields_modalities_mapping.keys()),
        fields_modalities=fields_modalities_mapping,
    )
    query_doc = field_dict_to_mm_doc(
        fields_values_mapping,
        data_class=data_class,
        modalities_dict=fields_modalities_mapping,
        field_names_to_dataclass_fields=field_names_to_dataclass_fields,
    )
    semantic_scores = get_semantic_scores(data, field_names_to_dataclass_fields)

    query_filter = {}
    for key, value in data.filters.items():
        key = 'tags__' + key if not key.startswith('tags__') else key
        query_filter[key] = {'$eq': value}

    docs = await jina_client_post(
        endpoint='/search',
        docs=query_doc,
        parameters={
            'limit': data.limit,
            'filter': query_filter,
            'create_temp_link': data.create_temp_link,
            'semantic_scores': semantic_scores,
            'get_score_breakdown': data.get_score_breakdown,
        },
        request_model=data,
    )
    matches = []
    for doc in docs[0].matches:
        # todo: use multimodal doc in the future!
        scores = {}
        for score_name, named_score in doc.scores.items():
            scores[score_name] = named_score.to_dict()
        # since multimodal doc is not supported, we take the first chunk
        if doc.chunks:
            field_names_and_chunks = [
                [field_name, get_chunk_by_field_name(doc, field_name)]
                for field_name in doc._metadata['multi_modal_schema'].keys()
            ]
        else:
            # TODO remove else path. It is only used to support the inmemory indexer since that one is operating on chunks while elastic responds with root documents
            field_names_and_chunks = [['result_field', doc]]
        results = {}
        for field_name, chunk in field_names_and_chunks:
            if chunk.blob:
                result = {'blob': base64.b64encode(chunk.blob).decode('utf-8')}
            elif chunk.text:
                result = {'text': chunk.text}
            elif chunk.uri:
                # in case we have content and uri, the content is preferred
                result = {'uri': chunk.uri}
            else:
                # We should not raise exception else it breaks the playground if a single chunk has no content
                # irrespective of what other chunks hold. We should just log it and move on.
                print('Result without content', doc.id, doc.tags)
                result = {'text': ''}
            results[field_name] = result
        match = SearchResponseModel(
            id=doc.id,
            scores=scores,
            tags=doc.tags,
            fields=results,
        )
        matches.append(match)
    return matches


def get_semantic_scores(
    data: SearchRequestModel, field_names_to_dataclass_fields: Dict[str, str]
) -> List[List[Any]]:
    """
    Extract and process the semantic scores from the request model to the format expected by the indexer.
    This includes converting the field names to the dataclass field names, for the query and for the index fields.
    Because `bm25_text` is not a field in the dataclass, it is not converted. It is a special field created by the
    indexer for the bm25 score.

    :param data: the request model
    :param field_names_to_dataclass_fields: a mapping from the field names in the request model to the field names in the dataclass
    :return: the semantic scores in the format expected by the indexer. Example:
        [['query_text', 'my_product_image', 'encoderclip', 1], ['query_text', 'bm25_text', 'bm25', 1]]
    """
    semantic_scores = []
    for sem_score in data.semantic_scores:
        sem_score = list(sem_score)
        sem_score[0] = field_names_to_dataclass_fields[sem_score[0]]
        try:
            sem_score[1] = user_input_in_bff.field_names_to_dataclass_fields[
                sem_score[1]
            ]
        except KeyError:
            if sem_score[1] != 'bm25_text':
                raise KeyError(
                    f'Field {sem_score[1]} not found in dataclass. Please select possible values: {user_input_in_bff.field_names_to_dataclass_fields.keys()}'
                )
        semantic_scores.append(sem_score)
    return semantic_scores


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
