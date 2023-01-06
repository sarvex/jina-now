import base64
from typing import List

from docarray import Document, DocumentArray, dataclass, field
from docarray.typing import Image, Text, Video
from fastapi import APIRouter

from deployment.bff.app.v1.models.search import (
    IndexRequestModel,
    SearchRequestModel,
    SearchResponseModel,
    SuggestionRequestModel,
)
from deployment.bff.app.v1.routers.helper import (
    fetch_user_input,
    field_dict_to_mm_doc,
    jina_client_post,
)
from now.data_loading.create_dataclass import create_dataclass

router = APIRouter()


@router.post(
    "/index",
    summary='Add more data to the indexer',
)
def index(data: IndexRequestModel):
    index_docs = DocumentArray()

    user_input = fetch_user_input(data)
    data_class = create_dataclass(user_input)

    for field_dict, tags_dict in data.data:
        doc = field_dict_to_mm_doc(
            field_dict,
            data_class=data_class,
            field_names_to_dataclass_fields=user_input.files_to_dataclass_fields,
        )
        doc.tags.update(tags_dict)
        index_docs.append(doc)

    jina_client_post(
        request_model=data,
        inputs=index_docs,
        endpoint='/index',
    )


@router.post(
    "/search",
    response_model=List[SearchResponseModel],
    summary='Search data via query',
)
def search(data: SearchRequestModel):
    # temporary class until actual mm docs are created
    @dataclass
    class MMQueryDoc:
        query_text: Text = field(default=None)
        query_image: Image = field(default=None)
        query_video: Video = field(default=None)

    query_doc = field_dict_to_mm_doc(data.query, data_class=MMQueryDoc)

    query_filter = {key: {'$eq': value} for key, value in data.filters.items()}

    docs = jina_client_post(
        endpoint='/search',
        inputs=query_doc,
        parameters={'limit': data.limit, 'filter': query_filter},
        request_model=data,
    )
    matches = []
    for doc in docs[0].matches:
        # todo: use multimodal doc in the future
        scores = {}
        for score_name, named_score in doc.scores.items():
            scores[score_name] = named_score.to_dict()
        # since multimodal doc is not supported, we take the first chunk
        if doc.chunks:
            field_names = doc._metadata['multi_modal_schema'].keys()
            field_names_and_chunks = [
                [field_name, getattr(doc, field_name)] for field_name in field_names
            ]
        else:
            # TODO remove else path. It is only used to support the inmemory indexer since that one is operating on chunks while elastic responds with root documents
            field_names_and_chunks = [['result_field', doc]]
        results = {}

        for field_name, chunk in field_names_and_chunks:
            if chunk.uri:
                result = {'uri': chunk.uri}
            elif chunk.blob:
                result = {'blob': base64.b64encode(chunk.blob).decode('utf-8')}
            elif chunk.text:
                result = {'text': chunk.text}
            else:
                raise Exception('Result without content', doc.id, doc.tags)
            results[field_name] = result
        match = SearchResponseModel(
            id=doc.id,
            scores=scores,
            tags=doc.tags,
            fields=results,
        )
        matches.append(match)
    return matches


@router.post(
    "/suggestion",
    summary='Get auto complete suggestion for query',
)
def suggestion(data: SuggestionRequestModel):
    suggest_doc = Document(text=data.text)
    docs = jina_client_post(
        endpoint='/suggestion',
        inputs=suggest_doc,
        request_model=data,
    )
    return docs.to_dict()
