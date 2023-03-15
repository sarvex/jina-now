from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

from docarray import Document, DocumentArray

from now.utils.docarray.helpers import get_chunk_by_field_name

metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}


def generate_score_calculation(
    docs_map: Dict[str, DocumentArray],
    encoder_to_fields: Dict[str, Union[List[str], str]],
) -> List[List]:
    """
    Generate score calculation from document mappings.

    :param docs_map: dictionary mapping encoder to DocumentArray.
    :param encoder_to_fields: dictionary mapping encoder to fields.
    :return: a list of score calculation, each of which is a tuple of
        (query_field, document_field, encoder, linear_weight).
        score calculation would then be for example:
        [('query_text', 'title', 'clip', 1.0)]
    """
    score_calculation = []
    for executor_name, da in docs_map.items():
        first_doc = da[0]
        field_names = first_doc._metadata['multi_modal_schema'].keys()
        try:
            document_fields = encoder_to_fields[executor_name]
        except KeyError as e:
            raise KeyError(
                f'Documents are not encoded with same encoder as query. executor_name: {executor_name}, encoder_to_fields: {encoder_to_fields}'
            ) from e
        for field_name in field_names:
            chunk = get_chunk_by_field_name(first_doc, field_name)
            if chunk.chunks.embeddings is None and chunk.embedding is None:
                continue
            for document_field in document_fields:
                score_calculation.append(
                    [
                        field_name,
                        document_field,
                        executor_name,
                        1,
                    ]
                )

    return score_calculation


def build_es_queries(
    docs_map,
    get_score_breakdown: bool,
    score_calculation: List[Tuple],
    metric: Optional[str] = 'cosine',
    filter: dict = {},
    query_to_curated_ids: Dict[str, list] = {},
) -> Dict:
    """
    Build script-score query used in Elasticsearch. To do this, we extract
    embeddings from the query document and pass them in the script-score
    query together with the fields to search on in the Elasticsearch index.
    The query document will be returned with all of its embeddings as tags with
    their corresponding field+encoder as key.

    :param docs_map: dictionary mapping encoder to DocumentArray.
    :param get_score_breakdown: whether to return the score breakdown for matches.
        For this function, this parameter determines whether to return the embeddings
        of a query document.
    :param score_calculation: list of nested lists containing (query_field, document_field, matching_method, linear_weight) which define
        how to calculate the score. Note, that the matching_method is the name of the encoder or `bm25`.
    :param metric: metric to use for vector search.
    :param filter: dictionary of filters to apply to the search.
    :param query_to_curated_ids: dictionary mapping query text to list of curated ids.
    :return: a dictionary containing query and filter.
    """
    queries = {}
    pinned_queries = {}
    docs = {}
    sources = {}
    script_params = defaultdict(dict)
    for executor_name, da in docs_map.items():
        for doc in da:
            if doc.id not in docs:
                docs[doc.id] = doc
                docs[doc.id].tags['embeddings'] = {}

            if doc.id not in queries:
                queries[doc.id] = get_default_query(
                    doc,
                    score_calculation,
                    filter,
                )
                pinned_queries[doc.id] = get_pinned_query(
                    doc,
                    query_to_curated_ids,
                )

                if any(
                    _matching_method == 'bm25'
                    for (_, _, _matching_method, _) in score_calculation
                ):
                    sources[doc.id] = '1.0 + _score / (_score + 10.0)'
                else:
                    sources[doc.id] = '1.0'

            for (
                query_field,
                document_field,
                matching_method,
                linear_weight,
            ) in get_scores(executor_name, score_calculation):
                field_doc = get_chunk_by_field_name(doc, query_field)
                if get_score_breakdown:
                    docs[doc.id].tags['embeddings'][
                        f'{query_field}-{matching_method}'
                    ] = field_doc.embedding

                query_string = f'params.query_{query_field}_{executor_name}'
                document_string = f'{document_field}-{matching_method}'

                sources[
                    doc.id
                ] += f" + {float(linear_weight)}*{metrics_mapping[metric]}({query_string}, '{document_string}.embedding')"

                script_params[doc.id][
                    f'query_{query_field}_{executor_name}'
                ] = field_doc.embedding

    es_queries = []

    for doc_id, query in queries.items():
        script_score = {
            'script_score': {
                'query': {
                    'bool': query['bool'],
                },
                'script': {
                    'source': sources[doc_id],
                    'params': script_params[doc_id],
                },
            },
        }
        if pinned_queries[doc_id]:
            query_json = {'pinned': pinned_queries[doc_id]['pinned']}
            query_json['pinned']['organic'] = script_score
        else:
            query_json = script_score
        es_queries.append((docs[doc_id], query_json))
    return es_queries


def get_default_query(
    doc: Document,
    score_calculation: List[Tuple],
    filter: Dict = {},
):
    query = {
        'bool': {
            'should': [
                {'match_all': {}},
            ],
        },
    }

    # build bm25 part
    for (query_field, index_field, matching_method, linear_weight) in score_calculation:
        if matching_method == 'bm25':
            text = get_chunk_by_field_name(doc, query_field).text
            query['bool']['should'].append(
                {
                    'multi_match': {
                        'query': text,
                        'fields': [f"{index_field}^{linear_weight}"],
                    }
                }
            )

    # add filter
    if filter:
        es_search_filter = process_filter(filter)
        query['bool']['filter'] = es_search_filter

    return query


def get_pinned_query(doc: Document, query_to_curated_ids: Dict[str, list] = {}) -> Dict:
    pinned_query = {}
    if getattr(doc, 'query_text', None):
        query_text = doc.query_text.text
        if query_text in query_to_curated_ids.keys():
            pinned_query = {'pinned': {'ids': query_to_curated_ids[query_text]}}
    return pinned_query


def process_filter(
    filter: Dict[str, Union[List[str], Dict[str, float]]]
) -> List[Dict[str, Any]]:
    es_search_filters = []
    for field, filters in filter.items():
        field = field.replace('__', '.', 1)
        es_search_filter = {}
        if isinstance(filters, list):  # must be categorical (list of terms)
            es_search_filter['terms'] = {field: filters}
        elif isinstance(filters, dict):  # must be numerical (range with operators)
            es_search_filter['range'] = {field: filters}
        else:
            raise ValueError(
                f'Filter {field}: {filters} is not a list of terms or a dictionary of ranges'
            )
        es_search_filters.append(es_search_filter)
    return es_search_filters


def get_scores(encoder, score_calculation):
    for (
        query_field,
        document_field,
        _encoder,
        linear_weight,
    ) in score_calculation:
        if encoder == _encoder:
            yield query_field, document_field, _encoder, linear_weight
