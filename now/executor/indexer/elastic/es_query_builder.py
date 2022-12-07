from collections import defaultdict, namedtuple
from typing import Dict, List, Optional, Union

from docarray import Document, DocumentArray

SemanticScore = namedtuple(
    'SemanticScores',
    [
        'query_field',
        'document_field',
        'encoder',
        'linear_weight',
    ],
)
metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}


class ESQueryBuilder:
    def __init__(self):
        pass

    @staticmethod
    def generate_semantic_scores(
        docs_map: Dict[str, DocumentArray],
        encoder_to_fields: Dict[str, Union[List[str], str]],
    ) -> List[SemanticScore]:
        """Generate semantic scores from document mappings."""
        semantic_scores = []
        # either take fields names from _metadata, if multimodal doc
        # or take modality from tags of root doc
        for executor_name, da in docs_map.items():
            if executor_name == 'preprocessor':
                # skip preprocessor
                continue
            first_doc = da[0]
            if first_doc._metadata:  # must be a multimodal doc
                field_names = first_doc._metadata['multi_modal_schema'].keys()
            else:  # must be a unimodal doc
                field_names = [first_doc.tags['modality']]
            try:
                document_fields = encoder_to_fields[executor_name]
            except KeyError as e:
                raise KeyError(
                    'Documents are not encoded with same encoder as query.'
                ) from e
            for field_name in field_names:
                for document_field in document_fields:
                    semantic_scores.append(
                        SemanticScore(
                            query_field=field_name,
                            document_field=document_field,
                            encoder=executor_name,
                            linear_weight=1,
                        )
                    )

        return semantic_scores

    def build_es_queries(
        self,
        docs_map,
        apply_default_bm25: bool,
        get_score_breakdown: bool,
        semantic_scores: List[SemanticScore],
        custom_bm25_query: Optional[dict] = None,
        metric: Optional[str] = 'cosine',
    ) -> Dict:
        """
        Build script-score query used in Elasticsearch. To do this, we extract
        embeddings from the query document and pass them in the script-score
        query together with the fields to search on in the Elasticsearch index.
        The query document will be returned with all of its embeddings as tags with
        their corresponding field+encoder as key.

        :param docs_map: dictionary mapping encoder to DocumentArray.
        :param apply_default_bm25: whether to combine bm25 with vector search. If False,
            will only perform vector search. If True, must supply a text
            field for bm25 searching.
        :param get_score_breakdown: whether to return the score breakdown for matches.
            For this function, this parameter determines whether to return the embeddings
            of a query document.
        :param custom_bm25_query: custom query to use for BM25.
        :param search_filter: dictionary of filters to apply to the search.
        :return: a dictionary containing query and filter.
        """
        queries = {}
        docs = {}
        sources = {}
        script_params = defaultdict(dict)
        for executor_name, da in docs_map.items():
            for doc in da:
                if doc.id not in docs:
                    docs[doc.id] = doc
                    docs[doc.id].tags['embeddings'] = {}

                if doc.id not in queries:
                    queries[doc.id] = self.get_default_query(
                        doc, apply_default_bm25, semantic_scores, custom_bm25_query
                    )

                    if apply_default_bm25 or custom_bm25_query:
                        sources[doc.id] = '1.0 + _score / (_score + 10.0)'
                    else:
                        sources[doc.id] = '1.0'

                for (
                    query_field,
                    document_field,
                    encoder,
                    linear_weight,
                ) in self.get_scores(executor_name, semantic_scores):
                    field_doc = getattr(doc, query_field)
                    if get_score_breakdown:
                        docs[doc.id].tags['embeddings'][
                            f'{query_field}-{encoder}'
                        ] = field_doc.embedding

                    query_string = f'params.query_{query_field}_{executor_name}'
                    document_string = f'{document_field}-{encoder}'

                    sources[
                        doc.id
                    ] += f" + {float(linear_weight)}*{metrics_mapping[metric]}({query_string}, '{document_string}.embedding')"

                    script_params[doc.id][
                        f'query_{query_field}_{executor_name}'
                    ] = field_doc.embedding

        es_queries = []

        for doc_id, query in queries.items():
            query_json = {
                'script_score': {
                    'query': query,
                    'script': {
                        'source': sources[doc_id],
                        'params': script_params[doc_id],
                    },
                }
            }
            es_queries.append((docs[doc_id], query_json))
        return es_queries

    @staticmethod
    def get_default_query(
        doc: Document,
        apply_default_bm25: bool,
        semantic_scores: List[SemanticScore],
        custom_bm25_query: Dict = None,
    ):
        query = {
            'bool': {
                'should': [
                    {'match_all': {}},
                ],
            },
        }

        # build bm25 part
        if apply_default_bm25:
            bm25_semantic_score = next(
                (x for x in semantic_scores if x.encoder == 'bm25')
            )
            if not bm25_semantic_score:
                raise ValueError(
                    'No bm25 semantic scores found. Please specify this in the default_semantic_scores.'
                )
            text = getattr(doc, bm25_semantic_score.query_field).text
            multi_match = {'multi_match': {'query': text, 'fields': ['bm25_text']}}
            query['bool']['should'].append(multi_match)
        elif custom_bm25_query:
            query['bool']['should'].append(custom_bm25_query)

        # add filter
        if 'es_search_filter' in doc.tags:
            query['bool']['filter'] = doc.tags['search_filter']
        elif 'search_filter' in doc.tags:
            search_filter = doc.tags['search_filter']
            es_search_filter = {}

            for field, filters in search_filter.items():
                for operator, filter in filters.items():
                    if isinstance(filter, str):
                        es_search_filter['term'] = {"tags." + field: filter}
                    elif isinstance(filter, int) or isinstance(filter, float):
                        operator = operator.replace('$', '')
                        es_search_filter['range'] = {
                            "tags." + field: {operator: filter}
                        }
            query['bool']['filter'] = es_search_filter

        return query

    @staticmethod
    def get_scores(encoder, semantic_scores):
        for (
            query_field,
            document_field,
            _encoder,
            linear_weight,
        ) in semantic_scores:
            if encoder == _encoder:
                yield query_field, document_field, _encoder, linear_weight
