import traceback
from collections import defaultdict, namedtuple
from typing import Any, Dict, List, Mapping, Optional, Union

from docarray import Document, DocumentArray
from docarray.score import NamedScore
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from numpy import dot
from numpy.linalg import norm

from now.executor.abstract.auth import (
    SecurityLevel,
    get_auth_executor_class,
    secure_request,
)
from now.executor.indexer.elastic.semantic_score import Scores

metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}

Executor = get_auth_executor_class()

SemanticScore = namedtuple(
    'SemanticScores',
    [
        'query_field',
        'query_encoder',
        'document_field',
        'document_encoder',
        'linear_weight',
    ],
)

FieldEmbedding = namedtuple(
    'FieldEmbedding',
    ['encoder', 'embedding_size', 'fields'],
)


class ElasticIndexer(Executor):
    def __init__(
        self,
        default_semantic_scores: List[SemanticScore],
        document_mappings: List[FieldEmbedding],
        hosts: Union[
            str, List[Union[str, Mapping[str, Union[str, int]]]], None
        ] = 'https://elastic:elastic@localhost:9200',
        es_config: Optional[Dict[str, Any]] = None,
        metric: str = 'cosine',
        index_name: str = 'now-index',
        traversal_paths: str = '@r',
        limit: int = 100,
        **kwargs,
    ):
        """
        Initializer function for the ElasticIndexer

        :param hosts: host configuration of the Elasticsearch node or cluster
        :param es_config: Elasticsearch cluster configuration object
        :param metric: The distance metric used for the vector index and vector search
        :param dims: The dimensions of your embeddings.
        :param index_name: ElasticSearch Index name used for the storage
        :param es_mapping: Mapping for new index. If none is specified, this will be
            generated from metric and dims. Embeddings from chunk documents will
            always be stored in fields `embedding_x` where x iterates over the number
            of embedding fields (length of `dims`) to be created in the index.
        :param traversal_paths: Default traversal paths on docs
                (used for indexing, delete and update), e.g. '@r', '@c', '@r,c'.
        :param limit: Default limit on the number of docs to be retrieved
        """
        super().__init__(**kwargs)

        self.hosts = hosts
        self.metric = metric
        self.index_name = index_name
        self.traversal_paths = traversal_paths
        self.limit = limit
        self.default_semantic_scores = default_semantic_scores
        self.encoder_to_fields = {
            document_mapping.encoder: document_mapping.fields
            for document_mapping in document_mappings
        }
        self.es_config = {'verify_certs': False} if not es_config else es_config
        self.es_mapping = ElasticIndexer.generate_es_mapping(
            document_mappings, self.metric
        )
        self.es = Elasticsearch(hosts=self.hosts, **self.es_config, ssl_show_warn=False)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=self.es_mapping)

    @staticmethod
    def generate_es_mapping(
        document_mappings: List[FieldEmbedding], metric: str
    ) -> Dict:
        """Creates Elasticsearch mapping for the defined document fields.

        :param document_mappings: field descriptions of the to-be-queryable vector representations
        :param metric: The distance metric used for the vector index and vector search
        """
        es_mapping = {
            'properties': {
                'id': {'type': 'keyword'},
                'bm25_text': {'type': 'text', 'analyzer': 'standard'},
            }
        }

        for encoder, embedding_size, fields in document_mappings:
            for field in fields:
                es_mapping['properties'][f'{field}-{encoder}'] = {
                    'properties': {
                        f'embedding': {
                            'type': 'dense_vector',
                            'dims': str(embedding_size),
                            'similarity': metric,
                            'index': 'true',
                        }
                    }
                }
        return es_mapping

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index(
        self,
        docs_map: Dict[str, DocumentArray] = None,  # encoder to docarray
        parameters: dict = None,
        **kwargs,
    ) -> DocumentArray:
        """
        Index new `Document`s by adding them to the Elasticsearch index.

        :param docs: Documents to be indexed.
        :param parameters: dictionary with options for indexing.
        :return: empty `DocumentArray`.
        """
        if not docs_map:
            return DocumentArray()
        if not parameters:
            parameters = {}

        es_docs = self._doc_map_to_es(docs_map)
        try:
            # self.es.index(document=es_docs[0], index=es_docs[0]['_index'])
            success, _ = bulk(self.es, es_docs)
            self.es.indices.refresh(index=self.index_name)
        except Exception as e:
            print(traceback.format_exc())
            raise
        if success:
            print(
                f'Inserted {success} documents into Elasticsearch index {self.index_name}'
            )
        return (
            DocumentArray()
        )  # prevent sending the data back by returning an empty DocumentArray

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search(
        self,
        docs_map: Dict[str, DocumentArray] = None,  # encoder to docarray
        parameters: dict = {},
        **kwargs,
    ):
        """Perform traditional bm25 + vector search. By convention, BM25 will search on
        the 'bm25_text' field of the index. For now, this field contains a concatenation of
        all text chunks of the documents.

        Search can be performed with candidate filtering. Filters are a triplet (column,operator,value).
        More than a filter can be applied during search. Therefore, conditions for a filter are specified as a list triplets.
        Each triplet contains:
            - field: Field used to filter.
            - operator: Binary operation between two values. Some supported operators include `['>','<','=','<=','>=']`.
            - value: value used to compare a candidate.

        :param docs: query `Document`s.
        :param parameters: dictionary of options for searching.
            Keys accepted:
                - 'filter' (dict): the filtering conditions on document tags
                - 'traversal_paths' (str): traversal paths for the docs
                - 'limit' (int): nr of matches to get per Document
        """
        if not docs_map:
            return DocumentArray()
        if not parameters:
            parameters = {}

        # search_filter = parameters.get('filter', None)
        limit = parameters.get('limit', self.limit)
        apply_bm25 = parameters.get('apply_bm25', False)
        get_score_breakdown = parameters.get('get_score_breakdown', False)

        es_queries = self._build_es_queries(docs_map, apply_bm25, get_score_breakdown)
        for doc, query in es_queries:
            try:
                result = self.es.search(
                    index=self.index_name,
                    query=query,
                    source=True,
                    size=limit,
                )['hits']['hits']
                doc.matches = self._transform_es_results_to_matches(
                    query_doc=doc,
                    es_results=result,
                    get_score_breakdown=get_score_breakdown,
                )
                doc.tags.pop('embeddings', None)
            except Exception:
                print(traceback.format_exc())
        return DocumentArray(list(zip(*es_queries))[0])

    @secure_request(on='/update', level=SecurityLevel.USER)
    def update(self, docs: DocumentArray, **kwargs) -> DocumentArray:
        """
        TODO: implement update endpoint, eg. update ES docs with new embeddings etc.
        """
        raise NotImplementedError()

    @secure_request(on='/list', level=SecurityLevel.USER)
    def list(self, parameters: dict = {}, **kwargs):
        """List all indexed documents.

        Note: this implementation is naive and does not
        consider the default maximum documents in a page returned by Elasticsearch.
        Should be addressed in future with `scroll`.

        :param parameters: dictionary with limit and offset
        - offset (int): number of documents to skip
        - limit (int): number of retrieved documents
        """
        limit = int(parameters.get('limit', self.limit))
        offset = int(parameters.get('offset', 0))
        try:
            # TODO: move the limit and offset to the ES query. That will speed up things a lot.
            result = self.es.search(
                index=self.index_name, size=limit, from_=offset, query={'match_all': {}}
            )['hits']['hits']
        except Exception:
            print(traceback.format_exc())
        if result:
            return self._transform_es_to_da(result, get_score_breakdown=False)
        else:
            return result

    @secure_request(on='/delete', level=SecurityLevel.USER)
    def delete(self, parameters: dict = {}, **kwargs):
        """
        Endpoint to delete documents from an index. Either delete documents by filter condition
        or by specifying a list of document IDs.

        :param parameters: dictionary with filter conditions or list of IDs to select
            documents for deletion.
        """
        search_filter = parameters.get('filter', None)
        ids = parameters.get('ids', None)
        if search_filter:
            es_search_filter = {'query': {'bool': {}}}
            for field, filters in search_filter.items():
                for operator, filter in filters.items():
                    if isinstance(filter, str):
                        es_search_filter['query']['bool']['filter'] = {
                            'term': {'tags.' + field: filter}
                        }
                    elif isinstance(filter, int) or isinstance(filter, float):
                        operator = operator.replace('$', '')
                        es_search_filter['query']['bool']['filter'] = {
                            'range': {'tags.' + field: {operator: filter}}
                        }
            try:
                resp = self.es.delete_by_query(
                    index=self.index_name, body=es_search_filter
                )
                self.es.indices.refresh(index=self.index_name)
            except Exception:
                print(traceback.format_exc())
                raise
        elif ids:
            resp = {'deleted': 0}
            try:
                for id in ids:
                    r = self.es.delete(index=self.index_name, id=id)
                    resp['deleted'] += r['deleted']
            except Exception as e:
                print(traceback.format_exc(), e)
        else:
            raise ValueError('No filter or IDs provided for deletion.')
        if resp:
            print(
                f"Deleted {resp['deleted']} documents in Elasticsearch index {self.index_name}"
            )
        return DocumentArray()

    def _build_es_queries(
        self,
        docs_map,
        apply_bm25: bool,
        get_score_breakdown: bool,
    ) -> Dict:
        """
        Build script-score query used in Elasticsearch. To do this, we extract
        embeddings from the query document and pass them in the script-score
        query together with the fields to search on in the Elasticsearch index.
        The query document will be returned with all of its embeddings as tags with
        their corresponding field+encoder as key.

        :param docs_map: dictionary mapping encoder to DocumentArray.
        :param apply_bm25: whether to combine bm25 with vector search. If False,
            will only perform vector search. If True, must supply a text
            field for bm25 searching.
        :param get_score_breakdown: whether to return the score breakdown for matches.
            For this function, this parameter determines whether to return the embeddings
            of a query document.
        :return: a dictionary containing query and filter.
        """
        queries = {}
        docs = {}
        sources = {}
        script_params = defaultdict(dict)
        semantic_scores = self.default_semantic_scores
        scores = Scores(semantic_scores)
        for encoder, da in docs_map.items():
            for doc in da:
                if doc.id not in docs:
                    docs[doc.id] = doc
                    docs[doc.id].tags['embeddings'] = {}

                if doc.id not in queries:
                    queries[doc.id] = ElasticIndexer._get_default_query(doc, apply_bm25)

                    if apply_bm25:
                        sources[doc.id] = '1.0 + _score / (_score + 10.0)'
                    else:
                        sources[doc.id] = '1.0'

                for (
                    query_field,
                    document_field,
                    document_encoder,
                    linear_weight,
                ) in scores.get_scores(encoder):
                    field_doc = getattr(doc, query_field)
                    if get_score_breakdown:
                        docs[doc.id].tags['embeddings'][
                            f'{query_field}-{document_encoder}'
                        ] = field_doc.embedding

                    query_string = f'params.query_{query_field}_{encoder}'
                    document_string = f'{document_field}-{document_encoder}'

                    sources[
                        doc.id
                    ] += f" + {float(linear_weight)}*{metrics_mapping[self.metric]}({query_string}, '{document_string}.embedding')"

                    script_params[doc.id][
                        f'query_{query_field}_{encoder}'
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
    def _get_default_query(doc, apply_bm25):

        query = {
            'bool': {
                'should': [
                    {'match_all': {}},
                ],
            },
        }

        # build bm25 part
        if apply_bm25:
            text = doc.text
            multi_match = {'multi_match': {'query': text, 'fields': ['bm25_text']}}
            query['bool']['should'].append(multi_match)

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

    def _transform_es_to_da(
        self, result: Union[Dict, List[Dict]], get_score_breakdown: bool
    ) -> DocumentArray:
        """
        Transform Elasticsearch documents into DocumentArray. Assumes that all Elasticsearch
        documents have a 'text' field. It returns embeddings as part of the tags for each field that is encoded.

        :param result: results from an Elasticsearch query.
        :param get_score_breakdown: whether to return the embeddings as tags for each document.
        :return: a DocumentArray containing all results.
        """
        if isinstance(result, Dict):
            result = [result]
        da = DocumentArray()
        for es_doc in result:
            doc = Document(id=es_doc['_id'])
            for k, v in es_doc['_source'].items():
                if k.startswith('chunk'):
                    chunk = Document.from_dict(v)
                    doc.chunks.append(chunk)
                elif (
                    k.startswith('embedding') or k.endswith('embedding')
                ) and get_score_breakdown:
                    if 'embeddings' not in doc.tags:
                        doc.tags['embeddings'] = {}
                    doc.tags['embeddings'][k] = v
                elif k in ['bm25_text', '_score']:
                    continue
                else:
                    doc.k = v
            da.append(doc)
        return da

    def _doc_map_to_es(self, docs_map: Dict[str, DocumentArray]) -> List[Dict]:
        es_docs = {}

        for encoder, documents in docs_map.items():
            for doc in documents:
                if doc.id not in es_docs:
                    es_doc = self._get_base_es_doc(doc)
                    es_docs[doc.id] = es_doc
                else:
                    es_doc = es_docs[doc.id]
                for encoded_field in self.encoder_to_fields[encoder]:
                    field_doc = getattr(doc, encoded_field)
                    embedding = field_doc.embedding
                    es_doc[f'{encoded_field}-{encoder}.embedding'] = embedding

                    if hasattr(field_doc, 'text') and field_doc.text:
                        es_doc['bm25_text'] += field_doc.text + ' '
        return list(es_docs.values())

    def _get_base_es_doc(self, doc: Document):
        es_doc = {k: v for k, v in doc.to_dict().items() if v}
        es_doc.pop('chunks', None)
        es_doc.pop('_metadata', None)
        es_doc['bm25_text'] = self._get_bm25_fields(doc)
        es_doc['_op_type'] = 'index'
        es_doc['_index'] = self.index_name
        es_doc['_id'] = doc.id
        return es_doc

    def _get_bm25_fields(self, doc: Document):
        try:
            return doc.bm25_text.text
        except:
            return ''

    def _transform_es_results_to_matches(
        self, query_doc: Document, es_results: List[Dict], get_score_breakdown: bool
    ) -> DocumentArray:
        """
        Transform a list of results from Elasticsearch into a matches in the form of a `DocumentArray`.
        :param es_results: List of dictionaries containing results from Elasticsearch querying.
        :return: `DocumentArray` that holds all matches in the form of `Document`s.
        """
        matches = DocumentArray()
        for result in es_results:
            d = self._transform_es_to_da(result, get_score_breakdown)[0]
            d.scores[self.metric] = NamedScore(value=result['_score'])
            if get_score_breakdown:
                d = self.calculate_score_breakdown(query_doc, d)
            matches.append(d)
        return matches

    def calculate_score_breakdown(
        self, query_doc: Document, retrieved_doc: Document
    ) -> Document:
        """
        Calculate the score breakdown for a given retrieved document. Each SemanticScore in the indexers
        `default_semantic_scores` should have a corresponding value, returned inside a list of scores in the documents
        tags under `score_breakdown`.

        :param query_doc: The query document. Contains embeddings for the semantic score calculation at tag level.
        :param retrieved_results: The Elasticsearch results, containing embeddings inside the `_source` field.
        :return: List of integers representing the score breakdown.
        """
        for (
            query_field,
            query_encoder,
            document_field,
            document_encoder,
            linear_weight,
        ) in self.default_semantic_scores:
            if document_encoder == 'bm25':
                continue
            q_emb = query_doc.tags['embeddings'][f'{query_field}-{query_encoder}']
            d_emb = retrieved_doc.tags['embeddings'][
                f'{document_field}-{document_encoder}.embedding'
            ]
            if self.metric == 'cosine':
                score = (
                    dot(q_emb, d_emb) / (norm(q_emb) * norm(d_emb))
                ) * linear_weight
            elif self.metric == 'l2_norm':
                score = norm(q_emb - d_emb) * linear_weight
            else:
                raise ValueError(f'Invalid metric {self.metric}')
            retrieved_doc.scores[
                '-'.join(
                    [
                        query_field,
                        document_field,
                        document_encoder,
                        str(linear_weight),
                    ]
                )
            ] = NamedScore(value=score)

        # calculate bm25 score
        vector_total = sum(
            [v.value for k, v in retrieved_doc.scores.items() if k != self.metric]
        )
        overall_score = retrieved_doc.scores[self.metric].value
        bm25_normalized = overall_score - vector_total
        bm25_raw = (bm25_normalized - 1) * 10

        retrieved_doc.scores['bm25_normalized'] = NamedScore(value=bm25_normalized)
        retrieved_doc.scores['bm25_raw'] = NamedScore(value=bm25_raw)

        # remove embeddings from document
        retrieved_doc.tags.pop('embeddings', None)
        return retrieved_doc
