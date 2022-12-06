import traceback
from collections import defaultdict, namedtuple
from typing import Any, Dict, List, Mapping, Optional, Union

from docarray import Document, DocumentArray
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from now.executor.abstract.auth import SecurityLevel, secure_request
from now.executor.abstract.base_indexer import NOWBaseIndexer as Executor
from now.executor.indexer.elastic.es_converter import ESConverter
from now.executor.indexer.elastic.semantic_score import Scores

metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}

ESConverter = ESConverter()

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


class NOWElasticIndexer(Executor):
    """
    NOWElasticIndexer indexes Documents into an Elasticsearch instance. To do this,
    it uses the ESConverter, converting documents to and from the accepted Elasticsearch
    format. It also uses the semantic scores to combine the scores of different fields/encoders,
    allowing multi-modal documents to be indexed and searched with multi-modal queries.
    """

    # override
    def construct(
        self,
        default_semantic_scores: List[SemanticScore],
        document_mappings: List[FieldEmbedding],
        es_mapping: Dict = None,
        hosts: Union[
            str, List[Union[str, Mapping[str, Union[str, int]]]], None
        ] = 'https://elastic:elastic@localhost:9200',
        es_config: Optional[Dict[str, Any]] = None,
        metric: str = 'cosine',
        index_name: str = 'now-index',
        traversal_paths: str = '@r',
        limit: int = 20,
        **kwargs,
    ):
        """
        Initialize/construct function for the NOWElasticIndexer.

        :param default_semantic_scores: list of SemanticScore tuples that define how
            to combine the scores of different fields and encoders.
        :param document_mappings: list of FieldEmbedding tuples that define which encoder
            encodes which fields, and the embedding size of the encoder.
        :param hosts: host configuration of the Elasticsearch node or cluster
        :param es_config: Elasticsearch cluster configuration object
        :param metric: The distance metric used for the vector index and vector search
        :param dims: The dimensions of your embeddings.
        :param index_name: ElasticSearch Index name used for the storage
        :param es_mapping: Mapping for new index. If none is specified, this will be
            generated from `document_mappings` and `metric`.
        :param traversal_paths: Default traversal paths on docs
            generated from metric and dims. Embeddings from chunk documents will
            always be stored in fields `embedding_x` where x iterates over the number
            of embedding fields (length of `dims`) to be created in the index.
                (used for indexing, delete and update), e.g. '@r', '@c', '@r,c'.
        :param limit: Default limit on the number of docs to be retrieved
        """
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
        self.es_mapping = (
            self.generate_es_mapping(document_mappings, self.metric)
            if not es_mapping
            else es_mapping
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
        parameters: dict = {},
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

        es_docs = ESConverter.convert_doc_map_to_es(
            docs_map, self.index_name, self.encoder_to_fields
        )
        try:
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
                - 'filter' (dict): The filtering conditions on document tags
                - 'limit' (int): Number of matches to get per Document, default 100.
                - 'get_score_breakdown' (bool): Wether to return the score breakdown, i.e. the scores of each
                    field+encoder combination/comparison.
                - 'apply_default_bm25' (bool): Whether to apply the default bm25 scoring. Default is False. Will
                    be ignored if 'custom_bm25_query' is specified.
                - 'custom_bm25_query' (dict): Custom query to use for BM25. Note: this query can only be
                    passed if also passing `es_mapping`. Otherwise, only default bm25 scoring is enabled.
        """
        if not docs_map:
            return DocumentArray()

        # search_filter = parameters.get('filter', None)
        limit = parameters.get('limit', self.limit)
        get_score_breakdown = parameters.get('get_score_breakdown', False)
        custom_bm25_query = parameters.get('custom_bm25_query', None)
        apply_default_bm25 = parameters.get('apply_default_bm25', False)

        es_queries = self._build_es_queries(
            docs_map, apply_default_bm25, get_score_breakdown, custom_bm25_query
        )
        for doc, query in es_queries:
            try:
                result = self.es.search(
                    index=self.index_name,
                    query=query,
                    source=True,
                    size=limit,
                )['hits']['hits']
                doc.matches = ESConverter.convert_es_results_to_matches(
                    query_doc=doc,
                    es_results=result,
                    get_score_breakdown=get_score_breakdown,
                    metric=self.metric,
                    semantic_scores=self.default_semantic_scores,
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
        consider the default maximum documents in a page returned by `Elasticsearch`.
        Should be addressed in future with `scroll`.

        :param parameters: dictionary with limit and offset
        - offset (int): number of documents to skip
        - limit (int): number of retrieved documents
        """
        limit = int(parameters.get('limit', self.limit))
        offset = int(parameters.get('offset', 0))
        try:
            result = self.es.search(
                index=self.index_name, size=limit, from_=offset, query={'match_all': {}}
            )['hits']['hits']
        except Exception:
            print(traceback.format_exc())
        if result:
            return ESConverter.convert_es_to_da(result, get_score_breakdown=False)
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
                    self.es.indices.refresh(index=self.index_name)
                    resp['deleted'] += r['result'] == 'deleted'
            except Exception as e:
                print(traceback.format_exc(), e)
        else:
            raise ValueError('No filter or IDs provided for deletion.')
        if resp:
            print(
                f"Deleted {resp['deleted']} documents in Elasticsearch index {self.index_name}"
            )
        return DocumentArray()

    # override
    def batch_iterator(self):
        """Unnecessary for ElasticIndexer, but need to override BaseIndexer."""
        yield []

    def _build_es_queries(
        self,
        docs_map,
        apply_default_bm25: bool,
        get_score_breakdown: bool,
        custom_bm25_query: Optional[dict] = None,
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
        semantic_scores = self.default_semantic_scores
        scores = Scores(semantic_scores)
        for encoder, da in docs_map.items():
            for doc in da:
                if doc.id not in docs:
                    docs[doc.id] = doc
                    docs[doc.id].tags['embeddings'] = {}

                if doc.id not in queries:
                    queries[doc.id] = self.get_default_query(
                        doc, apply_default_bm25, custom_bm25_query
                    )

                    if apply_default_bm25 or custom_bm25_query:
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

    def get_default_query(
        self, doc: Document, apply_default_bm25: bool, custom_bm25_query: Dict = None
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
                (
                    x
                    for x in self.default_semantic_scores
                    if x.document_encoder == 'bm25'
                ),
                None,
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
