import subprocess
import traceback
from collections import namedtuple
from time import sleep
from typing import Any, Dict, List, Mapping, Optional, Union

from docarray import Document, DocumentArray
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from now.executor.abstract.auth import SecurityLevel, secure_request
from now.executor.abstract.base_indexer import NOWBaseIndexer as Executor
from now.executor.indexer.elastic.es_converter import ESConverter
from now.executor.indexer.elastic.es_preprocessing import merge_subdocuments
from now.executor.indexer.elastic.es_query_building import (
    SemanticScore,
    build_es_queries,
    generate_semantic_scores,
    process_filter,
)

ESConverter = ESConverter()

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
        document_mappings: List[List],  # cannot take FieldEmbedding (not serializable)
        default_semantic_scores: Optional[List[SemanticScore]] = None,
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
        self.document_mappings = [FieldEmbedding(*dm) for dm in document_mappings]
        self.default_semantic_scores = default_semantic_scores or None
        self.encoder_to_fields = {
            document_mapping.encoder: document_mapping.fields
            for document_mapping in self.document_mappings
        }
        self.es_config = es_config or {'verify_certs': False}
        self.es_mapping = es_mapping or self.generate_es_mapping(
            self.document_mappings, self.metric
        )
        self.setup_elastic_server()
        self.es = Elasticsearch(hosts=self.hosts, **self.es_config, ssl_show_warn=False)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=self.es_mapping)
        self.query_to_curated_ids = {}

    def setup_elastic_server(self):
        # volume is not persisted at the moment
        try:
            subprocess.Popen(['/usr/local/bin/docker-entrypoint.sh'])
            sleep(10)
            self.logger.info('elastic server started')
        except FileNotFoundError:
            self.logger.info(
                'Elastic started outside of docker, assume cluster started already.'
            )

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
        docs_map: Dict[str, DocumentArray],  # encoder to docarray
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
        preprocessed_docs_map = merge_subdocuments(docs_map, self.encoder_to_fields)
        es_docs = ESConverter.convert_doc_map_to_es(
            preprocessed_docs_map, self.index_name, self.encoder_to_fields
        )
        try:
            success, _ = bulk(self.es, es_docs)
            self.es.indices.refresh(index=self.index_name)
        except Exception as e:
            self.logger.info(traceback.format_exc())
            raise
        if success:
            self.logger.info(
                f'Inserted {success} documents into Elasticsearch index {self.index_name}'
            )
        self.update_tags()
        return DocumentArray([])

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
        semantic_scores = parameters.get('default_semantic_scores', None)
        filter = parameters.get('filter', {})
        if not self.default_semantic_scores:
            self.default_semantic_scores = semantic_scores or generate_semantic_scores(
                docs_map, self.encoder_to_fields
            )
        es_queries = build_es_queries(
            docs_map=docs_map,
            apply_default_bm25=apply_default_bm25,
            get_score_breakdown=get_score_breakdown,
            semantic_scores=self.default_semantic_scores,
            custom_bm25_query=custom_bm25_query,
            metric=self.metric,
            filter=filter,
            query_to_curated_ids=self.query_to_curated_ids,
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
                doc.tags.pop('embeddings')
            except Exception:
                self.logger.info(traceback.format_exc())
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
            self.logger.info(traceback.format_exc())
        if result:
            return ESConverter.convert_es_to_da(result, get_score_breakdown=False)
        else:
            return DocumentArray()

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
            es_search_filter = {
                'query': {'bool': {'filter': process_filter(search_filter)}}
            }
            try:
                resp = self.es.delete_by_query(
                    index=self.index_name, body=es_search_filter
                )
                self.es.indices.refresh(index=self.index_name)
                self.update_tags()
            except Exception:
                self.logger.info(traceback.format_exc())
                raise
        elif ids:
            resp = {'deleted': 0}
            try:
                for id in ids:
                    r = self.es.delete(index=self.index_name, id=id)
                    self.es.indices.refresh(index=self.index_name)
                    resp['deleted'] += r['result'] == 'deleted'
            except Exception as e:
                self.logger.info(traceback.format_exc(), e)
        else:
            raise ValueError('No filter or IDs provided for deletion.')
        if resp:
            self.logger.info(
                f"Deleted {resp['deleted']} documents in Elasticsearch index {self.index_name}"
            )
        return DocumentArray()

    @secure_request(on='/tags', level=SecurityLevel.USER)
    def get_tags_and_values(self, **kwargs):
        """
        Endpoint to get all tags and their possible values in the index.
        """
        return DocumentArray([Document(text='tags', tags={'tags': self.doc_id_tags})])

    @secure_request(on='/curate', level=SecurityLevel.USER)
    def curate(self, parameters: dict = {}, **kwargs):
        """
        This endpoint is only relevant for text queries.
        It defines the top results as a list of IDs for
        each query, and stores these as dictionary items.
        `query_to_filter` sent in the `parameters` should
        have the following format:
        {
            'query_to_filter': {
                'query1': [
                    {'uri': {'$eq': 'uri1'}},
                    {'tags__internal_id': {'$eq': 'id1'}},
                ],
                'query2': [
                    {'uri': {'$eq': 'uri2'}},
                    {'tags__color': {'$eq': 'red'}},
                ],
            }
        }
        """
        search_filter = parameters.get('query_to_filter', None)
        if search_filter:
            self.update_curated_ids(search_filter)
        else:
            raise ValueError('No filter provided for curating.')

    def update_curated_ids(self, search_filter):
        for query, filters in search_filter.items():
            if query not in self.query_to_curated_ids:
                self.query_to_curated_ids[query] = []
            for filter in filters:
                es_query = {'query': {'bool': {'filter': process_filter(filter)}}}
                try:
                    resp = self.es.search(
                        index=self.index_name, body=es_query, size=100
                    )
                    self.es.indices.refresh(index=self.index_name)
                    ids = [r['_id'] for r in resp['hits']['hits']]
                    self.query_to_curated_ids[query] += [
                        id for id in ids if id not in self.query_to_curated_ids[query]
                    ]

                except Exception:
                    self.logger.info(traceback.format_exc())
                    raise

    def update_tags(self):
        """
        The indexer keeps track of which tags are indexed and what their possible
        values are, which is stored in self.doc_id_tags. This method queries the
        elasticsearch index for the current es_mapping to find the current tags on all
        indexed documents. It then queries elasticsearch for an aggregation of all values
        inside this field, and updates the self.doc_id_tags dictionary with tags as keys,
        and values as values in the dictionary.
        """
        es_mapping = self.es.indices.get_mapping(index=self.index_name)
        tag_categories = (
            es_mapping.get(self.index_name, {})
            .get('mappings', {})
            .get('properties', {})
            .get('tags', {})
            .get('properties', {})
        )

        aggs = {'aggs': {}, 'size': 0}
        for tag, map in tag_categories.items():
            if map['type'] == 'text':
                aggs['aggs'][tag] = {
                    'terms': {'field': f'tags.{tag}.keyword', 'size': 100}
                }
            elif map['type'] == 'float':
                # aggs['aggs'][f'min_{tag}'] = {'min': {'field': f'tags.{tag}'}}
                # aggs['aggs'][f'max_{tag}'] = {'max': {'field': f'tags.{tag}'}}
                # aggs['aggs'][f'avg_{tag}'] = {'avg': {'field': f'tags.{tag}'}}
                aggs['aggs'][tag] = {'terms': {'field': f'tags.{tag}', 'size': 100}}
        try:
            result = self.es.search(index=self.index_name, body=aggs)
            aggregations = result['aggregations']
            updated_tags = {}
            for tag, agg in aggregations.items():
                updated_tags[tag] = [bucket['key'] for bucket in agg['buckets']]
            self.doc_id_tags = updated_tags
        except Exception:
            self.logger.info(traceback.format_exc())

    # override
    def batch_iterator(self):
        """Unnecessary for ElasticIndexer, but need to override BaseIndexer."""
        yield []
