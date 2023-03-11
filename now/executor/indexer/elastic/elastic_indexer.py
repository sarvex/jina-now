import os
import traceback
from collections import namedtuple
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

import boto3
from docarray import Document, DocumentArray
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from now.constants import DatasetTypes
from now.executor.abstract.auth import (
    SecurityLevel,
    get_auth_executor_class,
    secure_request,
)
from now.executor.indexer.elastic.es_converter import (
    convert_doc_map_to_es,
    convert_es_results_to_matches,
    convert_es_to_da,
)
from now.executor.indexer.elastic.es_query_building import (
    build_es_queries,
    generate_score_calculation,
    process_filter,
)

FieldEmbedding = namedtuple(
    'FieldEmbedding',
    ['encoder', 'embedding_size', 'fields'],
)

Executor = get_auth_executor_class()

TIMEOUT = 60


class NOWElasticIndexer(Executor):
    """
    NOWElasticIndexer indexes Documents into an Elasticsearch instance. To do this,
    it uses helper functions from es_converter, converting documents to and from the accepted Elasticsearch
    format. It also uses the score calculation to combine the scores of different fields/encoders,
    allowing multi-modal documents to be indexed and searched with multi-modal queries.
    """

    def __init__(
        self,
        document_mappings: List[Tuple[str, int, List[str]]],
        metric: str = 'cosine',
        limit: int = 10,
        max_values_per_tag: int = 10,
        es_mapping: Dict = None,
        es_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ):
        """
        :param document_mappings: list of FieldEmbedding tuples that define which encoder
            encodes which fields, and the embedding size of the encoder.
        :param metric: Distance metric type. Can be 'euclidean', 'inner_product', or 'cosine'
        :param limit: Number of results to get for each query document in search
        :param max_values_per_tag: Maximum number of values per tag
        :param es_mapping: Mapping for new index. If none is specified, this will be
            generated from `document_mappings` and `metric`.
        :param hosts: host configuration of the Elasticsearch node or cluster
        :param es_config: Elasticsearch cluster configuration object
        :param index_name: ElasticSearch Index name used for the storage
        """

        super().__init__(*args, **kwargs)
        self.metric = metric
        self.limit = limit
        self.max_values_per_tag = max_values_per_tag
        self._check_env_vars()
        self.hosts = os.getenv('ES_HOSTS', 'http://localhost:9200')
        self.api_key = os.getenv('ES_API_KEY', 'TestApiKey')
        self.index_name = os.getenv('ES_INDEX_NAME', 'now-index')
        self.query_to_curated_ids = {}
        self.doc_id_tags = {}
        self.document_mappings = [FieldEmbedding(*dm) for dm in document_mappings]
        self.encoder_to_fields = {
            document_mapping.encoder: document_mapping.fields
            for document_mapping in self.document_mappings
        }
        self.es_config = es_config or {'verify_certs': False}
        self.es_mapping = es_mapping or self.generate_es_mapping()
        self.es = Elasticsearch(
            hosts=self.hosts,
            api_key=self.api_key,
            **self.es_config,
            ssl_show_warn=False,
        )
        self._do_health_check()
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=self.es_mapping)
        else:
            self.es.indices.put_mapping(index=self.index_name, body=self.es_mapping)

    def _check_env_vars(self):
        while not all(
            var in os.environ for var in ['ES_HOSTS', 'ES_INDEX_NAME', 'ES_API_KEY']
        ):
            timeout_counter = 0
            if timeout_counter < TIMEOUT:
                timeout_counter += 5
                self.logger.info('Environment variables not set yet. Waiting...')
                sleep(5)
            else:
                self.logger.error(
                    'Elasticsearch environment variables not set after 60 seconds. Exiting...'
                )
                raise Exception('Elasticsearch environment variables not set')

    def _do_health_check(self):
        """Checks that Elasticsearch is up and running with state 'yellow'. The default timeout
        on the health check is 30 seconds."""
        while True:
            try:
                self.es.cluster.health(wait_for_status='yellow')
                break
            except Exception:
                self.logger.info(traceback.format_exc())

    def generate_es_mapping(self) -> Dict:
        """Creates Elasticsearch mapping for the defined document fields."""
        es_mapping = {
            'properties': {
                'id': {'type': 'keyword'},
            }
        }

        for field in self.user_input.index_fields:
            if self.user_input.index_field_candidates_to_modalities[field] == 'text':
                es_mapping['properties'][
                    f"{self.user_input.field_names_to_dataclass_fields[field]}"
                ] = {'type': 'text', 'analyzer': 'standard'}

        if self.user_input.filter_fields:
            es_mapping['properties']['tags'] = {'type': 'object', 'properties': {}}
            for field in self.user_input.filter_fields:
                es_mapping['properties']['tags']['properties'][field] = {
                    'type': 'keyword'
                }

        for encoder, embedding_size, fields in self.document_mappings:
            for field in fields:
                es_mapping['properties'][f'{field}-{encoder}'] = {
                    'properties': {
                        f'embedding': {
                            'type': 'dense_vector',
                            'dims': str(embedding_size),
                            'similarity': self.metric,
                            'index': 'true',
                        }
                    }
                }
        return es_mapping

    def _handle_no_docs_map(self, docs: DocumentArray):
        if docs and len(self.encoder_to_fields) == 1:
            return {list(self.encoder_to_fields.keys())[0]: docs}
        else:
            return {}

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index(
        self,
        docs_map: Dict[str, DocumentArray] = None,  # encoder to docarray
        docs: Optional[DocumentArray] = None,
        **kwargs,
    ) -> DocumentArray:
        """
        Index new `Document`s by adding them to the Elasticsearch index.

        :param docs_map: map of encoder to DocumentArray
        :param docs: DocumentArray to index
        :return: empty `DocumentArray`.
        """
        if docs_map is None:
            docs_map = self._handle_no_docs_map(docs)
            if len(docs_map) == 0:
                return DocumentArray()
        aggregate_embeddings(docs_map)
        es_docs = convert_doc_map_to_es(
            docs_map, self.index_name, self.encoder_to_fields
        )
        success, _ = bulk(self.es, es_docs)
        self.es.indices.refresh(index=self.index_name)
        if success:
            self.logger.info(
                f'Inserted {success} documents into Elasticsearch index {self.index_name}'
            )
        self.update_tags()
        return DocumentArray([])

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search(
        self,
        docs_map: Dict[str, DocumentArray] = None,
        parameters: dict = {},
        docs: Optional[DocumentArray] = None,
        **kwargs,
    ):
        """Perform traditional bm25 + vector search.

        Search can be performed with candidate filtering. Filters are a triplet (column,operator,value).
        More than a filter can be applied during search. Therefore, conditions for a filter are specified as a list triplets.
        Each triplet contains:
            - field: Field used to filter.
            - operator: Binary operation between two values. Some supported operators include `['>','<','=','<=','>=']`.
            - value: value used to compare a candidate.

        :param docs_map: map of encoder to DocumentArray
        :param parameters: dictionary of options for searching.
            Keys accepted:
                - 'filter' (dict): The filtering conditions on document tags
                - 'limit' (int): Number of matches to get per Document, default 100.
                - 'get_score_breakdown' (bool): Wether to return the score breakdown, i.e. the scores of each
                    field+encoder combination/comparison.
                - 'score_calculation' (List[List]): list of tuples of (query_field, document_field, matching_method,
                    linear_weight) to show how to calculate the score. Note, that the matching_method is the name of the
                    encoder or `bm25`.
        :param docs: DocumentArray to search
        """
        if docs_map is None:
            docs_map = self._handle_no_docs_map(docs)
            if len(docs_map) == 0:
                return DocumentArray()
        aggregate_embeddings(docs_map)

        filter = parameters.get('filter', {})
        limit = parameters.get('limit', self.limit)
        get_score_breakdown = parameters.get('get_score_breakdown', False)
        score_calculation = parameters.get('score_calculation', None)
        if not score_calculation:
            score_calculation = generate_score_calculation(
                docs_map, self.encoder_to_fields
            )

        es_queries = build_es_queries(
            docs_map=docs_map,
            get_score_breakdown=get_score_breakdown,
            score_calculation=score_calculation,
            metric=self.metric,
            filter=filter,
            query_to_curated_ids=self.query_to_curated_ids,
        )
        for doc, query in es_queries:
            result = self.es.search(
                index=self.index_name,
                query=query,
                source=True,
                size=limit,
            )['hits']['hits']
            doc.matches = convert_es_results_to_matches(
                query_doc=doc,
                es_results=result,
                get_score_breakdown=get_score_breakdown,
                metric=self.metric,
                score_calculation=score_calculation,
            )
            doc.tags.pop('embeddings')
            for c in doc.chunks:
                c.embedding = None
        results = DocumentArray(list(zip(*es_queries))[0])

        if (
            parameters.get('create_temp_link', False)
            and self.user_input.dataset_type == DatasetTypes.S3_BUCKET
        ):
            self._create_temporary_links(results)

        return results

    def _create_temporary_links(self, docs: DocumentArray):
        """For every match, it replaces the URI with a temporary link such that no credentials are needed for access."""

        def _create_temp_link(d: Document) -> Document:
            if (
                not d.text
                and not d.blob
                and isinstance(d.uri, str)
                and d.uri.startswith('s3://')
            ):
                session = boto3.session.Session(
                    aws_access_key_id=self.user_input.aws_access_key_id,
                    aws_secret_access_key=self.user_input.aws_secret_access_key,
                    region_name=self.user_input.aws_region_name,
                )
                s3_client = session.client('s3')
                bucket_name = d.uri.split('/')[2]
                path_s3 = '/'.join(d.uri.split('/')[3:])
                temp_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': path_s3},
                    ExpiresIn=3600,
                )
                d.uri = temp_url
            return d

        for d in docs['@mc,mcc']:
            _create_temp_link(d)

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
            result = None
            self.logger.info(traceback.format_exc())
        if result:
            return convert_es_to_da(result, get_score_breakdown=False)
        else:
            return DocumentArray()

    @secure_request(on='/count', level=SecurityLevel.USER)
    def count(self, parameters: dict = {}, **kwargs):
        """Count all indexed documents.

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
            result = []
            self.logger.info(traceback.format_exc())
        return DocumentArray([Document(text='count', tags={'count': len(result)})])

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
    def tags(self, **kwargs):
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

                resp = self.es.search(index=self.index_name, body=es_query, size=100)
                self.es.indices.refresh(index=self.index_name)
                ids = [r['_id'] for r in resp['hits']['hits']]
                self.query_to_curated_ids[query] += [
                    id for id in ids if id not in self.query_to_curated_ids[query]
                ]

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
        tag_categories = {
            tag: map
            for tag, map in tag_categories.items()
            if tag in self.user_input.filter_fields
        }
        aggs = {'aggs': {}, 'size': 0}
        for tag, map in tag_categories.items():
            for tag_type, extension in [
                ['text', '.keyword'],
                ['keyword', ''],
                ['float', ''],
            ]:
                if map['type'] == tag_type:
                    aggs['aggs'][tag] = {
                        'terms': {'field': f'tags.{tag}{extension}', 'size': 100}
                    }

        try:
            if not aggs['aggs']:
                return
            result = self.es.search(index=self.index_name, body=aggs)
            aggregations = result['aggregations']
            updated_tags = {}
            for tag, agg in aggregations.items():
                updated_tags[tag] = [bucket['key'] for bucket in agg['buckets']]
            self.doc_id_tags = updated_tags
        except Exception:
            self.logger.info(traceback.format_exc())


def aggregate_embeddings(docs_map: Dict[str, DocumentArray]):
    """Aggregate embeddings of cc level to c level.

    :param docs_map: a dictionary of `DocumentArray`s, where the key is the embedding space aka encoder name.
    """
    for docs in docs_map.values():
        for doc in docs:
            for c in doc.chunks:
                if c.chunks.embeddings is not None:
                    c.embedding = c.chunks.embeddings.mean(axis=0)
                if c.chunks[0].text or not c.uri:
                    c.content = c.chunks[0].content
                c.chunks = DocumentArray()
