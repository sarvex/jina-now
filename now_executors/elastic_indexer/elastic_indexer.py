from typing import Any, Dict, List, Mapping, Optional, Union
import traceback

import numpy as np
from docarray import Document, DocumentArray
from docarray.score import NamedScore
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from now_executors import NOWAuthExecutor as Executor
from now_executors import SecurityLevel, secure_request

metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}


class ElasticIndexer(Executor):
    def __init__(
            self,
            hosts: Union[
                str, List[Union[str, Mapping[str, Union[str, int]]]], None
            ] = 'http://localhost:9200',
            es_config: Optional[Dict[str, Any]] = {},
            metric: str = 'cosine',
            dims: Union[List[int], int] = None,
            index_name: str = 'nest',
            traversal_paths: str = '@r',
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
        """
        super().__init__(**kwargs)

        self.hosts = hosts
        self.metric = metric
        self.index_name = index_name
        self.traversal_paths = traversal_paths
        if isinstance(dims, int):
            self.dims = [dims]
        else:
            self.dims = dims
        if dims:
            self.es_mapping = self._generate_es_mapping()
        else:
            print('Cannot create Elasticsearch mapping. Please specify `dims`')
            raise

        self.es = Elasticsearch(hosts=self.hosts, **es_config, ssl_show_warn=False)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=self.es_mapping)

    def _generate_es_mapping(self) -> Dict:
        es_mapping = {
            'properties': {
                'id': {'type': 'keyword'},
                'text': {'type': 'text', 'analyzer': 'standard'},
            }
        }
        for i, dim in enumerate(self.dims):
            es_mapping['properties'][f'embedding_{i}'] = {
                'type': 'dense_vector',
                'dims': dim,
                'similarity': self.metric,
                'index': 'true',
            }
        return es_mapping

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index(
            self, docs: DocumentArray, parameters: dict = {}, **kwargs
    ) -> DocumentArray:
        """
        Index new `Document`s by adding them to the Elasticsearch index.

        :param docs: Documents to be indexed.
        :param parameters: dictionary with options for indexing.
        :return: empty `DocumentArray`.
        """
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        docs = docs[traversal_paths]

        es_docs = self._transform_docs_to_es(docs)
        try:
            success, _ = bulk(self.es, es_docs, refresh='wait_for')
        except Exception:
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
            self, docs: Union[Document, DocumentArray], parameters: dict = {}, **kwargs
    ):
        """Perform traditional bm25 + vector search. By convention, BM25 will search on
        the 'text' field of the index. For now, this field contains a concatenation of
        all text chunks of the documents.

        :param docs: query `Document`s.
        :param parameters: dictionary of options for searching.
        """
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        limit = parameters.get('limit', 20)
        apply_bm25 = parameters.get('apply_bm25', False)
        docs = docs[traversal_paths]
        for doc in docs:
            query = self._build_es_query(doc, apply_bm25)
            try:
                result = self.es.search(
                    index=self.index_name,
                    query=query,
                    fields=['text'],
                    source=False,
                    size=limit,
                )['hits']['hits']
                doc.matches = self._transform_es_results_to_matches(result)
            except Exception:
                print(traceback.format_exc())
        return docs



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
        limit = int(parameters.get('limit', 20))
        offset = int(parameters.get('offset', 0))
        with_embedding = parameters.get('with_embedding', False)
        try:
            result = self.es.search(
                index=self.index_name, size=limit, query={'match_all': {}}
            )['hits']['hits']
        except Exception:
            print(traceback.format_exc())
        if result:
            result_da = self._transform_es_results_to_da(result, with_embedding)
        else:
            return result
        return result[offset:limit]

    def _build_es_query(
            self,
            query: Document,
            apply_bm25: bool,
    ) -> Dict:
        """
        Build script-score query used in Elasticsearch. To do this, we extract
        embeddings from the query document and pass them in the s   cript-score
        query together with the fields to search on in the Elasticsearch index.

        :param query: a `Document` with chunks containing a text embedding and
            image embedding.
        :param apply_bm25: whether to combine bm25 with vector search. If False,
            will only perform vector search queries. If True, must supply a text
            field for bm25 searching.
        :return: a dictionary containing query and filter.
        """
        # build bm25 part
        if apply_bm25:
            source = '_score / (_score + 10.0)'
            text = query.text
            query_script_score = {
                'bool': {
                    "should": [
                        {
                            "multi_match": {
                                "query": text,
                                "fields": ['text'],
                            }
                        },
                        {"match_all": {}},
                    ],
                },
            }
        else:
            source = ''
            query_script_score = {
                'bool': {
                    "should": [
                        {"match_all": {}},
                    ],
                },
            }
        # build vector search part
        query_embeddings = self._extract_embeddings(doc=query)
        params = {}
        for key, embedding in query_embeddings.items():
            source += (
                f"+ 0.5*{metrics_mapping[self.metric]}(params.query_{key}, '{key}')"
            )
            params[f'query_{key}'] = embedding
        source += '+ 1.0'
        query_json = {
            'script_score': {
                'query': query_script_score,
                'script': {'source': source, 'params': params},
            }
        }
        return query_json

    def _transform_es_to_da(self, result: List[Dict], with_embedding: bool) -> DocumentArray:
        """
        Transform Elasticsearch documents into DocumentArray. Assumes that all Elasticsearch
        documents have a 'text' field.

        :param result: results from an Elasticsearch query.
        :param with_embedding: whether to add embeddings to the final documents.
        :return: a DocumentArray containing all results.
        """
        da = DocumentArray()
        for es_doc in result:
            source = es_doc['_source']
            doc = Document(id=es_doc['_id'], text=source['text'])
            if with_embedding:
                embeddings = [source[key] for key in source.keys() if key.startswith('embedding_')]
                doc.chunks = [Document(embedding=e) for e in embeddings]
            da.append(doc)
        return da

    def _transform_docs_to_es(self, docs: DocumentArray) -> List[Dict]:
        """
        This function takes documents with chunks containing text and embeddings
        and returns a dictionary that can be indexed in Elasticsearch.

        :param docs: documents containing text and image chunks.
        :return: list of dictionaries containing text, text embedding and image embedding
        """
        es_docs = list()
        for doc in docs:
            es_doc = dict()
            es_doc['_id'] = doc.id
            es_doc['text'] = ''
            for i, chunk in enumerate(doc.chunks):
                # concatenate text fields
                if chunk.text:
                    es_doc['text'] += chunk.text + " "
                es_doc[f"embedding_{i}"] = chunk.embedding
            es_doc['_op_type'] = 'index'
            es_doc['_index'] = self.index_name
            es_docs.append(es_doc)
        return es_docs

    def _transform_es_results_to_matches(self, es_results: List[Dict]) -> DocumentArray:
        """
        Transform a list of results from Elasticsearch into a matches in the form of a `DocumentArray`.

        :param es_results: List of dictionaries containing results from Elasticsearch querying.
        :return: `DocumentArray` that holds all matches in the form of `Document`s.
        """
        matches = DocumentArray()
        for result in es_results:
            d = Document(id=result['_id'])
            d.text = result['fields']['text'][0]
            d.scores[self.metric] = NamedScore(value=result['_score'])
            matches.append(d)
        return matches

    def _extract_embeddings(self, doc: Document) -> Dict[str, np.ndarray]:
        """
        Get embeddings from a document's chunks. Currently assumes that each chunk
        has an embedding.

        :param doc: `Document` with chunks of text document and/or image document.
        :return: Embeddings as values in a dictionary, modality specified in key.
        """
        embeddings = {}
        for i, chunk in enumerate(doc.chunks):
            embeddings[f"embedding_{i}"] = chunk.embedding
        if not embeddings:
            print('No embeddings extracted')
            raise
        return embeddings
