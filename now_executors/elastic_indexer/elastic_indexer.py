import traceback
from typing import Any, Dict, List, Mapping, Optional, Union

import numpy as np
from docarray import Document, DocumentArray
from docarray.score import NamedScore
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from now_common.abstract_executors.NOWAuthExecutor.executor import (
    NOWAuthExecutor as Executor,
)
from now_common.abstract_executors.NOWAuthExecutor.executor import (
    SecurityLevel,
    secure_request,
)

metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}


class ElasticIndexer(Executor):
    def __init__(
        self,
        dims: Union[List[int], int],
        hosts: Union[
            str, List[Union[str, Mapping[str, Union[str, int]]]], None
        ] = 'https://elastic:elastic@localhost:9200',
        es_config: Optional[Dict[str, Any]] = None,
        metric: str = 'cosine',
        index_name: str = 'now-index',
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
        self.es_config = {'verify_certs': False} if not es_config else es_config
        self.dims = dims if isinstance(dims, list) else [dims]
        self.es_mapping = self._generate_es_mapping(dims)
        self.es = Elasticsearch(hosts=self.hosts, **self.es_config, ssl_show_warn=False)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=self.es_mapping)

    def _generate_es_mapping(self, dims: List[int]) -> Dict:
        es_mapping = {
            'properties': {
                'id': {'type': 'keyword'},
                'bm25_text': {'type': 'text', 'analyzer': 'standard'},
            }
        }
        if isinstance(dims, list) and 'c' in self.traversal_paths:
            for i, dim in enumerate(self.dims):
                es_mapping['properties'][f'chunk_{i}'] = {
                    'properties': {
                        f'embedding': {
                            'type': 'dense_vector',
                            'dims': dim,
                            'similarity': self.metric,
                            'index': 'true',
                        }
                    }
                }
        elif isinstance(dims, int) and 'r' in self.traversal_paths:
            es_mapping['properties']['embedding'] = {
                'type': 'dense_vector',
                'dims': dims,
                'similarity': self.metric,
                'index': 'true',
            }
        else:
            raise ValueError(
                f'Invalid combination of traversal_paths {self.traversal_paths} '
                f'and dims {self.dims}'
            )
        return es_mapping

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index(
        self, docs: DocumentArray, parameters: dict = None, **kwargs
    ) -> DocumentArray:
        """
        Index new `Document`s by adding them to the Elasticsearch index.

        :param docs: Documents to be indexed.
        :param parameters: dictionary with options for indexing.
        :return: empty `DocumentArray`.
        """
        if not docs:
            return
        if not parameters:
            parameters = {}
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        es_docs = self._transform_docs_to_es(docs, traversal_paths)
        try:
            success, _ = bulk(self.es, es_docs)
            self.es.indices.refresh(index=self.index_name)
        except Exception as e:
            print(e)
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
        if not docs:
            return
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        search_filter = parameters.get('filter', None)
        limit = parameters.get('limit', 20)
        apply_bm25 = parameters.get('apply_bm25', False)
        for doc in docs:
            query = self._build_es_query(
                doc=doc,
                apply_bm25=apply_bm25,
                traversal_paths=traversal_paths,
                search_filter=search_filter,
            )
            try:
                result = self.es.search(
                    index=self.index_name,
                    query=query,
                    source=True,
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
        try:
            result = self.es.search(index=self.index_name, query={'match_all': {}})[
                'hits'
            ]['hits']
        except Exception:
            print(traceback.format_exc())
        if result:
            result_da = self._transform_es_to_da(result)
            return result_da[offset : offset + limit]
        else:
            return result

    @secure_request(on='/delete', level=SecurityLevel.USER)
    def delete(self, parameters: dict = {}, **kwargs):
        """
        Delete endpoint to delete document/documents from the index.

        :param parameters: dictionary with filter conditions to select
            documents for deletion.
        """
        search_filter = parameters.get('filter', None)
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
            resp = self.es.delete_by_query(index=self.index_name, body=es_search_filter)
            self.es.indices.refresh(index=self.index_name)
        except Exception:
            print(traceback.format_exc())
            raise
        if resp:
            print(
                f"Deleted {resp['deleted']} documents in Elasticsearch index {self.index_name}"
            )
        return DocumentArray()

    def _build_es_query(
        self,
        doc: Document,
        apply_bm25: bool,
        traversal_paths: str,
        search_filter: Optional[Dict] = None,
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
        :param traversal_paths: traversal paths for the query document.
        :param search_filter: dictionary of filters to apply to the search.
        :return: a dictionary containing query and filter.
        """
        source = ''
        query = {
            'bool': {
                'should': [
                    {'match_all': {}},
                ],
            },
        }

        # build bm25 part
        if apply_bm25:
            source += '_score / (_score + 10.0)'
            text = doc.text
            multi_match = {'multi_match': {'query': text, 'fields': ['bm25_text']}}
            query['bool']['should'].append(multi_match)

        # add filter
        if search_filter:
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

        # build vector search part
        query_embeddings = self._extract_embeddings(
            doc=doc, traversal_paths=traversal_paths
        )
        params = {}
        for key, embedding in query_embeddings.items():
            if key == 'embedding':
                source += (
                    f"+ 0.5*{metrics_mapping[self.metric]}(params.query_{key}, '{key}')"
                )
            else:
                source += f"+ 0.5*{metrics_mapping[self.metric]}(params.query_{key}, '{key}.embedding')"
            params[f'query_{key}'] = embedding
        source += '+ 1.0'
        query_json = {
            'script_score': {
                'query': query,
                'script': {'source': source, 'params': params},
            }
        }
        return query_json

    def _transform_es_to_da(self, result: Union[Dict, List[Dict]]) -> DocumentArray:
        """
        Transform Elasticsearch documents into DocumentArray. Assumes that all Elasticsearch
        documents have a 'text' field. It does not return embeddings as part of the Document.

        :param result: results from an Elasticsearch query.
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
                elif k.startswith('embedding'):
                    continue
                elif k in ['bm25_text', '_score']:
                    continue
                else:
                    doc.k = v
            da.append(doc)
        return da

    def _transform_docs_to_es(
        self, docs: DocumentArray, traversal_paths: str
    ) -> List[Dict]:
        """
        This function takes Documents as input and transforms them into a list of
        dictionaries that can be indexed in Elasticsearch.

        :param docs: documents containing text and image chunks.
        :param traversal_paths: traversal paths to extract chunks from documents.
        :return: list of dictionaries containing text, text embedding and image embedding
        """
        es_docs = list()
        for doc in docs:
            es_doc = {k: v for k, v in doc.to_dict().items() if v}
            es_doc['_id'] = doc.id
            es_doc['bm25_text'] = doc.text
            chunks = es_doc.pop('chunks', None)
            if chunks and 'c' in traversal_paths:
                for i, chunk in enumerate(chunks):
                    es_doc[f'chunk_{i}'] = {k: v for k, v in chunk.items() if v}
                    if chunk['text']:
                        es_doc['bm25_text'] += " " + chunk['text']
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
            d = self._transform_es_to_da(result)[0]
            d.scores[self.metric] = NamedScore(value=result['_score'])
            matches.append(d)
        return matches

    def _extract_embeddings(
        self, doc: Document, traversal_paths: str
    ) -> Dict[str, np.ndarray]:
        """
        Get embeddings from a documents.

        :param doc: `Document` with chunks of text document and/or image document.
        :param traversal_paths: traversal paths to extract embeddings from documents.
        :return: Embeddings as values in a dictionary, modality specified in key.
        """
        embeddings = {}
        if 'r' in traversal_paths:
            embeddings['embedding'] = doc.embedding
        if 'c' in traversal_paths:
            for i, chunk in enumerate(doc.chunks):
                embeddings[f"chunk_{i}"] = chunk.embedding
        if not embeddings:
            print('No embeddings extracted')
            raise
        return embeddings
