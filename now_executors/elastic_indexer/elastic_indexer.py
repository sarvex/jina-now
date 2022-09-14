import warnings
from typing import Any, Dict, List, Mapping, Optional, Union

import numpy as np
from docarray import Document, DocumentArray
from docarray.score import NamedScore
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from jina import Executor, requests

warnings.filterwarnings('ignore', category=DeprecationWarning)
metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}

MAPPING = {
    'properties': {
        'id': {'type': 'text', 'analyzer': 'standard'},
        'text': {'type': 'text', 'analyzer': 'standard'},
        'text_embedding': {
            'type': 'dense_vector',
            'dims': 768,
            'similarity': 'cosine',
            'index': 'true',
        },
    }
}


class ElasticIndexer(Executor):
    def __init__(
        self,
        hosts: Union[
            str, List[Union[str, Mapping[str, Union[str, int]]]], None
        ] = 'http://localhost:9200',
        es_config: Optional[Dict[str, Any]] = {},
        metric: str = 'cosine',
        index_name: str = 'nestxxx',
        es_mapping: Optional[Dict] = MAPPING,
        **kwargs,
    ):
        """
        Initializer function for the ElasticIndexer

        :param hosts: host configuration of the Elasticsearch node or cluster
        :param es_config: Elasticsearch cluster configuration object
        :param metric: The distance metric used for the vector index and vector search
        :param index_name: ElasticSearch Index name used for the storage
        :param es_mapping: Mapping for new index.
        """
        super().__init__(**kwargs)

        self.hosts = hosts
        self.metric = metric
        self.index_name = index_name
        self.es_mapping = es_mapping

        self.es = Elasticsearch(hosts=self.hosts, **es_config, ssl_show_warn=False)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=self.es_mapping)

    @requests(on='/index')
    def index(self, docs: DocumentArray, **kwargs) -> DocumentArray:
        """
        Index new `Document`s by adding them to the Elasticsearch index.

        :param docs: Documents to be indexed.
        :return: empty `DocumentArray`.
        """
        es_docs = self._transform_docs_to_es(docs)
        success, _ = bulk(self.es, es_docs, refresh='wait_for')
        if success:
            print(
                f'Inserted {success} documents into Elasticsearch index {self.index_name}'
            )
        return (
            DocumentArray()
        )  # prevent sending the data back by returning an empty DocumentArray

    @requests(on='/search')
    def search(
        self, docs: Union[Document, DocumentArray], limit: Optional[int] = 20, **kwargs
    ):
        """Perform traditional bm25 + vector search.

        :param docs: query `Document`s.
        :param limit: return top `limit` results.
        """
        for doc in docs:
            query = self._build_es_query(doc)
            result = self.es.search(
                index=self.index_name,
                query=query,
                fields=['text'],
                source=False,
                size=limit,
            )['hits']['hits']
            doc.matches = self._transform_es_results_to_matches(result)
        return docs

    def _build_es_query(
        self,
        query: Document,
    ) -> Dict:
        """
        Build script-score query used in Elasticsearch. To do this, we extract
        embeddings from the query document and pass them in the script-score
        query together with the fields to search on in the Elasticsearch index.

        :param query: a `Document` with chunks containing a text embedding and
            image embedding.
        :return: a dictionary containing query and filter.
        """
        query_embeddings = self._extract_embeddings(doc=query)
        source = '_score / (_score + 10.0)'
        params = {}
        # if query has embeddings then search using hybrid search, otherwise only bm25
        if query_embeddings:
            for k, v in query_embeddings.items():
                source += (
                    f"+ 0.5*{metrics_mapping[self.metric]}(params.query_{k}, '{k}')"
                )
                params[f'query_{k}'] = v
            source += '+ 1.0'
        query_json = {
            'script_score': {
                'query': {
                    'bool': {},
                },
                'script': {'source': source, 'params': params},
            }
        }
        return query_json

    @requests(on='/update')
    def update(self, docs: DocumentArray, **kwargs) -> DocumentArray:
        """
        TODO: implement update endpoint, eg. update ES docs with new embeddings etc.
        """
        raise NotImplementedError()

    @requests(on='/filter')
    def filter(self, parameters: dict = {}, **kwargs):
        """
        TODO: implement filter query for Elasticsearch

        :returns: filtered results in root, chunks and matches level
        """
        raise NotImplementedError()

    def _transform_docs_to_es(self, docs: DocumentArray) -> List[Dict]:
        """
        This function takes a `DocumentArray` containing `Document`s
        with text and image chunks and returns a dictionary with text,
        embedding and image embedding.

        :param doc: A `Document` containing text and image chunks.
        :return: A list of dictionaries containing text, text embedding and image embedding
        """
        new_docs = list()
        for doc in docs:
            new_doc = dict()
            new_doc['_id'] = doc.id
            for chunk in doc.chunks:
                if chunk.modality == 'text':
                    new_doc['text'] = chunk.text
                    new_doc['text_embedding'] = chunk.embedding
                elif chunk.modality == 'image':
                    new_doc['image_embedding'] = chunk.embedding
            new_doc['_op_type'] = 'index'
            new_doc['_index'] = self.index_name
            new_docs.append(new_doc)
        return new_docs

    def _transform_es_results_to_matches(self, es_results: List[Dict]) -> DocumentArray:
        """
        Transform a list of results from Elasticsearch into a matches in the form of a `DocumentArray`.

        :param es_results: List of dictionaries containing results from Elasticsearch querying.
        :return: `DocumentArray` that holds all matches in the form of `Document`s.
        """
        matches = DocumentArray()
        for result in es_results:
            d = Document(id=result['_id'])
            d.scores[self.metric] = NamedScore(value=result['_score'])
            matches.append(d)
        return matches

    def _extract_embeddings(self, doc: Document) -> Dict[str, np.ndarray]:
        """
        Get embeddings from the document. Currently supports at most two
        modalities and two embeddings (text and image).

        :param doc: `Document` with chunks of text document and/or image document.
        :return: Embeddings as values in a dictionary, modality specified in key.
        """
        embeddings = {}
        for c in doc.chunks:
            if c.modality == 'text':
                embeddings['text_embedding'] = c.embedding
            elif c.modality == 'image':
                embeddings['image_embedding'] = c.embedding
            else:
                print('Modality not found')
                raise
        if not embeddings:
            print('No embeddings extracted')
            raise
        return embeddings
