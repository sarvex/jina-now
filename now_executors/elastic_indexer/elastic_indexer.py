import warnings
from typing import Dict, List, Optional, Union

import numpy as np
from docarray import Document, DocumentArray
from docarray.score import NamedScore
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from jina import Executor, Flow, requests

warnings.filterwarnings("ignore", category=DeprecationWarning)
metrics_mapping = {
    'cosine': 'cosineSimilarity',
    'l2_norm': 'l2norm',
}

MAPPING = {
    "properties": {
        "id": {"type": "text", "analyzer": "standard"},
        "text": {"type": "text", "analyzer": "standard"},
        "text_embedding": {
            "type": "dense_vector",
            "dims": 768,
            "similarity": "cosine",
            "index": "true",
        },
        "image_embedding": {
            "type": "dense_vector",
            "dims": 512,
            "similarity": "cosine",
            "index": "true",
        },
    }
}


class ElasticIndexer(Executor):
    def __init__(
        self,
        es_connection_str: Optional[str] = 'http://localhost:9200',
        distance: str = 'cosine',
        index_name: str = 'nest',
        es_mapping: Optional[Dict] = None,
        **kwargs,
    ):
        """
        Initializer function for the ElasticIndexer

        :param es_connection_str: host configuration of the ElasticSearch node or cluster
        :param distance: The distance metric used for the vector index and vector search
        :param index_name: ElasticSearch Index name used for the storage
        :param es_mapping: Mapping for new index.
        """
        super().__init__(**kwargs)

        self.es_connection_str = es_connection_str
        self.distance = distance
        self.index_name = index_name
        if not es_mapping:
            self.es_mapping = MAPPING
        else:
            self.es_mapping = es_mapping

        self.es = Elasticsearch(es_connection_str, verify_certs=False)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, mappings=self.es_mapping)

    @requests(on="/index")
    def index(self, docs: DocumentArray, **kwargs) -> DocumentArray:
        """
        Index new `Document`s by adding them to the Elasticsearch indeex.

        :param docs: Documents to be indexed.
        :return: empty `DocumentArray`.
        """
        es_docs = self._transform_docs_to_es(docs)
        bulk(self.es, es_docs, refresh="wait_for")
        return (
            DocumentArray()
        )  # prevent sending the data back by returning an empty DocumentArray

    @requests(on="/search")
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
            )["hits"]["hits"]
            doc.matches = self._transform_es_results_to_matches(result)
        return docs

    def _build_es_query(
        self,
        query: DocumentArray,
    ) -> Dict:
        """
        Build script-score query used in Elasticsearch.

        :param query: two `Document`s in this `DocumentArray`, one with the query encoded with
            text encoder and another with the query encoded with clip-text encoder.
        :return: query dict containing query and filter.
        """
        query_embeddings = self._extract_embeddings(doc=query)
        query_json = {
            "script_score": {
                "query": {
                    "bool": {},
                },
                "script": {
                    "source": f"""_score / (_score + 10.0)
                            + 0.5*{metrics_mapping[self.distance]}(params.query_ImageVector, 'image_embedding')
                            + 0.5*{metrics_mapping[self.distance]}(params.query_TextVector, 'text_embedding')
                            + 1.0""",
                    "params": {
                        "query_TextVector": query_embeddings['query_TextEmbedding'],
                        "query_ImageVector": query_embeddings['query_ImageEmbedding'],
                    },
                },
            }
        }
        return query_json

    @requests(on="/update")
    def update(self, docs: DocumentArray, **kwargs) -> DocumentArray:
        """
        TODO: implement update endpoint, eg. update ES docs with new embeddings etc.
        """
        raise NotImplementedError()

    @requests(on="/filter")
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
                if chunk.modality == "text":
                    new_doc['text'] = chunk.text
                    new_doc['text_embedding'] = chunk.embedding
                elif chunk.modality == "image":
                    new_doc['image_embedding'] = chunk.embedding
            new_doc["_op_type"] = "index"
            new_doc["_index"] = self.index_name
            if all(
                field in new_doc
                for field in ("text", "text_embedding", "image_embedding")
            ):
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
            d.scores[self.distance] = NamedScore(value=result['_score'])
            matches.append(d)
        return matches

    def _extract_embeddings(self, doc: Document) -> Dict[str, np.ndarray]:
        """
        Get embeddings from the document.

        :param doc: `Document` with chunks of text document and image document
        :return: Two embeddings in a dictionary, one for image and one for text
        """
        embeddings = {}
        for c in doc.chunks:
            if c.modality == 'text':
                embeddings['query_TextEmbedding'] = c.embedding
            elif c.modality == 'image':
                embeddings['query_ImageEmbedding'] = c.embedding
            else:
                print("Modality not found")
                raise
        if embeddings and len(embeddings) == 2:
            return embeddings
        else:
            print("No/not all embeddings extracted")
            raise


if __name__ == "__main__":
    with Flow().add(uses=ElasticIndexer) as f:
        f.index(
            DocumentArray(
                [
                    Document(
                        id='123',
                        chunks=[
                            Document(
                                id='x',
                                text='this is a flower',
                                embedding=np.random.rand(768),
                                modality='text',
                            ),
                            Document(
                                id='xx',
                                uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                                embedding=np.random.rand(512),
                                modality='image',
                            ),
                        ],
                    ),
                    Document(
                        id='456',
                        chunks=[
                            Document(
                                id='xxx',
                                text='this is a cat',
                                embedding=np.random.rand(768),
                                modality='text',
                            ),
                            Document(
                                id='xxxx',
                                uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                                embedding=np.random.rand(512),
                                modality='image',
                            ),
                        ],
                    ),
                ]
            )
        )

        x = f.search(
            DocumentArray(
                [
                    Document(
                        chunks=[
                            Document(
                                text='this is a flower',
                                embedding=np.random.rand(768),
                                modality='text',
                            ),
                            Document(
                                uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                                embedding=np.random.rand(512),
                                modality='image',
                            ),
                        ]
                    )
                ]
            )
        )
        x.summary()
        x[0].matches.summary()
