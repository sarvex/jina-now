import warnings
from typing import Any, Dict, List, Mapping, Optional, Union

from docarray import Document, DocumentArray, dataclass
from docarray.typing import Image, Text
from jina import Executor, Flow, requests

warnings.filterwarnings("ignore", category=DeprecationWarning)
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
        n_dim: int = 128,
        distance: str = 'cosine',
        index_name: str = 'nest',
        es_config: Optional[Dict[str, Any]] = None,
        tag_indices: Optional[List[str]] = None,
        batch_size: int = 64,
        ef_construction: Optional[int] = None,
        m: Optional[int] = None,
        **kwargs,
    ):
        """
        Initializer function for the ElasticIndexer

        :param hosts: host configuration of the ElasticSearch node or cluster
        :param n_dim: number of dimensions
        :param distance: The distance metric used for the vector index and vector search
        :param index_name: ElasticSearch Index name used for the storage
        :param es_config: ElasticSearch cluster configuration object
        :param index_text: If set to True, ElasticSearch will index the text attribute of each Document to allow text
            search
        :param tag_indices: Tag fields to be indexed in ElasticSearch to allow text search on them.
        :param batch_size: Batch size used to handle storage refreshes/updates.
        :param ef_construction: The size of the dynamic list for the nearest neighbors. Defaults to the default
            `ef_construction` value in ElasticSearch
        :param m: The maximum number of connections per element in all layers. Defaults to the default
            `m` in ElasticSearch.
        """
        super().__init__(**kwargs)
        self.distance = distance
        self.index = DocumentArray()

    @requests(on="/index")
    def index(self, docs: DocumentArray, **kwargs):
        docs.summary()
        self.index.extend(docs)
        return (
            DocumentArray()
        )  # prevent sending the data back by returning an empty DocumentArray

    @requests(on="/search")
    def search(self, docs: DocumentArray, limit: Optional[int] = 20, **kwargs):
        """Perform traditional bm25 + vector search.

        :param docs: query `Document`s.
        :param limit: return top `limit` results.
        """
        result_da = DocumentArray()
        for doc in docs:
            text_encoded_query = doc  # encoded with sbert
            image_encoded_query = doc  # encoded with clip-text
            query = self._build_es_query(
                DocumentArray([text_encoded_query, image_encoded_query])
            )
            results = self.index.find(query=query, limit=limit)
            result_da.append(results)
        return result_da

    def _build_es_query(
        self,
        query: DocumentArray,
    ) -> Dict:
        """
        Build script-score query used in Elasticsearch.

        :param query: two `Document`s in this `DocumentArray`, one with the query encoded with
            text encoder and another with the query encoded with clip-text encoder.
        :param mode: support two modes: bm25 / vector. Different query dict template will
        be used in different modes.
        :param filter_ids: only perform search on docs within these ids.
        :return: query dict containing query and filter.
        """

        query_json = {
            "script_score": {
                "query": {
                    "bool": {},
                },
                "script": {
                    "source": f"""_score / (_score + 10.0)
                            + 0.5*{metrics_mapping[self.distance]}(params.query_ImageVector, 'image')
                            + 0.5*{metrics_mapping[self.distance]}(params.query_TextVector, 'text')
                            + 1.0""",
                    "params": {
                        "query_TextVector": query[0].embedding,
                        "query_ImageVector": query[1].embedding,
                    },
                },
            }
        }
        return query_json

    @requests(on="/filter")
    def filter(self, parameters: dict = {}, **kwargs):
        """
        /filter endpoint, filters through documents if docs is passed using some
        filtering conditions e.g. {"codition1":value1, "condition2": value2}
        in case of multiple conditions "and" is used

        :returns: filtered results in root, chunks and matches level
        """
        filtering_condition = parameters.get("filter", {})
        traversal_paths = parameters.get("traversal_paths", self.traversal_paths)
        result = self.index[traversal_paths].find(filtering_condition)
        return result


if __name__ == "__main__":

    @dataclass
    class ESDocument:
        text: Text
        image: Image
        bm25_text: Text

    @dataclass
    class ESQuery:
        text: Text

    with Flow().add(uses=ElasticIndexer) as f:
        f.index(
            DocumentArray(
                [
                    Document(
                        ESDocument(
                            text='this is a flower',
                            image='https://t3.ftcdn.net/jpg/03/31/21/08/240_F_331210846_9yjYz8hRqqvezWIIIcr1sL8UB4zyhyQg.jpg',
                            bm25_text='this is a flower and some other stuff',
                        )
                    ),
                    Document(
                        ESDocument(
                            text='i have a cat',
                            image='https://t3.ftcdn.net/jpg/03/31/21/08/240_F_331210846_9yjYz8hRqqvezWIIIcr1sL8UB4zyhyQg.jpg',
                            bm25_text='i have a cat and some other stuff',
                        )
                    ),
                ],
                storage='elasticsearch',
                config={'index_name': 'nest_index', 'n_dim': 5},
                subindex_configs={
                    '@.[text]': {'n_dim': 512},
                    '@.[image]': {'n_dim': 786},
                    '@.[bm25_text]': None,
                },
            )
        )

        x = f.search(Document(ESQuery(text='flower')))
        print(x)
