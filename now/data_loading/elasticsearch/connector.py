import logging
from typing import Dict, Generator, List, Optional

from elasticsearch import Elasticsearch

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("elastic_transport").setLevel(logging.WARNING)


class ElasticsearchConnector:
    def __init__(
        self,
        connection_str: str = 'http://localhost:9200',
        connection_args: Optional[Dict] = None,
    ):
        """
        Provides an interface to an Elasticsearch database.
        :param connection_str: A connection string for the ES instance. Usually, it
            includes url, port, username, password, etc. Typically, it has the form:
            'https://{user_name}:{password}@{host}:{port}'
        :param connection_args: Dictionary with additional connection arguments,
            e.g., information about certificates
        """
        self._connection_str = connection_str
        self._connection_args = (
            connection_args if connection_args else {'verify_certs': False}
        )
        self.es = Elasticsearch(self._connection_str, **self._connection_args)

    def __enter__(self) -> 'ElasticsearchConnector':
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def get_documents(
        self, index_name: str, page_size: Optional[int] = 10
    ) -> List[Dict]:
        """
        Returns all documents stored in a specific index
        :param index_name: Name of the index in the Elasticsearch database
        :param page_size: To retrieve a large number of documents, multiple request are
            executed, where each request returns a spefic number of documents
            (max. 10,000). The `page_size` refers to this number.
        :return: Documents in the form of a list of dictionaries
        """
        query = {"match_all": {}}
        resp = self.es.search(
            index=index_name, query=query, scroll='2m', size=page_size
        )
        documents = resp['hits']['hits']
        scroll_id = resp['_scroll_id']
        scroll_size = len(documents)
        while scroll_size > 0:
            resp = self.es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = resp['_scroll_id']
            new_documents = resp['hits']['hits']
            scroll_size = len(new_documents)
            documents.extend(new_documents)
        return documents

    def get_documents_by_query(
        self, query: Dict, index_name: str, page_size: Optional[int] = 10
    ) -> Generator[List[Dict], None, None]:
        """
        Executes an Elasticsearch query on a given index and returns a generator which
        yields pages of documents from the query results.
        :param query: Elasticsearch query
        :param index_name: Name of an Elasticsearch index
        :param page_size: Number of documents per page
        :return: Generator which yields one page of documents on each call.
        """
        resp = self.es.search(
            **query, index=index_name, scroll='2m', size=page_size, source=False
        )
        documents = [
            {**doc['_source'], **{'id': doc['_id']}} for doc in resp['hits']['hits']
        ]
        scroll_id = resp['_scroll_id']
        scroll_size = len(documents)
        while scroll_size > 0:
            yield documents
            resp = self.es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = resp['_scroll_id']
            documents = [
                {**doc['_source'], **{'id': doc['_id']}} for doc in resp['hits']['hits']
            ]
            scroll_size = len(documents)

    def close(self) -> None:
        """
        Closes Elasticsearch connection.
        """
        self.es.close()
