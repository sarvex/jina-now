import logging
from typing import Dict, Optional, Type, Union

from docarray import Document, DocumentArray

from now.data_loading.elasticsearch.connector import ElasticsearchConnector
from now.now_dataclasses import UserInput

logging.getLogger("PIL.Image").setLevel(logging.CRITICAL + 1)

ID_TAG = 'id'
FIELD_TAG = 'field_name'
EXTRACTION_TYPE_TAG = 'extraction_type'


class ElasticsearchExtractor:
    def __init__(
        self,
        query: Dict,
        index: str,
        user_input: UserInput,
        connection_str: str,
        data_class: Type = None,
        connection_args: Optional[Dict] = None,
    ):
        """
        For extracting documents from Elasticsearch into a `docarray.DocumentArray`
        dataset, this class implements an iterator which yields `docarray.Document`
        objects. To specify the data for extraction, one needs to provide an
        es query together with the index name and parameters to connect to
        the Elasticsearch instance.
        :param query: Elasticsearch query in the form of a JSON string
        :param index: Name of the ES index containing the documents to be extracted
        :param connection_str: A connection string for the ES instance. Usually, it
            includes url, port, username, password, etc. Typically, it has the form:
            'https://{user_name}:{password}@{host}:{port}'
        :param connection_args: Dictionary with additional connection arguments,
            e.g., information about certificates
        """
        self._es_connector = ElasticsearchConnector(
            connection_str=connection_str,
            connection_args=(connection_args if connection_args else {}),
        )
        self._query = query
        self._index = index
        self._user_input = user_input
        self._data_class = data_class
        self._document_cache = []
        self._query_result = self._es_connector.get_documents_by_query(
            self._query, self._index
        )

    def extract(self) -> DocumentArray:
        return DocumentArray([doc for doc in self._extract_documents()])

    def _extract_documents(self):
        try:
            next_doc = self._get_next_document()
            while next_doc:
                yield next_doc
                next_doc = self._get_next_document()
        except StopIteration:
            self._es_connector.close()
            return

    def _get_next_document(self) -> Union[Document, None]:
        """
        Returns the next document from the Elasticsearch database.
        In order to retrieve further documents, Elasticsearch documents are retrieved
        in pages of multiple documents. After retrieving a page, its contained
        documents are stored in a document cache. If documents are left in the cache
        this function returns one of those documents. Otherwise, the next page is
        queried. If there is no page left, None will be returned.
        :return: extracted document
        """
        if len(self._document_cache) == 0:
            self._document_cache = next(self._query_result)
            if len(self._document_cache) == 0:
                return None
        return self._construct_document(self._document_cache.pop())

    def _construct_document(self, es_document: Dict) -> Document:
        """
        Constructs a `docarray.Document` object from an Elasticsearch document.

        :param es_document: Elasticsearch document

        :return: `docarray.Document` object

        Creates a document using the dataclass specified in the user input.
        """
        kwargs = {}
        for field_name, field_value in es_document.items():
            if (
                field_name
                in self._user_input.index_fields + self._user_input.filter_fields
            ):
                kwargs[
                    self._user_input.field_names_to_dataclass_fields[field_name]
                ] = field_value
                continue

        return Document(self._data_class(**kwargs))
