import os
from typing import Dict, List, Optional, Union, Set

from docarray import Document, DocumentArray
from PIL import Image

from .connector import ElasticsearchConnector

import logging

logging.getLogger("PIL.Image").setLevel(logging.CRITICAL + 1)


ID_TAG = 'id'
FIELD_TAG = 'field_name'
EXTRACTION_TYPE_TAG = 'extraction_type'


class ElasticsearchExtractor:
    def __init__(
        self,
        query: Dict,
        index: str,
        connection_str: str,
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
        self._document_cache = []
        self._query_result = self._es_connector.get_documents_by_query(
            self._query, self._index
        )
        self._supported_pil_extensions = self._get_supported_image_extensions()

    def extract(self) -> DocumentArray:
        """
        Returns extracted data as a `DocumentArray` where every `Document`
        contains chunks for each field.
        For Example:
        Document(chunks=[
            Document(content='hello', modality='text', tags={'field_name': 'title'}),
            Document(content='https://bla.com/img.jpeg', modality='image', tags={'field_name': 'uris'}),
            ]
        )
        """
        return DocumentArray(
            [self._transform_es_doc(doc) for doc in self._extract_documents()]
        )

    def _extract_documents(self):
        try:
            next_doc = self._get_next_document()
            while next_doc:
                yield next_doc
                next_doc = self._get_next_document()
        except StopIteration:
            self._es_connector.close()
            return

    @classmethod
    def _transform_es_doc(cls, document: Document) -> Document:
        """
        Transform data extracted from Elasticsearch to a more convenient form.
        :param document: `Document` containing ES data.
        :return: Transformed `Document`.
        """
        attr_values, attr_modalities = {}, {}
        cls._parse_es_doc(document, attr_values, attr_modalities, [])
        transformed_doc = Document(
            chunks=[
                Document(
                    content=attr_values[name],
                    modality=attr_modalities[name],
                    tags={'field_name': name},
                )
                for name in attr_values
            ]
        )
        return transformed_doc

    @classmethod
    def _parse_es_doc(
        cls,
        document: Document,
        attr_values: Dict,
        attr_modalities: Dict,
        names: List[str],
    ):
        """
        Extract attributes and modalities from a `Document` and store it as a dictionary.
        Recursively iterates across different chunks of the `Document` and collects
        attributes with their corresponding values.
        :param document: `Document` we want to transform.
        :param attr_values: Dictionary of attribute values extracted from the document.
        :param attr_modalities: Dictionary of attribute modalities extracted from the document.
        :param names: Name of an attribute (attribute names may be nested, e.g.
            info.cars, and we need to store name(s) on every level of recursion).
        """
        if not document.chunks:
            names.append(document.tags['field_name'])
            attr_name = '.'.join(names)
            attr_val = document.text if document.modality == 'text' else document.uri
            if attr_name not in attr_modalities:
                attr_modalities[attr_name] = document.modality
                attr_values[attr_name] = []
            attr_values[attr_name].append(attr_val)
        else:
            if 'field_name' in document.tags:
                names.append(document.tags['field_name'])
            for doc in document.chunks:
                cls._parse_es_doc(doc, attr_values, attr_modalities, names[:])

    def _get_next_document(self) -> Union[Document, None]:
        """
        Returns the next document from the Elasticsearch database.
        In order to retrieve further documents, Elasticsearch documents are retrieved
        in pages of multiple documents. After retrieving a page, its contained
        documents are stored in a document cache. If documents are left in the cache
        this function returns of of those documents. Otherwise, the next page is
        queried. If there is no page left, None will be returned.
        :return: extracted document
        """
        if len(self._document_cache) == 0:
            self._document_cache = next(self._query_result)
            if len(self._document_cache) == 0:
                return None
        return self._construct_document(self._document_cache.pop())

    def _construct_simple_document(self, key, content) -> Document:
        """
        Transforms the values of a filed of an Elasticsearch document into a
        `docarray.Document` object (without chunks).
        The name of the field where the attribute is extracted from is added as a tag
        to the document.
        :param key: the name of the field which stores the attribute in the
            Elasticsearch database
        :param content: the content of the attribute
        :return: `docarray.DocumentArray` object.
        """
        if os.path.splitext(content)[-1] in self._supported_pil_extensions:
            return Document(
                uri=content,
                modality='image',
                tags={
                    FIELD_TAG: key,
                    EXTRACTION_TYPE_TAG: 'literal',
                },
            )
        else:
            return Document(
                text=content,
                modality='text',
                tags={
                    FIELD_TAG: key,
                    EXTRACTION_TYPE_TAG: 'literal',
                },
            )

    def _construct_document(self, es_document: Dict) -> Document:
        """
        Constructs a `docarray.Document` from an Elasticsearch document returned by the
        `ElasticsearchConnector.get_documents_by_query` function.
        :param es_document: dictionary containing the content of an Elasticsearch
            document
        :return: A `docarray.Document` object where each chunk represents an element of
            a field from an Elasticsearch document.
        """
        root = (es_document, Document())
        frontier = [root]
        while len(frontier) > 0:
            new_frontier = []
            for es_doc, da_doc in frontier:
                for key in es_doc.keys():
                    new_doc = None
                    if key == 'id':
                        # process the id key value pair
                        da_doc.tags[ID_TAG] = es_doc[key]
                        da_doc.tags[EXTRACTION_TYPE_TAG] = 'root'
                    elif type(es_doc[key][0]) == dict:
                        # process a nested field
                        new_doc = Document(
                            tags={FIELD_TAG: key, EXTRACTION_TYPE_TAG: 'record'}
                        )
                        new_frontier.extend(
                            [(record, new_doc) for record in es_doc[key]]
                        )
                    elif type(es_doc[key][0]) == str:
                        # process a field with a list of string values
                        deep_chunks = [
                            self._construct_simple_document(key, content)
                            for content in es_doc[key]
                        ]
                        new_doc = Document(
                            chunks=deep_chunks, tags={EXTRACTION_TYPE_TAG: 'list'}
                        )
                    else:
                        raise RuntimeError('Unsupported type: ', type(es_doc[key][0]))
                    if new_doc is not None:
                        da_doc.chunks.append(new_doc)
            frontier = new_frontier
        return root[1]

    @staticmethod
    def _get_supported_image_extensions() -> Set[str]:
        extensions = Image.registered_extensions()
        return {ex for ex, f in extensions.items() if f in Image.OPEN}
