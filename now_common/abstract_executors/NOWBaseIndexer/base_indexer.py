from collections import defaultdict
from copy import deepcopy
from sys import maxsize
from typing import List, Optional

from docarray import Document, DocumentArray
from now_common.abstract_executors.NOWAuthExecutor.executor import (
    NOWAuthExecutor as Executor,
)
from now_common.abstract_executors.NOWAuthExecutor.executor import (
    SecurityLevel,
    secure_request,
)
from now_common.abstract_executors.NOWBaseIndexer.ranking import merge_matches_sum


class NOWBaseIndexer(Executor):
    def __init__(
        self,
        dim: int,
        columns: Optional[List] = None,
        metric: str = 'cosine',
        limit: int = 10,
        traversal_paths: str = '@r',
        max_values_per_tag: int = 10,
        *args,
        **kwargs,
    ):
        """
        :param dim: Dimensionality of vectors to index
        :param columns: List of tuples of the form (column_name, str_type). Here str_type must be a string that can be
        parsed as a valid Python type.
        :param metric: Distance metric type. Can be 'euclidean', 'inner_product', or 'cosine'
        :param limit: Number of results to get for each query document in search
        :param traversal_paths: Default traversal paths on docs
        :param max_values_per_tag: Maximum number of values per tag
        (used for search), e.g. '@r', '@c', '@r,c'
        """

        super().__init__(*args, **kwargs)
        self.columns = self.parse_columns(columns)
        self.dim = dim
        self.metric = metric
        self.limit = limit
        self.traversal_paths = traversal_paths
        self.max_values_per_tag = max_values_per_tag
        self.construct()
        self.doc_id_tags = {}
        self.document_list = DocumentArray()
        self.load_document_list()

    @secure_request(on='/tags', level=SecurityLevel.USER)
    def get_tags_and_values(self, **kwargs):
        """Returns tags and their possible values

        for example if indexed docs are the following:
            docs = DocumentArray([
                Document(.., tags={'color':'red'}),
                Document(.., tags={'color':'blue'}),
                Document(.., tags={'greeting':'hello'}),
            ])

        the resulting response would be a document array with
        one document containg a dictionary in tags like the following:
        {'tags':{'color':['red', 'blue'], 'greeting':['hello']}}
        """

        count_dict = defaultdict(lambda: defaultdict(int))
        for tags in self.doc_id_tags.values():
            for key, value in tags.items():
                count_dict[key][value] += 1

        tag_to_values = dict()
        for key, value_to_count in count_dict.items():
            sorted_values = sorted(
                value_to_count.items(), reverse=True, key=lambda item: item[1]
            )
            tag_to_values[key] = [
                value
                for (value, _), _ in zip(sorted_values, range(self.max_values_per_tag))
            ]
        return DocumentArray([Document(text='tags', tags={'tags': tag_to_values})])

    @secure_request(on='/list', level=SecurityLevel.USER)
    def list(self, parameters: dict = {}, **kwargs):
        """List all indexed documents.
        :param parameters: dictionary with limit and offset
        - offset (int): number of documents to skip
        - limit (int): number of retrieved documents
        """
        limit = int(parameters.get('limit', maxsize))
        offset = int(parameters.get('offset', 0))
        # add removal of duplicates
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        if traversal_paths == '@c':
            docs = DocumentArray()
            chunks_size = int(parameters.get('chunks_size', 3))
            parent_ids = set()
            for d in self.document_list[offset * chunks_size :]:
                if len(parent_ids) == limit:
                    break
                if d.parent_id in parent_ids:
                    continue
                parent_ids.add(d.parent_id)
                docs.append(d)
        else:
            docs = self.document_list[offset : offset + limit]
        return docs

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index_endpoint(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        """Base function for indexing documents. Handles the data management for the index and list endpoints.

        :param docs: the Documents to index
        :param parameters: dictionary with options for indexing
        """
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        flat_docs = docs[traversal_paths]
        if len(flat_docs) == 0:
            return
        self.index(flat_docs, parameters, **kwargs)
        self.extend_inmemory_docs_and_tags(flat_docs)
        return DocumentArray([])

    @secure_request(on='/delete', level=SecurityLevel.USER)
    def delete_endpoint(self, parameters: dict = {}, **kwargs):
        """
        Delete endpoint to delete document/documents from the index.
        Filter conditions can be passed to select documents for deletion.
        """
        filter = parameters.get("filter", {})
        if filter:
            filtered_docs = deepcopy(self.document_list.find(filter=filter))
            self.delete_inmemory_docs_and_tags(filtered_docs)
            self.delete(filtered_docs, parameters, **kwargs)

        return DocumentArray()

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search_endpoint(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        """Perform a vector similarity search and retrieve Document matches"""
        limit = int(parameters.get('limit', self.limit))
        search_filter = parameters.get('filter', {})
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        flat_docs = docs[traversal_paths][
            :1
        ]  # only search on the first document for now

        if self.traversal_paths == '@c':
            retrieval_limit = limit * 3
        else:
            retrieval_limit = limit

        self.search(flat_docs, parameters, retrieval_limit, search_filter)

        if self.traversal_paths == '@c':
            merge_matches_sum(flat_docs, limit)

        self.clean_response(flat_docs)
        return flat_docs

    def clean_response(self, docs):
        """removes the embedding from the root level and also from the matches."""
        for doc in docs:
            doc.embedding = None
            for match in doc.matches:
                match.embedding = None

    def parse_columns(self, columns):
        """Parse the columns to index"""
        self._valid_input_columns = ['str', 'float', 'int']
        if columns:
            corrected_list = []
            for i in range(0, len(columns), 2):
                corrected_list.append((columns[i], columns[i + 1]))
            columns = corrected_list
            for n, t in columns:
                assert (
                    t in self._valid_input_columns
                ), f'column of type={t} is not supported. Supported types are {self._valid_input_columns}'
        return columns

    def load_document_list(self):
        """is needed for the list endpoint"""
        document_list = DocumentArray()
        for batch in self.batch_iterator():
            self.extend_inmemory_docs_and_tags(batch)
        self.document_list = DocumentArray(
            sorted([d for d in document_list], key=lambda x: x.id)
        )

    def extend_inmemory_docs_and_tags(self, batch):
        """Extend the in-memory DocumentArray with new documents"""
        for d in batch:
            self.document_list.append(
                Document(id=d.id, uri=d.uri, tags=d.tags, parent_id=d.parent_id)
            )
            self.doc_id_tags[d.id] = d.tags

    def delete_inmemory_docs_and_tags(self, docs):
        """Delete documents from the in-memory DocumentArray"""
        for d in docs:
            del self.document_list[d.id]
            self.doc_id_tags.pop(d.id)

    def update_inmemory_docs_and_tags(self, docs):
        """Update documents in the in-memory DocumentArray"""
        self.delete_inmemory_docs_and_tags(docs)
        self.extend_inmemory_docs_and_tags(docs)

    def construct(self, **kwargs):
        """Calls the constructor of the specialized indexer"""
        raise NotImplementedError

    def batch_iterator(self):
        """Needs to be implemented in derived classes. Iterates over all documents in betches and yields them"""
        raise NotImplementedError

    def index(self, docs: DocumentArray, parameters: dict, **kwargs):
        """Needs to be implemented in derived classes. Indexes the documents"""
        raise NotImplementedError

    def delete(self, docs_to_delete, parameters: dict, **kwargs):
        """Needs to be implemented in derived classes. Deletes the documents"""
        raise NotImplementedError

    def search(
        self,
        docs: DocumentArray,
        parameters: dict,
        limit: int,
        search_filter: dict,
        **kwargs,
    ):
        """Needs to be implemented in derived classes. Searches the documents"""
        raise NotImplementedError
