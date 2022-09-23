from copy import deepcopy
from sys import maxsize
from typing import Dict, List, Optional

import annlite
from jina import Document, DocumentArray
from jina.logging.logger import JinaLogger
from now_executors.NOWAuthExecutor.executor import NOWAuthExecutor as Executor
from now_executors.NOWAuthExecutor.executor import SecurityLevel, secure_request


class NOWAnnLiteIndexer(Executor):
    """
    A simple Indexer based on PQLite that stores all the Document data together in a local LMDB store.

    To be used as a hybrid indexer, supporting pre-filtering searching.
    """

    def __init__(
        self,
        dim: int = 0,
        metric: str = 'cosine',
        limit: int = 10,
        ef_construction: int = 200,
        ef_query: int = 50,
        max_connection: int = 16,
        include_metadata: bool = True,
        index_traversal_paths: str = '@r',
        search_traversal_paths: str = '@r',
        columns: Optional[List] = None,
        serialize_config: Optional[Dict] = None,
        *args,
        **kwargs,
    ):
        """
        :param dim: Dimensionality of vectors to index
        :param metric: Distance metric type. Can be 'euclidean', 'inner_product', or 'cosine'
        :param include_metadata: If True, return the document metadata in response
        :param limit: Number of results to get for each query document in search
        :param ef_construction: The construction time/accuracy trade-off
        :param ef_query: The query time accuracy/speed trade-off
        :param max_connection: The maximum number of outgoing connections in the
            graph (the "M" parameter)
        :param index_traversal_paths: Default traversal paths on docs
                (used for indexing, delete and update), e.g. '@r', '@c', '@r,c'
        :param search_traversal_paths: Default traversal paths on docs
        (used for search), e.g. '@r', '@c', '@r,c'
        :param columns: List of tuples of the form (column_name, str_type). Here str_type must be a string that can be
                parsed as a valid Python type.
        :param serialize_config: The configurations used for serializing documents, e.g., {'protocol': 'pickle'}
        """
        super().__init__(*args, **kwargs)
        self.logger = JinaLogger(self.__class__.__name__)

        assert dim > 0, 'Please specify the dimension of the vectors to index!'

        self.metric = metric
        self.limit = limit
        self.include_metadata = include_metadata
        self.index_traversal_paths = index_traversal_paths
        self.search_traversal_paths = search_traversal_paths
        self._valid_input_columns = ['str', 'float', 'int']

        if columns:
            corrected_list = []
            for i in range(0, len(columns), 2):
                corrected_list.append((columns[i], columns[i + 1]))
            columns = corrected_list
            cols = []
            for n, t in columns:
                assert (
                    t in self._valid_input_columns
                ), f'column of type={t} is not supported. Supported types are {self._valid_input_columns}'
                cols.append((n, eval(t)))
            columns = cols

        self._index = annlite.AnnLite(
            dim=dim,
            metric=metric,
            columns=columns,
            ef_construction=ef_construction,
            ef_query=ef_query,
            max_connection=max_connection,
            data_path=self.workspace or './workspace',
            serialize_config=serialize_config or {},
            **kwargs,
        )
        self.da = DocumentArray()
        for cell_id in range(self._index.n_cells):
            for docs in self._index.documents_generator(cell_id, batch_size=10240):
                self.extend_inmemory_docs(docs)

        self.da = DocumentArray(sorted([d for d in self.da], key=lambda x: x.id))

    def extend_inmemory_docs(self, docs):
        """Extend the in-memory DocumentArray with new documents"""
        self.da.extend(Document(id=d.id, uri=d.uri, tags=d.tags) for d in docs)

    def update_inmemory_docs(self, docs):
        """Update documents in the in-memory DocumentArray"""
        for d in docs:
            self.da[d.id] = d

    def delete_inmemory_docs(self, docs):
        """Delete documents from the in-memory DocumentArray"""
        for d in docs:
            del self.da[d.id]

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        """Index new documents

        :param docs: the Documents to index
        :param parameters: dictionary with options for indexing
        Keys accepted:
            - 'traversal_paths' (str): traversal path for the docs
        """
        if not docs:
            return

        traversal_paths = parameters.get('traversal_paths', self.index_traversal_paths)
        flat_docs = docs[traversal_paths]
        if len(flat_docs) == 0:
            return

        self._index.index(flat_docs)
        self.extend_inmemory_docs(flat_docs)
        return DocumentArray([])

    @secure_request(on='/update', level=SecurityLevel.USER)
    def update(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        """Update existing documents

        :param docs: the Documents to update
        :param parameters: dictionary with options for updating
        Keys accepted:

            - 'traversal_paths' (str): traversal path for the docs
        """

        if not docs:
            return

        traversal_paths = parameters.get('traversal_paths', self.index_traversal_paths)
        flat_docs = docs[traversal_paths]
        if len(flat_docs) == 0:
            return

        self.update_inmemory_docs(flat_docs)
        self._index.update(flat_docs)

    @secure_request(on='/delete', level=SecurityLevel.USER)
    def delete(self, parameters: dict = {}, **kwargs):
        """
        Delete endpoint to delete document/documents from the index.
        Filter conditions can be passed to select documents for deletion.
        """
        filter = parameters.get("filter", {})
        if filter:
            filtered_docs = deepcopy(self.da.find(filter=filter))
            self.delete_inmemory_docs(filtered_docs)
            self._index.delete(filtered_docs)
        return DocumentArray()

    @secure_request(on='/list', level=SecurityLevel.USER)
    def list(self, parameters: dict = {}, **kwargs):
        """List all indexed documents.
        :param parameters: dictionary with limit and offset
        - offset (int): number of documents to skip
        - limit (int): number of retrieved documents
        """
        limit = int(parameters.get('limit', maxsize))
        offset = int(parameters.get('offset', 0))
        return self.da[offset : offset + limit]

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        """Perform a vector similarity search and retrieve Document matches

        Search can be performed with candidate filtering. Filters are a triplet (column,operator,value).
        More than a filter can be applied during search. Therefore, conditions for a filter are specified as a list triplets.
        Each triplet contains:

        - column: Column used to filter.
        - operator: Binary operation between two values. Some supported operators include `['>','<','=','<=','>=']`.
        - value: value used to compare a candidate.

        :param docs: the Documents to search with
        :param parameters: dictionary for parameters for the search operation
        Keys accepted:

            - 'filter' (dict): the filtering conditions on document tags
            - 'traversal_paths' (str): traversal paths for the docs
            - 'limit' (int): nr of matches to get per Document
        """
        if not docs:
            return

        limit = int(parameters.get('limit', self.limit))
        search_filter = parameters.get('filter', {})
        include_metadata = bool(
            parameters.get('include_metadata', self.include_metadata)
        )

        traversal_paths = parameters.get('traversal_paths', self.search_traversal_paths)
        flat_docs = docs[traversal_paths]
        if len(flat_docs) == 0:
            return

        if self.search_traversal_paths == '@c':
            retrieval_limit = limit * 3
        else:
            retrieval_limit = limit

        self._index.search(
            flat_docs,
            filter=search_filter,
            limit=retrieval_limit,
            include_metadata=include_metadata,
        )

        if self.search_traversal_paths == '@c':
            docs = docs[0].chunks
            for d in docs:
                unique_matches = []
                parent_ids = set()
                d.embedding = None
                for m in d.matches:
                    m.embedding = None
                    if m.parent_id in parent_ids:
                        continue
                    unique_matches.append(m)
                    parent_ids.add(m.parent_id)
                    if len(unique_matches) == limit:
                        break
                d.matches = unique_matches
        return docs

    @secure_request(on='/status', level=SecurityLevel.USER)
    def status(self, **kwargs) -> DocumentArray:
        """Return the document containing status information about the indexer.

        The status will contain information on the total number of indexed and deleted
        documents, and on the number of (searchable) documents currently in the index.
        """

        status = Document(tags=self._index.stat)
        return DocumentArray([status])

    @secure_request(on='/clear', level=SecurityLevel.USER)
    def clear(self, **kwargs):
        """Clear the index of all entries."""
        self._index.clear()

    def close(self, **kwargs):
        """Close the index."""
        self._index.close()
