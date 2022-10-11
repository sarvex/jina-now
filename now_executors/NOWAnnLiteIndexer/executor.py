import annlite
from jina import DocumentArray
from now_common.abstract_executors.NOWBaseIndexer.base_indexer import (
    NOWBaseIndexer as Executor,
)


class NOWAnnLiteIndexer(Executor):
    """
    A simple Indexer based on PQLite that stores all the Document data together in a local LMDB store. (Deprecated)
    """

    # override
    def construct(self, **kwargs):
        """Construct the Indexer"""
        self._index = annlite.AnnLite(
            n_dim=self.dim,
            metric=self.metric,
            columns=self.columns,
            ef_construction=200,
            ef_query=50,
            max_connection=16,
            data_path=self.workspace or './workspace',
            serialize_config={},
            **kwargs,
        )

    # override
    def batch_iterator(self):
        """Return a batch iterator over the indexed documents"""
        for cell_id in range(self._index.n_cells):
            for docs in self._index.documents_generator(cell_id, batch_size=10240):
                yield docs

    # override
    def index(self, docs: DocumentArray, parameters: dict, **kwargs):
        """Index new documents"""
        self._index.index(docs)

    # override
    def delete(self, documents_to_delete, parameters: dict = {}, **kwargs):
        """
        Delete endpoint to delete document/documents from the index.
        Filter conditions can be passed to select documents for deletion.
        """
        for d in documents_to_delete:
            del self._index[d.id]

    # override
    def search(
        self,
        docs: DocumentArray,
        parameters: dict,
        limit: int,
        search_filter: dict,
        **kwargs
    ):
        """Perform a vector similarity search and retrieve Document matches"""
        self._index.search(
            docs,
            filter=search_filter,
            limit=limit,
        )
