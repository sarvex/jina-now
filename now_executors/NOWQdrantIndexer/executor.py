from jina import DocumentArray
from now_common.abstract_executors.NOWBaseIndexer.base_indexer import (
    NOWBaseIndexer as Executor,
)
from now_executors.NOWQdrantIndexer.server import setup_qdrant_server


class NOWQdrantIndexer15(Executor):
    """NOWQdrantIndexer15 indexes Documents into a Qdrant server using DocumentArray  with `storage='qdrant'`"""

    # override
    def construct(self, **kwargs):
        setup_qdrant_server(self.workspace, self.logger)
        self._index = DocumentArray(
            storage='qdrant',
            config={
                'collection_name': 'Persisted',
                'host': 'localhost',
                'port': 6333,
                'n_dim': self.dim,
                'distance': self.metric,
                'ef_construct': None,
                'm': None,
                'scroll_batch_size': 64,
                'full_scan_threshold': None,
                'serialize_config': {},
                'columns': self.columns,
            },
        )

    # override
    def batch_iterator(self):
        """Iterator wich iterates through the documents of self._index and yields batches"""
        batch = []
        for item in self._index:
            batch.append(item)
            if len(batch) == 1000:
                yield batch
                batch = []
        if batch:
            yield batch

    # override
    def index(self, docs: DocumentArray, parameters: dict, **kwargs):
        """Index new documents"""
        # qdrant needs a list of values when filtering on sentences
        for d in docs:
            if 'title' in d.tags:
                d.tags['title'] = d.tags['title'].lower().split()
            else:
                d.tags['title'] = []
        self._index.extend(docs)

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
        docs.match(self._index, filter=search_filter, limit=limit)
