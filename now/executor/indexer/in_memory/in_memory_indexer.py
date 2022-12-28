from docarray import DocumentArray

from now.executor.abstract.base_indexer import NOWBaseIndexer


class InMemoryIndexer(NOWBaseIndexer):
    """InMemoryIndexer indexes Documents into a DocumentArray with `storage='memory'`"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('InMemoryIndexer init')

    def construct(self, **kwargs):
        self._index = DocumentArray()

    def batch_iterator(self):
        for doc in self._index:
            yield doc

    def convert_filter_syntax(self, search_filter={}, search_filter_not={}):
        converted_filter = {}
        for key, val in search_filter.items():
            converted_filter[f"tags__{key}"] = val
        if search_filter_not:
            converted_filter['$not'] = {}
            for key_not, val_not in search_filter_not.items():
                converted_filter['$not'][f"tags__{key_not}"] = val_not
        return converted_filter

    def index(self, docs, parameters, **kwargs):
        for d in docs:
            if 'title' in d.tags:
                d.tags['title'] = d.tags['title'].lower()
        self._index.extend(docs)

    def delete(self, filtered_docs, *args, **kwargs):
        for doc in filtered_docs:
            del self._index[doc.id]

    def search(self, docs, parameters, retrieval_limit, search_filter, **kwargs):
        docs.match(self._index.find(search_filter), limit=retrieval_limit)
