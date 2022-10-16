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

    def convert_filter_syntax(self, filter):
        return filter

    def index(self, docs, parameters, **kwargs):
        self._index.extend(docs)

    def delete(self, filtered_docs, *args, **kwargs):
        for doc in filtered_docs:
            del self._index[doc.id]

    def search(self, docs, parameters, retrieval_limit, search_filter, **kwargs):
        docs.match(self._index, limit=retrieval_limit)
        docs[0].matches = [
            d for d in docs[0].matches if self.match_filter(d, search_filter)
        ]

    def match_filter(self, doc, search_filter):
        """
        determines if a document matches the search filter
        example for search_filter: {'filter': {'price': {'$lt': 50.0}}}
        filter tags can be $lt, $gt, $let, $get, $eq
        """
        operator_to_function = {
            '$lt': lambda x, y: x < y,
            '$gt': lambda x, y: x > y,
            '$let': lambda x, y: x <= y,
            '$get': lambda x, y: x >= y,
            '$eq': lambda x, y: x == y,
        }
        for key, filter_condition in search_filter.items():
            for operator_string, value in filter_condition.items():
                if key not in doc.tags or not operator_to_function[operator_string](
                    doc.tags[key], value
                ):
                    return False
        return True
