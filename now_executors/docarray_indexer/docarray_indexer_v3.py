from collections import defaultdict
from copy import deepcopy
from sys import maxsize

from docarray import Document, DocumentArray
from jina import Executor, requests


class DocarrayIndexerV3_EXPERIMENT(Executor):
    def __init__(self, traversal_paths: str = "@r", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.traversal_paths = traversal_paths
        self.index = DocumentArray()

    @requests(on="/index")
    def index(self, docs: DocumentArray, **kwargs):
        # for the experiment, we don't need blobs in the root and chunk level
        for d in docs:
            d.blob = None
            for c in d.chunks:
                c.blob = None
        self.index.extend(docs)
        return DocumentArray()
        #  prevent sending the data back by returning an empty DocumentArray

    @requests(on="/list")
    def list(self, parameters: dict = {}, **kwargs):
        """List all indexed documents.
        :param parameters: dictionary with limit and offset
        - offset (int): number of documents to skip
        - limit (int): number of retrieved documents
        """
        limit = int(parameters.get('limit', maxsize))
        offset = int(parameters.get('offset', 0))
        part = self.index[offset : offset + limit]
        return DocumentArray([Document(id=d.id, uri=d.uri, tags=d.tags) for d in part])

    @requests(on="/search")
    def search(self, docs: DocumentArray, parameters, **kwargs):
        """
        Search endpoint to search for document/documents and match across
        the index.
        We use the same interface of Annlite for filtering, this means the
        filter key in parameters should contain queries for annlite
        for example: {'filter': {'$and':[{'color':{'$eq':'red'}}]}}
        We adapt this query by parsing the above query and add the prefix tags__
        to all tags.
        """
        limit = int(parameters.get("limit", 20))
        traversal_paths = parameters.get("traversal_paths", self.traversal_paths)
        ranking_method = parameters.get("ranking_method", "sum")
        docs = docs[traversal_paths]
        filtered_docs = self.filter_docs(self.index[traversal_paths], parameters)

        match_limit = limit
        if traversal_paths == "@c":
            match_limit = limit * 3
        docs.match(filtered_docs, limit=match_limit)
        if traversal_paths == "@c":
            if ranking_method == "min":
                self.merge_matches_min(docs, limit)
            elif ranking_method == "sum":
                self.merge_matches_sum(docs, limit)
        return docs

    @requests(on="/delete")
    def delete(self, parameters, **kwargs):
        """
        Delete endpoint to delete document/documents from the index.
        Filter conditions can be passed to select documents for deletion.
        """
        filtered_docs = self.filter_docs(parameters)
        for doc in filtered_docs:
            self.index.remove(doc)
        return DocumentArray()

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

    def merge_matches_min(self, docs, limit):
        #   to avoid having duplicate root level matches, we have to:
        #   0. matching happening on chunk level
        #   1. retrieve more docs since some of them could be duplicates
        #   2. maintain a list of unique parent docs
        #   3. break once we retrieved `limit` results
        for d in docs:
            parents = []
            parent_ids = []
            for m in d.matches:
                if m.parent_id in parent_ids:
                    continue
                parent = self.index[m.parent_id]
                #  to save bandwidth, we don't return the chunks.
                #  But, without deepcopy, we would modify the ined
                parent = deepcopy(parent)
                parent.chunks = []
                parents.append(parent)
                parent_ids.append(m.parent_id)
                if len(parents) == limit:
                    break
            d.matches = parents

    def merge_matches_sum(self, docs, limit):
        # in contrast to merge_matches_min, merge_matches_avg sorts the parent matches by the average distance of all chunk matches
        # we have 3 chunks indexed for each root document but the matches might contain less than 3 chunks
        # in case of less than 3 chunks, we assume that the distance of the missing chunks is the same to the last match
        # m.score.value is a distance metric
        for d in docs:
            parent_id_count_and_sum = defaultdict(lambda: [0, 0])
            for m in d.matches:
                count_and_sum = parent_id_count_and_sum[m.parent_id]
                distance = m.scores['cosine'].value
                count_and_sum[0] += 1
                count_and_sum[1] += distance
            all_parent_ids = []
            for group in (3, 2, 1):
                parent_to_sum = {
                    parent: count_and_sum[1]
                    for parent, count_and_sum in parent_id_count_and_sum.items()
                    if count_and_sum[0] == group
                }
                parent_to_sum_sorted = sorted(parent_to_sum.items(), key=lambda x: x[1])
                parent_ids = [parent for parent, _ in parent_to_sum_sorted]
                all_parent_ids.extend(parent_ids)
                print(f'# num parents for group {group}: {len(parent_ids)}')
            parent_docs = [self.index[parent_id] for parent_id in all_parent_ids]
            d.matches = parent_docs[:limit]

    def filter_docs(self, documents, parameters):
        filter = parameters.get("filter", {})
        if '$and' in filter.keys():
            for query in filter['$and']:
                #  adapt filter interface to annlite interface by adding tags
                tag = list(query.keys())[0]
                query['tags__' + tag] = query[tag]
                del query[tag]
        filtered_docs = documents.find(filter=filter)
        return filtered_docs
