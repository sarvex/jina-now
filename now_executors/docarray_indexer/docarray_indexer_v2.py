from copy import deepcopy

import numpy as np
from docarray import Document, DocumentArray
from jina import Executor, Flow, requests


class DocarrayIndexerV2(Executor):
    def __init__(self, traversal_paths: str = "@r", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.traversal_paths = traversal_paths
        self.index = DocumentArray()

    @requests(on="/index")
    def index(self, docs: DocumentArray, **kwargs):
        docs.summary()
        self.index.extend(docs)
        return (
            DocumentArray()
        )  # prevent sending the data back by returning an empty DocumentArray

    @requests(on="/search")
    def search(self, docs: DocumentArray, parameters, **kwargs):
        limit = parameters.get("limit", 20)
        filter = parameters.get("filter", {})
        traversal_paths = parameters.get("traversal_paths", self.traversal_paths)
        if traversal_paths == "@r":
            docs.match(self.index.find(filter=filter), limit=limit)
        elif traversal_paths == "@c":
            index = self.index[traversal_paths]
            # to avoid having duplicate root level matches, we have to:
            # 0. matching happening on chunk level
            # 1. retrieve more docs since some of them could be duplicates
            # 2. maintain a list of unique parent docs
            # 3. break once we retrieved `limit` results
            docs = docs[0].chunks
            docs.match(index.find(filter=filter), limit=limit * 10)
            for d in docs:
                parents = []
                parent_ids = []
                for m in d.matches:
                    if m.parent_id in parent_ids:
                        continue
                    parent = self.index[m.parent_id]
                    # to save bandwidth, we don't return the chunks.
                    # But, without deepcopy, we would modify the ined
                    parent = deepcopy(parent)
                    parent.chunks = []
                    parents.append(parent)
                    parent_ids.append(m.parent_id)
                    if len(parents) == limit:
                        break
                d.matches = parents
        else:
            raise Exception("traversal paths not supported", traversal_paths)
        return docs

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


if __name__ == "__main__":

    with Flow().add(uses=DocarrayIndexerV2, uses_with={"traversal_paths": "@r"}) as f:
        f.index(
            DocumentArray(
                [
                    Document(
                        id="parent",
                        blob=b"gif...",
                        embedding=np.ones(5),
                        chunks=[
                            Document(
                                id="chunk1",
                                blob=b"jpg...",
                                embedding=np.ones(5),
                                tags={'color': 'red'},
                            ),
                            Document(
                                id="chunk2",
                                blob=b"jpg...",
                                embedding=np.ones(5),
                                tags={'color': 'blue'},
                            ),
                        ],
                    ),
                    Document(
                        id="doc1",
                        blob=b"jpg...",
                        embedding=np.ones(5),
                        tags={'color': 'red', 'length': 18},
                    ),
                    Document(
                        id="doc2",
                        blob=b"jpg...",
                        embedding=np.ones(5),
                        tags={'color': 'blue'},
                    ),
                ]
            )
        )

        x = f.post(
            on='/filter',
            parameters={
                'filter': {
                    '$and': [
                        {'tags__color': {'$eq': 'red'}},
                        {'tags__length': {'$eq': 18}},
                    ]
                }
            },
        )
        print('filter res:', x)
        if len(x) > 0:
            x[0].summary()

        x = f.post(
            on='/filter', parameters={'filter': {'tags__something': {'$eq': 'kind'}}}
        )
        print('filter res:', x)
        if len(x) > 0:
            x[0].summary()

        x = f.post(on='/filter', parameters={'filter': {'tags__color': {'$eq': 'red'}}})
        print('filter res:', x)
        if len(x) > 0:
            x[0].summary()

        x = f.search(
            Document(
                id="doc1",
                blob=b"jpg...",
                embedding=np.ones(5),
            ),
            return_results=True,
            parameters={'filter': {'tags__color': {'$eq': 'blue'}}},
        )
        x[0].summary()
        x = f.search(
            Document(
                id="doc1",
                blob=b"jpg...",
                embedding=np.ones(5),
            ),
            return_results=True,
        )
        x[0].summary()
