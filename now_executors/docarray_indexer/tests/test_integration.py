import numpy as np
from docarray import Document, DocumentArray
from jina import Flow
from now_executors.docarray_indexer.docarray_indexer_v3 import DocarrayIndexerV3


def test_list():
    with Flow().add(uses=DocarrayIndexerV3) as f:
        f.index(
            DocumentArray(
                [
                    Document(
                        id="doc2",
                        blob=b"jpg...",
                        embedding=np.ones(5),
                        tags={'color': 'red', 'length': 18},
                    ),
                    Document(
                        id="doc3",
                        blob=b"jpg...",
                        embedding=np.ones(5),
                        tags={'color': 'blue'},
                    ),
                ]
            )
        )

        x = f.post('/list')
        assert len(x) == 2
        assert 'color' in x[0].tags
        assert x[0].embedding is None
        assert x[0].blob == b''
        assert len(x[0].chunks) == 0


def test_filtering():
    with Flow().add(uses=DocarrayIndexerV3, uses_with={"traversal_paths": "@r"}) as f:
        f.index(
            DocumentArray(
                [
                    Document(
                        id="doc1",
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
                                id="chunk1",
                                blob=b"jpg...",
                                embedding=np.ones(5),
                                tags={'color': 'blue'},
                            ),
                        ],
                    ),
                    Document(
                        id="doc2",
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
        result = f.post(
            on='/filter',
            parameters={
                'filter': {
                    '$and': [
                        {'tags__color': {'$eq': 'red'}},
                        {'tags__length': {'$eq': 19}},
                    ]
                }
            },
        )
        assert len(result) == 0
        result = f.post(
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
        assert len(result) == 1

        result = f.post(
            on='/filter', parameters={'filter': {'tags__something': {'$eq': 'kind'}}}
        )
        assert len(result) == 0

        result = f.post(
            on='/filter', parameters={'filter': {'tags__color': {'$eq': 'red'}}}
        )
        assert len(result) == 1


def test_search():
    with Flow().add(uses=DocarrayIndexerV3, uses_with={"traversal_paths": "@r"}) as f:
        f.index(
            DocumentArray(
                [
                    Document(
                        id="doc1",
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
                                id="chunk1",
                                blob=b"jpg...",
                                embedding=np.ones(5),
                                tags={'color': 'blue'},
                            ),
                        ],
                    ),
                    Document(
                        id="doc2",
                        blob=b"jpg...",
                        embedding=np.ones(5),
                        tags={'color': 'red', 'length': 18},
                    ),
                    Document(
                        id="doc3",
                        blob=b"jpg...",
                        embedding=np.ones(5),
                        tags={'color': 'blue'},
                    ),
                ]
            )
        )
        result = f.search(
            Document(
                id="doc1",
                blob=b"jpg...",
                embedding=np.ones(5),
            ),
            return_results=True,
            parameters={'filter': {'tags__color': {'$eq': 'blue'}}},
        )
        assert len(result[0].matches) == 1

        result2 = f.search(
            Document(
                id="doc2",
                blob=b"jpg...",
                embedding=np.ones(5),
            ),
            return_results=True,
        )
        assert len(result2[0].matches) == 3
