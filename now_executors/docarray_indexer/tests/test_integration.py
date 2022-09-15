import numpy as np
import pytest
from docarray import Document, DocumentArray
from jina import Flow
from now_executors.docarray_indexer.docarray_indexer_v3 import (
    DocarrayIndexerV3_EXPERIMENT,
)


def test_delete():
    """testing deleting of docs using filter conditions"""
    with Flow().add(uses=DocarrayIndexerV3_EXPERIMENT) as f:
        f.index(
            DocumentArray(
                [
                    Document(
                        id="doc2",
                        tags={'color': 'red', 'length': 18},
                    ),
                    Document(
                        id="doc3",
                        tags={'color': 'blue'},
                    ),
                ]
            )
        )
        f.delete(
            parameters={'filter': {'tags__color': {'$eq': 'blue'}}},
        )
        result = f.post('/list')
        assert len(result) == 1
        assert result[0].id == 'doc2'
        assert result[0].tags['color'] == 'red'


def test_list():
    with Flow().add(uses=DocarrayIndexerV3_EXPERIMENT) as f:
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
    with Flow().add(
        uses=DocarrayIndexerV3_EXPERIMENT, uses_with={"traversal_paths": "@r"}
    ) as f:
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


@pytest.fixture()
def documents():
    return DocumentArray(
        [
            Document(
                id="doc1",
                blob=b"gif...",
                chunks=[
                    Document(
                        id="chunk11",
                        blob=b"jpg...",
                        embedding=np.array([0.1, 0.1]),
                        tags={'color': 'red'},
                    ),
                    Document(
                        id="chunk12",
                        blob=b"jpg...",
                        embedding=np.array([0.2, 0.1]),
                        tags={'color': 'blue'},
                    ),
                ],
            ),
            Document(
                id="doc2",
                blob=b"jpg...",
                tags={'color': 'red', 'length': 18},
                chunks=[
                    Document(
                        id="chunk21",
                        blob=b"jpg...",
                        embedding=np.array([0.3, 0.1]),
                        tags={'color': 'red'},
                    ),
                    Document(
                        id="chunk22",
                        blob=b"jpg...",
                        embedding=np.array([0.4, 0.1]),
                        tags={'color': 'red'},
                    ),
                ],
            ),
            Document(
                id="doc3",
                blob=b"jpg...",
                tags={'color': 'red', 'length': 18},
                chunks=[
                    Document(
                        id="chunk31",
                        blob=b"jpg...",
                        embedding=np.array([0.5, 0.1]),
                        tags={'color': 'red'},
                    ),
                ],
            ),
            Document(
                id="doc4",
                blob=b"jpg...",
                embedding=np.ones(6),
                tags={'color': 'blue'},
            ),
        ]
    )


def test_search(documents):
    with Flow().add(
        uses=DocarrayIndexerV3_EXPERIMENT, uses_with={"traversal_paths": "@r"}
    ) as f:
        f.index(documents)
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


def test_search_chunk(documents):
    with Flow().add(
        uses=DocarrayIndexerV3_EXPERIMENT, uses_with={"traversal_paths": "@c"}
    ) as f:
        f.index(
            documents,
        )
        result = f.search(
            Document(
                id="doc_search",
                chunks=Document(
                    id="chunk_search",
                    blob=b"jpg...",
                    embedding=np.ones(5),
                ),
            ),
            return_results=True,
            parameters={'filter': {'tags__color': {'$eq': 'blue'}}},
        )
        assert len(result[0].matches) == 1

        result2 = f.search(
            Document(
                chunks=Document(
                    id="doc2",
                    blob=b"jpg...",
                    embedding=np.ones(5),
                )
            ),
            return_results=True,
        )
        assert len(result2[0].matches) == 2


def test_search_chunk_using_sum_ranker(documents):
    with Flow().add(
        uses=DocarrayIndexerV3_EXPERIMENT, uses_with={"traversal_paths": "@c"}
    ) as f:
        f.index(
            documents,
        )
        result = f.search(
            Document(
                id="doc_search",
                chunks=Document(
                    id="chunk_search",
                    blob=b"jpg...",
                    embedding=np.array([0.5, 0.1]),
                ),
            ),
            return_results=True,
            parameters={'ranking_method': 'sum'},
        )
        assert len(result[0].matches) == 3
        assert result[0].matches[0].id == 'doc2'
        assert result[0].matches[1].id == 'doc1'
        assert result[0].matches[2].id == 'doc3'
