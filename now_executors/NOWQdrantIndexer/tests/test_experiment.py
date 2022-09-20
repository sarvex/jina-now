import numpy as np
import pytest
from docarray import Document, DocumentArray
from jina import Flow


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
                        tags={'title': 'that is red for sure'},
                    ),
                    Document(
                        id="chunk12",
                        blob=b"jpg...",
                        embedding=np.array([0.2, 0.1]),
                        tags={'title': 'really blue'},
                    ),
                ],
            ),
            Document(
                id="doc2",
                blob=b"jpg...",
                tags={'title': 'red', 'length': 18},
                chunks=[
                    Document(
                        id="chunk21",
                        blob=b"jpg...",
                        embedding=np.array([0.3, 0.1]),
                        tags={'title': 'my red shirt'},
                    ),
                    Document(
                        id="chunk22",
                        blob=b"jpg...",
                        embedding=np.array([0.4, 0.1]),
                        tags={'title': 'red is nice'},
                    ),
                ],
            ),
            Document(
                id="doc3",
                blob=b"jpg...",
                tags={'title': 'blue', 'length': 18},
                chunks=[
                    Document(
                        id="chunk31",
                        blob=b"jpg...",
                        embedding=np.array([0.5, 0.1]),
                        tags={'title': 'it is red'},
                    ),
                ],
            ),
            Document(
                id="doc4",
                blob=b"jpg...",
                embedding=np.ones(6),
                tags={'title': 'blue'},
            ),
        ]
    )


def test_search_chunk_using_sum_ranker(documents):
    with Flow().add(
        uses='jinahub+docker://QdrantIndexer3/experiment8',
        uses_with={"traversal_paths": "@c", "dim": 2},
    ) as f:
        f.index(
            documents,
        )
        result = f.search(
            Document(
                id="doc_search",
                chunks=Document(
                    id="chunk_search",
                    # text='blue',
                    text='red',
                    # blob=b"jpg...",
                    embedding=np.array([0.5, 0.1]),
                ),
            ),
            return_results=True,
            parameters={'ranking_method': 'sum'},
        )
        assert len(result[0].matches) == 3
        assert result[0].matches[0].id == 'chunk22'
        assert result[0].matches[1].id == 'chunk31'
        assert result[0].matches[2].id == 'chunk11'
