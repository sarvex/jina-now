import numpy as np
import pytest
from docarray import Document, DocumentArray
from jina import Flow
from now_executors.NOWQdrantIndexer.executor import NOWQdrantIndexer15


@pytest.fixture()
def documents():
    return DocumentArray(
        [
            Document(
                id="doc1",
                blob=b"gif...",
                embedding=np.array([0.3, 0.1, 0.1]),
                tags={'title': 'blue'},
                chunks=[
                    Document(
                        id="chunk11",
                        blob=b"jpg...",
                        embedding=np.array([0.1, 0.1]),
                        tags={'title': 'that is rEd for sure'},
                    ),
                    Document(
                        id="chunk12",
                        blob=b"jpg...",
                        embedding=np.array([0.2, 0.1]),
                        tags={'title': 'really bluE'},
                    ),
                ],
            ),
            Document(
                id="doc2",
                blob=b"jpg...",
                tags={'title': 'red', 'length': 18},
                embedding=np.array([0.4, 0.1, 0.1]),
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
                embedding=np.array([0.5, 0.1, 0.1]),
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
                embedding=np.array([0.6, 0.1, 0.1]),
                tags={'title': 'blue'},
            ),
        ]
    )


@pytest.mark.parametrize(
    'level,query,embedding,res_ids',
    [
        ('@c', 'blue', [0.5, 0.1], ['chunk12', 'chunk22', 'chunk31']),
        ('@c', 'red', [0.5, 0.1], ['chunk22', 'chunk31', 'chunk11']),
        ('@r', 'blue', [0.8, 0.1, 0.1], ['doc4', 'doc3', 'doc1', 'doc2']),
        ('@r', 'red', [0.8, 0.1, 0.1], ['doc2', 'doc4', 'doc3', 'doc1']),
    ],
)
def test_search_chunk_using_sum_ranker(
    documents, docker_compose, level, query, embedding, res_ids
):
    with Flow().add(
        uses=NOWQdrantIndexer15,
        uses_with={
            "traversal_paths": level,
            "dim": len(embedding),
            'columns': ['title', 'str'],
        },
    ) as f:
        f.index(
            documents,
        )
        result = f.search(
            Document(
                id="doc_search",
                embedding=np.array(embedding) if level == '@r' else None,
                text=query,
                chunks=Document(
                    id="chunk_search",
                    text=query,
                    embedding=np.array(embedding) if level == '@c' else None,
                ),
            ),
            return_results=True,
            parameters={'ranking_method': 'sum'},
        )
        print('all match ids', [match.id for match in result[0].matches])
        for d, res_id in zip(result[0].matches, res_ids):
            assert d.id == res_id
