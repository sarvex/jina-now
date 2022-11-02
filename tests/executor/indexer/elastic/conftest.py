import numpy as np
import pytest
from docarray import Document, DocumentArray


@pytest.fixture
def multimodal_da():
    return DocumentArray(
        [
            Document(
                id='123',
                tags={'cost': 18.0},
                chunks=[
                    Document(
                        id='x',
                        text='this is a flower',
                        embedding=np.ones(7),
                    ),
                    Document(
                        id='xx',
                        uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                        embedding=np.ones(5),
                    ),
                ],
            ),
            Document(
                id='456',
                tags={'cost': 21.0},
                chunks=[
                    Document(
                        id='xxx',
                        text='this is a cat',
                        embedding=np.array([1, 2, 3, 4, 5, 6, 7]),
                    ),
                    Document(
                        id='xxxx',
                        uri='https://cdn.pixabay.com/photo/2015/04/23/21/59/tree-736877_1280.jpg',
                        embedding=np.array([1, 2, 3, 4, 5]),
                    ),
                ],
            ),
        ]
    )


@pytest.fixture
def multimodal_query():
    return DocumentArray(
        [
            Document(
                text='cat',
                chunks=[
                    Document(
                        embedding=np.array([1, 2, 3, 4, 5, 6, 7]),
                    ),
                    Document(
                        embedding=np.array([1, 2, 3, 4, 5]),
                    ),
                ],
            )
        ]
    )


@pytest.fixture
def text_da():
    return DocumentArray(
        [
            Document(
                id='123',
                tags={'cost': 18.0},
                text='test text',
                embedding=np.ones(7),
            ),
            Document(
                id='456',
                tags={'cost': 21.0},
                text='another text',
                embedding=np.array([1, 2, 3, 4, 5, 6, 7]),
            ),
        ]
    )


@pytest.fixture
def text_query():
    return DocumentArray([Document(text='text', embedding=np.ones(7))])


@pytest.fixture
def docs_matrix_index():
    return [
        DocumentArray(
            Document(
                chunks=[
                    Document(
                        text='this', embedding=np.ones(768)
                    ),  # embedding from SBERT
                    Document(uri='https://jina.ai'),  # not encoded by SBERT
                ]
            )
        ),
        DocumentArray(
            Document(
                chunks=[
                    Document(
                        text='this', embedding=np.ones(512)
                    ),  # embedding from CLIP text model
                    Document(
                        uri='https://jina.ai', embedding=np.ones(512)
                    ),  # embedding from CLIP image model
                ]
            )
        ),
    ]


@pytest.fixture
def docs_matrix_search():
    return [
        DocumentArray(
            [
                Document(
                    chunks=[
                        Document(
                            text='this', embedding=np.ones(768)
                        ),  # embedding from SBERT
                    ]
                )
            ]
        ),
        DocumentArray(
            [
                Document(
                    chunks=[
                        Document(
                            text='this', embedding=np.ones(512)
                        ),  # embedding from CLIP text model
                    ]
                )
            ]
        ),
    ]
