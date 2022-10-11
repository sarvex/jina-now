from docarray import Document, DocumentArray
from jina import Flow
from now_executors.NOWAutoCompleteExecutor.executor import NOWAutoCompleteExecutor


def test_autocomplete():
    with Flow().add(uses=NOWAutoCompleteExecutor) as f:
        f.post(
            on='/search',
            inputs=DocumentArray(
                [
                    Document(text='background'),
                    Document(text='background'),
                    Document(text='bang'),
                    Document(text='loading'),
                    Document(text='loading'),
                    Document(text='laugh'),
                    Document(text='fuck'),
                ]
            ),
        )
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='b')]))
        assert response[0].tags['suggestions'] == [['background'], ['bang']]
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='l')]))
        assert response[0].tags['suggestions'] == [['loading'], ['laugh']]
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='bac')])
        )
        assert response[0].tags['suggestions'] == [['background']]
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='fuc')])
        )
        assert response[0].tags['suggestions'] == []
