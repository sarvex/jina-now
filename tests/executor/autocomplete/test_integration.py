from docarray import Document, DocumentArray
from jina import Flow

from now.executor.autocomplete.executor import NOWAutoCompleteExecutor2


def test_autocomplete():
    with Flow().add(uses=NOWAutoCompleteExecutor2) as f:
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
                    Document(text='red long dress'),
                ]
            ),
        )
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='b')]))
        assert response[0].tags['suggestions'] == ['background', 'bang']
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='l')]))
        assert response[0].tags['suggestions'] == ['loading', 'long', 'laugh']
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='bac')])
        )
        assert response[0].tags['suggestions'] == ['background']
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='fuc')])
        )
        assert response[0].tags['suggestions'] == []
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='red')])
        )
        assert response[0].tags['suggestions'] == ['red', 'red long dress']
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='d')]))
        assert response[0].tags['suggestions'] == ['dress']
