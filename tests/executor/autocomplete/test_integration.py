from docarray import Document, DocumentArray
from jina import Flow

from now.executor.autocomplete.executor import NOWAutoCompleteExecutor


def test_autocomplete():
    with Flow().add(uses=NOWAutoCompleteExecutor) as f:
        f.post(
            on='/search',
            inputs=DocumentArray(
                [
                    Document(chunks=[Document(text='background')]),
                    Document(chunks=[Document(text='background')]),
                    Document(chunks=[Document(text='bang')]),
                    Document(chunks=[Document(text='loading')]),
                    Document(chunks=[Document(text='loading')]),
                    Document(chunks=[Document(text='laugh')]),
                    Document(chunks=[Document(text='fuck')]),
                    Document(chunks=[Document(text='red long dress')]),
                ]
            ),
        )
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='b')]))
        assert response[0].tags['suggestions'] == [['background'], ['bang']]
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='l')]))
        assert response[0].tags['suggestions'] == [['loading'], ['long'], ['laugh']]
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='bac')])
        )
        assert response[0].tags['suggestions'] == [['background']]
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='fuc')])
        )
        assert response[0].tags['suggestions'] == []
        response = f.post(
            on='/suggestion', inputs=DocumentArray([Document(text='red')])
        )
        assert response[0].tags['suggestions'] == [['red'], ['red long dress']]
        response = f.post(on='/suggestion', inputs=DocumentArray([Document(text='d')]))
        assert response[0].tags['suggestions'] == [['dress']]
