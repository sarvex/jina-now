from docarray import Document, DocumentArray, dataclass
from docarray.typing import Text
from jina import Flow

from now.executor.autocomplete.executor import NOWAutoCompleteExecutor2


def test_autocomplete(tmpdir):
    @dataclass
    class MMDoc:
        text: Text

    with Flow().add(uses=NOWAutoCompleteExecutor2, workspace=tmpdir) as f:
        f.post(
            on='/search',
            inputs=DocumentArray(
                [
                    Document(MMDoc(text='background')),
                    Document(MMDoc(text='background')),
                    Document(MMDoc(text='bang')),
                    Document(MMDoc(text='loading')),
                    Document(MMDoc(text='loading')),
                    Document(MMDoc(text='laugh')),
                    Document(MMDoc(text='hello')),
                    Document(MMDoc(text='red long dress')),
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
