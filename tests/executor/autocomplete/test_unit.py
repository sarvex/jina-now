import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Text

from now.executor.autocomplete.executor import NOWAutoCompleteExecutor2


@dataclass
class MMDoc:
    text: Text


@pytest.fixture
def mmdocs_with_text():

    return DocumentArray(
        [
            Document(MMDoc(text='background')),
            Document(MMDoc(text='background')),
            Document(MMDoc(text='bang')),
            Document(MMDoc(text='loading')),
            Document(MMDoc(text='loading')),
            Document(MMDoc(text='laugh')),
        ]
    )


def word_list():
    words = [
        'loading',
        'loader',
        'arrow',
        'success',
        'confetti',
        'background',
        'login',
        '404',
        'money',
        'car',
    ]
    counts = [
        '39131',
        '8787',
        '6744',
        '6724',
        '6439',
        '4791',
        '4768',
        '4421',
        '4339',
        '4084',
    ]

    return words, counts


def test_empty():
    executor = NOWAutoCompleteExecutor2()

    assert executor.words == {}


def test_initialize():
    words, counts = word_list()

    new_words = {}
    for word, count in zip(words, counts):
        new_words[word] = {'count': count}

    executor = NOWAutoCompleteExecutor2(words=new_words)
    assert len(executor.words) == 10
    assert executor.words['loading']['count'] == '39131'


def test_search_update(tmpdir, mmdocs_with_text):
    executor = NOWAutoCompleteExecutor2(workspace=tmpdir)

    executor.search_update(mmdocs_with_text)
    assert executor.words['background']['count'] == 2
    assert executor.words['loading']['count'] == 2
    assert executor.words['bang']['count'] == 1


def test_search_update_profanity(tmpdir):
    executor = NOWAutoCompleteExecutor2(workspace=tmpdir)

    da = DocumentArray(
        [
            Document(MMDoc(text='background')),
            Document(MMDoc(text='background')),
            Document(MMDoc(text='shit')),
            Document(MMDoc(text='loading')),
            Document(MMDoc(text='fuck')),
            Document(MMDoc(text='f*ck')),
            Document(MMDoc(text='laugh')),
            Document(MMDoc(text='fuck shit somethings')),
        ]
    )

    executor.search_update(da)
    assert executor.words['background']['count'] == 2
    assert executor.words['loading']['count'] == 1
    assert 'fuck' not in executor.words
    assert 'shit' not in executor.words


def test_get_suggestion(tmpdir, mmdocs_with_text):
    executor = NOWAutoCompleteExecutor2(workspace=tmpdir)

    executor.search_update(mmdocs_with_text)

    da_sugg_1 = DocumentArray([Document(text='b')])
    executor.get_suggestion(da_sugg_1)
    da_sugg_2 = DocumentArray([Document(text='l')])
    executor.get_suggestion(da_sugg_2)
    da_sugg_3 = DocumentArray([Document(text='bac')])
    executor.get_suggestion(da_sugg_3)
    assert da_sugg_1[0].tags['suggestions'] == ['background', 'bang']
    assert da_sugg_2[0].tags['suggestions'] == ['loading', 'laugh']
    assert da_sugg_3[0].tags['suggestions'] == ['background']


def test_get_suggestion_bitrigrams(tmpdir):
    executor = NOWAutoCompleteExecutor2(workspace=tmpdir)

    da = DocumentArray(
        [
            Document(MMDoc(text='aziz')),
            Document(MMDoc(text='test')),
            Document(MMDoc(text='aziz test')),
            Document(MMDoc(text='red')),
            Document(MMDoc(text='red dress')),
            Document(MMDoc(text='red long dress')),
        ]
    )

    executor.search_update(da)

    da_sugg_1 = DocumentArray([Document(text='azi')])
    executor.get_suggestion(da_sugg_1)
    da_sugg_2 = DocumentArray([Document(text='r')])
    executor.get_suggestion(da_sugg_2)
    da_sugg_3 = DocumentArray([Document(text='d')])
    executor.get_suggestion(da_sugg_3)
    da_sugg_4 = DocumentArray([Document(text='l')])
    executor.get_suggestion(da_sugg_4)
    assert da_sugg_1[0].tags['suggestions'] == ['aziz', 'aziz test']
    assert da_sugg_2[0].tags['suggestions'] == [
        'red',
        'red dress',
        'red long dress',
    ]
    assert da_sugg_3[0].tags['suggestions'] == ['dress']
    assert da_sugg_4[0].tags['suggestions'] == ['long']
