from docarray import Document, DocumentArray
from now_executors.NOWAutoCompleteExecutor.executor import NOWAutoCompleteExecutor


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
    executor = NOWAutoCompleteExecutor()

    assert executor.words == {}


def test_initialize():
    words, counts = word_list()

    new_words = {}
    for word, count in zip(words, counts):
        new_words[word] = {'count': count}

    executor = NOWAutoCompleteExecutor(words=new_words)
    assert len(executor.words) == 10
    assert executor.words['loading']['count'] == '39131'


def test_search_update():
    executor = NOWAutoCompleteExecutor()

    da = DocumentArray(
        [
            Document(text='background'),
            Document(text='background'),
            Document(text='bang'),
            Document(text='loading'),
            Document(text='loading'),
            Document(text='laugh'),
        ]
    )

    executor.search_update(da)
    assert executor.words['background']['count'] == 2
    assert executor.words['loading']['count'] == 2
    assert executor.words['bang']['count'] == 1


def test_search_update_profanity():
    executor = NOWAutoCompleteExecutor()

    da = DocumentArray(
        [
            Document(text='background'),
            Document(text='background'),
            Document(text='shit'),
            Document(text='loading'),
            Document(text='fuck'),
            Document(text='laugh'),
            Document(text='l3sbi4n sorry I feel bad whe writing this test'),
        ]
    )

    executor.search_update(da)
    assert executor.words['background']['count'] == 2
    assert executor.words['loading']['count'] == 1
    assert 'fuck' not in executor.words
    assert 'shit' not in executor.words
    assert 'l3sbian' not in executor.words


def test_get_suggestion():
    executor = NOWAutoCompleteExecutor()

    da = DocumentArray(
        [
            Document(text='background'),
            Document(text='background'),
            Document(text='bang'),
            Document(text='loading'),
            Document(text='loading'),
            Document(text='laugh'),
        ]
    )

    executor.search_update(da)

    da_sugg_1 = DocumentArray([Document(text='b')])
    executor.get_suggestion(da_sugg_1)
    da_sugg_2 = DocumentArray([Document(text='l')])
    executor.get_suggestion(da_sugg_2)
    da_sugg_3 = DocumentArray([Document(text='bac')])
    executor.get_suggestion(da_sugg_3)
    assert da_sugg_1[0].tags['suggestions'] == [['background'], ['bang']]
    assert da_sugg_2[0].tags['suggestions'] == [['loading'], ['laugh']]
    assert da_sugg_3[0].tags['suggestions'] == [['background']]
