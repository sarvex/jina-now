import numpy as np
import pytest
from jina import Document, DocumentArray, Flow
from now_executors.NOWAnnLiteIndexer.executor import NOWAnnLiteIndexer

NUMBER_OF_DOCS = 10
DIM = 128


def gen_docs(num, has_chunk=False):
    res = DocumentArray()
    k = np.random.random((num, DIM)).astype(np.float32)
    for i in range(num):
        doc = Document(
            id=f'{i}',
            text='parent',
            embedding=k[i],
            uri='my-parent-uri',
            tags={'parent_tag': 'value'},
        )
        if has_chunk:
            for j in range(2):
                doc.chunks.append(
                    Document(
                        id=f'{i}_{j}',
                        embedding=doc.embedding,
                        uri='my-parent-uri',
                        tags={'parent_tag': 'value'},
                    )
                )
            doc.embedding = None
        res.append(doc)
    return res


def docs_with_tags(NUMBER_OF_DOCS):
    prices = [10.0, 25.0, 50.0, 100.0]
    categories = ['comics', 'movies', 'audiobook']
    X = np.random.random((NUMBER_OF_DOCS, DIM)).astype(np.float32)
    docs = [
        Document(
            id=f'{i}',
            embedding=X[i],
            tags={
                'price': np.random.choice(prices),
                'category': np.random.choice(categories),
            },
        )
        for i in range(NUMBER_OF_DOCS)
    ]
    da = DocumentArray(docs)

    return da


def test_index(tmpdir):
    """Test indexing does not return anything"""
    metas = {'workspace': str(tmpdir)}
    docs = gen_docs(NUMBER_OF_DOCS)
    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={
            'dim': DIM,
        },
        uses_metas=metas,
    )
    with f:
        result = f.post(on='/index', inputs=docs, return_results=True)
        assert len(result) == 0


@pytest.mark.parametrize(
    'offset, limit', [(0, 0), (10, 0), (0, 10), (10, 10), (None, None)]
)
@pytest.mark.parametrize('has_chunk', [True, False])
def test_list(tmpdir, offset, limit, has_chunk):
    """Test list returns all indexed docs"""
    metas = {'workspace': str(tmpdir)}
    docs = gen_docs(NUMBER_OF_DOCS, has_chunk=has_chunk)
    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={
            'dim': DIM,
        },
        uses_metas=metas,
    )
    with f:
        parameters = {}
        if offset is not None:
            parameters.update({'offset': offset, 'limit': limit})
        if has_chunk:
            parameters.update({'traversal_paths': '@c', 'chunks_size': 2})

        f.post(on='/index', inputs=docs, parameters=parameters)
        list_res = f.post(on='/list', parameters=parameters, return_results=True)
        if offset is None:
            l = NUMBER_OF_DOCS
        else:
            l = max(limit - offset, 0)
        assert len(list_res) == l
        if l > 0:
            if has_chunk:
                assert len(list_res[0].chunks) == 0
                assert len(set([d.id for d in list_res])) == l
                assert [d.id for d in list_res] == [f'{i}_0' for i in range(l)]
                assert [d.uri for d in list_res] == ['my-parent-uri'] * l
                assert [d.tags['parent_tag'] for d in list_res] == ['value'] * l
            else:
                assert list_res[0].id == str(offset) if offset is not None else '0'
                assert list_res[0].uri == 'my-parent-uri'
                assert len(list_res[0].chunks) == 0
                assert list_res[0].embedding is None
                assert list_res[0].text == ''
                assert list_res[0].tags == {'parent_tag': 'value'}


def test_search(tmpdir):
    metas = {'workspace': str(tmpdir)}
    docs = gen_docs(NUMBER_OF_DOCS)
    docs_query = gen_docs(1)
    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={
            'dim': DIM,
        },
        uses_metas=metas,
    )
    with f:
        f.post(on='/index', inputs=docs)

        query_res = f.post(on='/search', inputs=docs_query, return_results=True)
        assert len(query_res) == 1

        for i in range(len(query_res[0].matches) - 1):
            assert (
                query_res[0].matches[i].scores['cosine'].value
                <= query_res[0].matches[i + 1].scores['cosine'].value
            )


def test_search_match(tmpdir):
    metas = {'workspace': str(tmpdir)}
    docs = gen_docs(NUMBER_OF_DOCS, has_chunk=True)
    docs_query = gen_docs(NUMBER_OF_DOCS, has_chunk=True)
    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={
            'dim': DIM,
            'traversal_paths': '@c',
        },
        uses_metas=metas,
    )
    with f:
        f.post(on='/index', inputs=docs)

        query_res = f.post(
            on='/search',
            inputs=docs_query,
            parameters={'limit': 15},
            return_results=True,
        )
        c = query_res[0]
        assert c.embedding is None
        assert c.matches[0].embedding is None
        assert len(c.matches) == NUMBER_OF_DOCS

        for i in range(len(c.matches) - 1):
            assert (
                c.matches[i].scores['cosine'].value
                <= c.matches[i + 1].scores['cosine'].value
            )


def test_search_with_filtering(tmpdir):
    metas = {'workspace': str(tmpdir)}

    docs = docs_with_tags(NUMBER_OF_DOCS)
    docs_query = gen_docs(1)
    columns = ['price', 'float', 'category', 'str']

    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={'dim': DIM, 'columns': columns},
        uses_metas=metas,
    )

    with f:
        f.post(on='/index', inputs=docs)
        query_res = f.post(
            on='/search',
            inputs=docs_query,
            return_results=True,
            parameters={'filter': {'price': {'$lt': 50.0}}},
        )
        assert all([m.tags['price'] < 50 for m in query_res[0].matches])


def test_delete(tmpdir):
    metas = {'workspace': str(tmpdir)}
    docs = gen_docs(NUMBER_OF_DOCS)
    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={
            'dim': DIM,
        },
        uses_metas=metas,
    )
    with f:
        docs[0].tags['parent_tag'] = 'different_value'
        f.post(on='/index', inputs=docs)
        listed_docs = f.post(on='/list', return_results=True)
        assert len(listed_docs) == NUMBER_OF_DOCS
        f.post(
            on='/delete',
            parameters={'filter': {'tags__parent_tag': {'$eq': 'different_value'}}},
        )
        listed_docs = f.post(on='/list', return_results=True)
        assert len(listed_docs) == NUMBER_OF_DOCS - 1
        docs_query = gen_docs(NUMBER_OF_DOCS)
        f.post(on='/search', inputs=docs_query, return_results=True)


def test_get_tags(tmpdir):
    metas = {'workspace': str(tmpdir)}
    docs = DocumentArray(
        [
            Document(
                text='hi',
                embedding=np.random.rand(DIM).astype(np.float32),
                tags={'color': 'red'},
            ),
            Document(
                blob=b'b12',
                embedding=np.random.rand(DIM).astype(np.float32),
                tags={'color': 'blue'},
            ),
            Document(
                blob=b'b12',
                embedding=np.random.rand(DIM).astype(np.float32),
                uri='file_will.never_exist',
            ),
        ]
    )
    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={
            'dim': DIM,
        },
        uses_metas=metas,
    )
    with f:
        f.post(on='/index', inputs=docs)
        response = f.post(on='/tags')
        assert response[0].text == 'tags'
        assert 'tags' in response[0].tags
        assert 'color' in response[0].tags['tags']
        assert response[0].tags['tags']['color'] == ['red', 'blue'] or response[0].tags[
            'tags'
        ]['color'] == ['blue', 'red']


def test_delete_tags(tmpdir):
    metas = {'workspace': str(tmpdir)}
    docs = DocumentArray(
        [
            Document(
                text='hi',
                embedding=np.random.rand(DIM).astype(np.float32),
                tags={'color': 'red'},
            ),
            Document(
                blob=b'b12',
                embedding=np.random.rand(DIM).astype(np.float32),
                tags={'color': 'blue'},
            ),
            Document(
                blob=b'b12',
                embedding=np.random.rand(DIM).astype(np.float32),
                uri='file_will.never_exist',
            ),
            Document(
                blob=b'b12',
                embedding=np.random.rand(DIM).astype(np.float32),
                tags={'greeting': 'hello'},
            ),
        ]
    )
    f = Flow().add(
        uses=NOWAnnLiteIndexer,
        uses_with={
            'dim': DIM,
        },
        uses_metas=metas,
    )
    with f:
        f.post(on='/index', inputs=docs)
        f.post(
            on='/delete',
            parameters={'filter': {'tags__color': {'$eq': 'blue'}}},
        )
        response = f.post(on='/tags')
        assert response[0].text == 'tags'
        assert 'tags' in response[0].tags
        assert 'color' in response[0].tags['tags']
        assert response[0].tags['tags']['color'] == ['red']
        f.post(
            on='/delete',
            parameters={'filter': {'tags__greeting': {'$eq': 'hello'}}},
        )
        response = f.post(on='/tags')
        assert 'greeting' not in response[0].tags['tags']
