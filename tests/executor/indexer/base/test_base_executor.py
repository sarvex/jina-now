import os
from copy import deepcopy

import numpy as np
import pytest
from jina import Document, DocumentArray, Flow
from tests.executors.indexer.base.in_memory_indexer import InMemoryIndexer

from now.executor.indexer.qdrant.executor import NOWQdrantIndexer15

NUMBER_OF_DOCS = 10
DIM = 128


@pytest.mark.parametrize('indexer', [InMemoryIndexer, NOWQdrantIndexer15])
class TestBaseIndexer:
    @pytest.fixture(scope='function', autouse=True)
    def setup(self):
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        compose_yml = os.path.abspath(os.path.join(cur_dir, 'docker-compose.yml'))
        os.system(
            f"docker-compose -f {compose_yml} --project-directory . up  --build -d --remove-orphans"
        )
        yield
        os.system(
            f"docker-compose -f {compose_yml} --project-directory . down --remove-orphans"
        )

    def gen_docs(self, num, has_chunk=False):
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

    def docs_with_tags(self, NUMBER_OF_DOCS):
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

    def test_index(self, tmpdir, indexer):
        """Test indexing does not return anything"""
        metas = {'workspace': str(tmpdir)}
        docs = self.gen_docs(NUMBER_OF_DOCS)
        f = Flow().add(
            uses=indexer,
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
    def test_list(self, tmpdir, offset, limit, has_chunk, indexer):
        """Test list returns all indexed docs"""
        metas = {'workspace': str(tmpdir)}
        docs = self.gen_docs(NUMBER_OF_DOCS, has_chunk=has_chunk)
        f = Flow().add(
            uses=indexer,
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

    def test_search(self, tmpdir, indexer):
        metas = {'workspace': str(tmpdir)}
        docs = self.gen_docs(NUMBER_OF_DOCS)
        docs_query = self.gen_docs(1)
        f = Flow().add(
            uses=indexer,
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

    def test_search_match(self, tmpdir, indexer):
        metas = {'workspace': str(tmpdir)}
        docs = self.gen_docs(NUMBER_OF_DOCS, has_chunk=True)
        docs_query = self.gen_docs(NUMBER_OF_DOCS, has_chunk=True)
        f = Flow().add(
            uses=indexer,
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

    def test_search_with_filtering(self, tmpdir, indexer):
        metas = {'workspace': str(tmpdir)}

        docs = self.docs_with_tags(NUMBER_OF_DOCS)
        docs_query = self.gen_docs(1)
        columns = ['price', 'float', 'category', 'str']

        f = Flow().add(
            uses=indexer,
            uses_with={'dim': DIM, 'columns': columns},
            uses_metas=metas,
        )

        with f:
            f.index(inputs=docs)
            query_res = f.search(
                inputs=docs_query,
                return_results=True,
                parameters={'filter': {'price': {'$lt': 50.0}}},
            )
            assert all([m.tags['price'] < 50 for m in query_res[0].matches])

    def test_delete(self, tmpdir, indexer):
        metas = {'workspace': str(tmpdir)}
        docs = self.gen_docs(NUMBER_OF_DOCS)
        f = Flow().add(
            uses=indexer,
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
            docs_query = self.gen_docs(NUMBER_OF_DOCS)
            f.post(on='/search', inputs=docs_query, return_results=True)

    def test_get_tags(self, tmpdir, indexer):
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
            uses=indexer,
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
            assert response[0].tags['tags']['color'] == ['red', 'blue'] or response[
                0
            ].tags['tags']['color'] == ['blue', 'red']

    def test_delete_tags(self, tmpdir, indexer):
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
            uses=indexer,
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

    @pytest.fixture()
    def documents(self):
        uri = 'https://jina.ai/assets/images/text-to-image-output.png'
        return DocumentArray(
            [
                Document(
                    id="doc1",
                    blob=b"gif...",
                    embedding=np.array([0.3, 0.1, 0.1]),
                    tags={'title': 'blue'},
                    uri=uri,
                    chunks=[
                        Document(
                            id="chunk11",
                            blob=b"jpg...",
                            embedding=np.array([0.1, 0.1]),
                            tags={'title': 'that is rEd for sure'},
                            uri=uri,
                        ),
                        Document(
                            id="chunk12",
                            blob=b"jpg...",
                            embedding=np.array([0.2, 0.1]),
                            tags={'title': 'really bluE'},
                            uri=uri,
                        ),
                    ],
                ),
                Document(
                    id="doc2",
                    blob=b"jpg...",
                    tags={'title': 'red', 'length': 18},
                    uri=uri,
                    embedding=np.array([0.4, 0.1, 0.1]),
                    chunks=[
                        Document(
                            id="chunk21",
                            blob=b"jpg...",
                            embedding=np.array([0.3, 0.1]),
                            tags={'title': 'my red shirt'},
                            uri=uri,
                        ),
                        Document(
                            id="chunk22",
                            blob=b"jpg...",
                            embedding=np.array([0.4, 0.1]),
                            tags={'title': 'red is nice'},
                            uri=uri,
                        ),
                    ],
                ),
                Document(
                    id="doc3",
                    blob=b"jpg...",
                    embedding=np.array([0.5, 0.1, 0.1]),
                    tags={'title': 'blue', 'length': 18},
                    uri=uri,
                    chunks=[
                        Document(
                            id="chunk31",
                            blob=b"jpg...",
                            embedding=np.array([0.5, 0.1]),
                            tags={'title': 'it is red'},
                            uri=uri,
                        ),
                    ],
                ),
                Document(
                    id="doc4",
                    blob=b"jpg...",
                    embedding=np.array([0.6, 0.1, 0.1]),
                    tags={'title': 'blue'},
                    uri=uri,
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
        self, documents, indexer, level, query, embedding, res_ids
    ):
        if indexer == InMemoryIndexer:
            pytest.skip('InMemoryIndexer does not contain the title hack')
        with Flow().add(
            uses=indexer,
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
                if d.uri:
                    assert d.blob == b'', f'got blob {d.blob} for {d.id}'

    def test_no_blob_with_working_uri(self, tmpdir, indexer):
        metas = {'workspace': str(tmpdir)}
        with Flow().add(
            uses=indexer,
            uses_with={
                'dim': 3,
            },
            uses_metas=metas,
        ) as f:
            doc_blob = Document(
                uri='https://jina.ai/assets/images/text-to-image-output.png',
                embedding=np.array([0.1, 0.1, 0.4]),
            ).load_uri_to_blob()

            doc_tens = Document(
                uri='https://jina.ai/assets/images/text-to-image-output.png',
                embedding=np.array([0.1, 0.1, 0.5]),
            ).load_uri_to_image_tensor()

            inputs = DocumentArray(
                [
                    Document(text='hi', embedding=np.array([0.1, 0.1, 0.1])),
                    Document(blob=b'b12', embedding=np.array([0.1, 0.1, 0.2])),
                    Document(
                        blob=b'b12',
                        uri='file_will.never_exist',
                        embedding=np.array([0.1, 0.1, 0.3]),
                    ),
                    doc_blob,
                    doc_tens,
                ]
            )

            f.index(deepcopy(inputs), parameters={})

            response = f.search(Document(embedding=np.array([0.1, 0.1, 0.1])))
            matches = response[0].matches
            assert matches[0].text == inputs[0].text
            assert matches[1].blob == inputs[1].blob
            assert matches[2].blob == inputs[2].blob
            assert matches[3].blob == b''
            assert matches[4].tensor is None
