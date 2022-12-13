import random

import numpy as np
import pytest
from docarray import dataclass
from docarray.typing import Image, Text
from jina import Document, DocumentArray, Executor, Flow, requests

from now.constants import TAG_INDEXER_DOC_HAS_TEXT, TAG_OCR_DETECTOR_TEXT_IN_DOC
from now.executor.indexer.elastic import NOWElasticIndexer

NUMBER_OF_DOCS = 10
DIM = 128
MAX_RETRIES = 20


class DummyEncoder1(Executor):
    @requests
    def foo(self, docs: DocumentArray, **kwargs):
        pass


class DummyEncoder2(Executor):
    @requests
    def foo(self, docs: DocumentArray, **kwargs):
        pass


@pytest.mark.parametrize(
    'indexer,setup',
    [
        (NOWElasticIndexer, 'setup_service_running'),
    ],
)
class TestBaseIndexer:
    def get_docs(self, num):
        prices = [10.0, 25.0, 50.0, 100.0]
        colors = ['blue', 'red']
        greetings = ['hello']
        res = DocumentArray()

        @dataclass
        class MMDoc:
            title: Text

        k = np.random.random((num, DIM)).astype(np.float32)
        for i in range(num):
            doc = Document(
                MMDoc(
                    title='parent',
                )
            )
            doc.title.embedding = k[i]
            doc.id = str(i)
            doc.tags['parent_tag'] = 'value'
            doc.tags['price'] = random.choice(prices)
            doc.tags['color'] = random.choice(colors)
            doc.tags['greeting'] = random.choice(greetings)
            res.append(doc)
        return res

    @pytest.fixture
    def random_index_name(self):
        return f"test-index-{random.randint(0, 10000)}"

    def test_index(self, tmpdir, indexer, setup, random_index_name, request):
        """Test indexing does not return anything"""
        if setup:
            request.getfixturevalue(setup)
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )
        with f:
            result = f.post(on='/index', inputs=docs, return_results=True)
            assert len(result) == 0

    @pytest.mark.parametrize(
        'offset, limit', [(0, 10), (10, 0), (0, 0), (10, 10), (None, None)]
    )
    def test_list(self, tmpdir, offset, limit, indexer, setup, random_index_name):
        """Test list returns all indexed docs"""
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )
        with f:
            parameters = {}
            if offset is not None:
                parameters.update({'offset': offset, 'limit': limit})

            f.post(on='/index', inputs=docs, parameters=parameters)
            list_res = f.post(on='/list', parameters=parameters, return_results=True)
            if offset is None:
                l = NUMBER_OF_DOCS
            else:
                l = max(limit - offset, 0)
            assert len(list_res) == l
            if l > 0:
                assert len(list_res[0].chunks) == 1
                assert isinstance(list_res[0].title, Document)
                assert len(set([d.id for d in list_res])) == l
                assert [d.tags['parent_tag'] for d in list_res] == ['value'] * l

    def test_search(self, tmpdir, indexer, setup, random_index_name):
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        docs_query = self.get_docs(1)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )
        with f:
            f.post(on='/index', inputs=docs)

            query_res = f.post(on='/search', inputs=docs_query, return_results=True)
            assert len(query_res) == 1

            for i in range(len(query_res[0].matches) - 1):
                assert (
                    query_res[0].matches[i].scores['cosine'].value
                    >= query_res[0].matches[i + 1].scores['cosine'].value
                )

    def test_search_match(self, tmpdir, indexer, setup, random_index_name):
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        docs_query = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
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
                    >= c.matches[i + 1].scores['cosine'].value
                )

    def test_search_with_filtering(self, tmpdir, indexer, setup, random_index_name):
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        docs_query = self.get_docs(1)

        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )

        with f:
            f.index(inputs=docs)
            query_res = f.search(
                inputs=docs_query,
                return_results=True,
                parameters={'filter': {'price': {'$lt': 50.0}}},
            )
            assert all([m.tags['price'] < 50 for m in query_res[0].matches])

    def test_delete(self, tmpdir, indexer, setup, random_index_name):
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )
        with f:
            docs[0].tags['parent_tag'] = 'different_value'
            f.post(on='/index', inputs=docs)
            listed_docs = f.post(on='/list', return_results=True)
            assert len(listed_docs) == NUMBER_OF_DOCS
            f.post(
                on='/delete',
                parameters={'filter': {'parent_tag': {'$eq': 'different_value'}}},
            )
            listed_docs = f.post(on='/list', return_results=True)
            assert len(listed_docs) == NUMBER_OF_DOCS - 1
            docs_query = self.get_docs(NUMBER_OF_DOCS)
            f.post(on='/search', inputs=docs_query, return_results=True)

    def test_get_tags(self, tmpdir, indexer, setup, random_index_name):
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )
        with f:
            f.post(on='/index', inputs=docs)
            response = f.post(on='/tags')
            assert response[0].text == 'tags'
            assert 'tags' in response[0].tags
            assert 'color' in response[0].tags['tags']
            assert sorted(response[0].tags['tags']['color']) == sorted(['red', 'blue'])

    def test_delete_tags(self, tmpdir, indexer, setup, random_index_name):
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['title']),
                        ('dummy_encoder2', DIM, ['title']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )
        with f:
            f.post(on='/index', inputs=docs)
            f.post(
                on='/delete',
                parameters={'filter': {'color': {'$eq': 'blue'}}},
            )
            response = f.post(on='/tags')
            assert response[0].text == 'tags'
            assert 'tags' in response[0].tags
            assert 'color' in response[0].tags['tags']
            assert 'blue' not in response[0].tags['tags']['color']
            f.post(
                on='/delete',
                parameters={'filter': {'greeting': {'$eq': 'hello'}}},
            )
            response = f.post(on='/tags')
            assert 'hello' not in response[0].tags['tags']['greeting']

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
                            tags={
                                'title': 'that is rEd for sure',
                                TAG_OCR_DETECTOR_TEXT_IN_DOC: "r t",
                            },
                            uri=uri,
                        ),
                        Document(
                            id="chunk12",
                            blob=b"jpg...",
                            embedding=np.array([0.2, 0.1]),
                            tags={
                                'title': 'really bluE',
                                TAG_OCR_DETECTOR_TEXT_IN_DOC: "r t",
                            },
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
                            tags={
                                'title': 'my red shirt',
                                TAG_OCR_DETECTOR_TEXT_IN_DOC: "red shirt",
                            },
                            uri=uri,
                        ),
                        Document(
                            id="chunk22",
                            blob=b"jpg...",
                            embedding=np.array([0.4, 0.1]),
                            tags={
                                'title': 'red is nice',
                                TAG_OCR_DETECTOR_TEXT_IN_DOC: "red shirt",
                            },
                            uri=uri,
                        ),
                    ],
                ),
                Document(
                    id="doc3",
                    blob=b"jpg...",
                    embedding=np.array([0.5, 0.1, 0.1]),
                    tags={'length': 18},
                    uri=uri,
                    chunks=[
                        Document(
                            id="chunk31",
                            blob=b"jpg...",
                            embedding=np.array([0.5, 0.1]),
                            tags={
                                'title': 'blue red',
                                TAG_OCR_DETECTOR_TEXT_IN_DOC: "i iz ret",
                            },
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

    @pytest.mark.skip('not implemented for NOWElasticIndexer')
    @pytest.mark.parametrize(
        'query,embedding,res_ids',
        [
            ('blue', [0.5, 0.1], ['chunk12', 'chunk31', 'chunk22']),
            ('red', [0.5, 0.1], ['chunk11', 'chunk31', 'chunk22']),
        ],
    )
    def test_search_chunk_using_sum_ranker(
        self, documents, indexer, setup, query, embedding, res_ids, tmpdir
    ):
        metas = {'workspace': str(tmpdir)}
        documents = DocumentArray([Document(chunks=[doc]) for doc in documents])
        with Flow().add(
            uses=indexer,
            uses_with={
                "dim": len(embedding),
                'columns': ['title', 'str', TAG_INDEXER_DOC_HAS_TEXT, 'bool'],
            },
            uses_metas=metas,
        ) as f:
            f.index(
                documents,
            )
            result = f.search(
                Document(
                    chunks=Document(
                        chunks=Document(
                            id="chunk_search",
                            text=query,
                            embedding=np.array(embedding),
                        ),
                    ),
                ),
                return_results=True,
            )
            print('all match ids', [match.id for match in result[0].matches])
            for d, res_id in zip(result[0].matches, res_ids):
                assert d.id == res_id
                if d.uri:
                    assert d.blob == b'', f'got blob {d.blob} for {d.id}'

    def test_no_blob_or_tensor_on_matches(
        self, tmpdir, indexer, setup, random_index_name
    ):
        @dataclass
        class Pic:
            pic: Image

        mdoc = Pic(pic='https://jina.ai/assets/images/text-to-image-output.png')
        doc_with_tensor = Document(mdoc)
        doc_with_tensor.pic.embedding = np.random.random([DIM])
        doc_with_blob = Document(mdoc)
        doc_with_blob.pic.load_uri_to_blob()
        doc_with_blob.pic.embedding = np.random.random([DIM])
        docs = DocumentArray([doc_with_tensor, doc_with_blob])

        metas = {'workspace': str(tmpdir)}
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=indexer,
                uses_with={
                    'hosts': 'http://localhost:9200',
                    'index_name': random_index_name,
                    'document_mappings': [
                        ('dummy_encoder1', DIM, ['pic']),
                        ('dummy_encoder2', DIM, ['pic']),
                    ],
                },
                uses_metas=metas,
                needs=['dummy_encoder1', 'dummy_encoder2'],
                no_reduce=True,
            )
        )
        with f:
            f.post(on='/index', inputs=docs)
            query_doc = Document(
                Pic(pic='https://jina.ai/assets/images/text-to-image-output.png')
            )
            query_doc.pic.embedding = np.random.random([DIM])
            response = f.post(
                on='/search',
                inputs=DocumentArray([query_doc]),
                return_results=True,
            )
            matches = response[0].matches
            assert matches[0].pic.blob == b''
            assert matches[1].pic.blob == b''
            assert matches[1].pic.tensor is None
            assert matches[0].pic.tensor is None

    @pytest.mark.skip('not implemented for NOWElasticIndexer')
    def test_curate_endpoint(self, tmpdir, indexer, setup):
        """Test indexing does not return anything"""
        metas = {'workspace': str(tmpdir)}
        docs = self.get_docs(NUMBER_OF_DOCS)
        docs.append(
            Document(
                chunks=[
                    Document(
                        chunks=[
                            Document(
                                id='c1',
                                embedding=np.random.random(DIM).astype(np.float32),
                                tags={'color': 'red'},
                                uri='uri2',
                            ),
                            Document(
                                id='c2',
                                embedding=np.random.random(DIM).astype(np.float32),
                                tags={'color': 'red'},
                                uri='uri2',
                            ),
                        ]
                    )
                ]
            )
        )
        f = Flow().add(
            uses=indexer,
            uses_with={
                'dim': DIM,
            },
            uses_metas=metas,
        )
        with f:
            f.post(
                on='/curate',
                parameters={
                    'query_to_filter': {
                        'query1': [
                            {'uri': {'$eq': 'uri2'}},
                            {'tags__color': {'$eq': 'red'}},
                        ],
                    }
                },
            )
            f.index(docs, return_results=True)
            result = f.search(
                inputs=Document(
                    chunks=[
                        Document(
                            chunks=[
                                Document(text='query1', embedding=np.array([0.1] * 128))
                            ]
                        ),
                    ]
                ),
                return_results=True,
            )
            assert len(result) == 1
            assert result[0].matches[0].uri == 'uri2'
            assert result[0].matches[1].uri != 'uri2'  # no duplicated results
            assert result[0].matches[0].tags['color'] == 'red'

            # not crashing in case of curated list + non-curated query
            f.search(
                inputs=Document(
                    chunks=[
                        Document(
                            chunks=[
                                Document(
                                    text='another string',
                                    embedding=np.array([0.1] * 128),
                                )
                            ]
                        ),
                    ]
                )
            )
