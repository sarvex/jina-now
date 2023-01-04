import random

import numpy as np
import pytest
from docarray import dataclass
from docarray.typing import Text
from jina import Document, DocumentArray, Executor, Flow, requests

from now.constants import TAG_INDEXER_DOC_HAS_TEXT, TAG_OCR_DETECTOR_TEXT_IN_DOC
from now.executor.indexer.elastic import NOWElasticIndexer
from now.executor.preprocessor import NOWPreprocessor

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


class TestBaseIndexerElastic:
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
                    title=f'parent_{i}',
                )
            )
            doc = NOWPreprocessor().preprocess(DocumentArray(doc), {})[0]
            doc.title.chunks[0].embedding = k[i]
            doc.id = str(i)
            doc.tags['parent_tag'] = 'value'
            doc.tags['price'] = random.choice(prices)
            doc.tags['color'] = random.choice(colors)
            doc.tags['greeting'] = random.choice(greetings)
            res.append(doc)
        return res

    def get_query(self):
        @dataclass
        class MMQuery:
            query_text: Text

        q = Document(MMQuery(query_text='query_1'))
        da = NOWPreprocessor().preprocess(DocumentArray([q]), {})
        da[0].query_text.chunks[0].embedding = np.random.random(DIM)
        return da

    @pytest.fixture
    def random_index_name(self):
        return f"test-index-{random.randint(0, 10000)}"

    @pytest.fixture(scope='function', autouse=True)
    def metas(self, tmpdir):
        return {'workspace': str(tmpdir)}

    def test_index(self, metas, setup_service_running, random_index_name, request):
        """Test indexing does not return anything"""
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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
    def test_list(self, metas, offset, limit, setup_service_running, random_index_name):
        """Test list returns all indexed docs"""
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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

    def test_search(self, metas, setup_service_running, random_index_name):
        docs = self.get_docs(NUMBER_OF_DOCS)
        docs_query = self.get_query()
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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

    def test_search_match(self, metas, setup_service_running, random_index_name):
        docs = self.get_docs(NUMBER_OF_DOCS)
        docs_query = self.get_query()
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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

    def test_search_with_filtering(
        self, metas, setup_service_running, random_index_name
    ):
        docs = self.get_docs(NUMBER_OF_DOCS)
        docs_query = self.get_query()

        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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
                parameters={'filter': {'tags__price': {'$lt': 50.0}}},
            )
            assert all([m.tags['price'] < 50 for m in query_res[0].matches])

    def test_delete(self, metas, setup_service_running, random_index_name):
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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
                parameters={'filter': {'tags__parent_tag': {'$eq': 'different_value'}}},
            )
            listed_docs = f.post(on='/list', return_results=True)
            assert len(listed_docs) == NUMBER_OF_DOCS - 1
            docs_query = self.get_query()
            f.post(on='/search', inputs=docs_query, return_results=True)

    def test_get_tags(self, metas, setup_service_running, random_index_name):
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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

    def test_delete_tags(self, metas, setup_service_running, random_index_name):
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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
                parameters={'filter': {'tags__color': {'$eq': 'blue'}}},
            )
            response = f.post(on='/tags')
            assert response[0].text == 'tags'
            assert 'tags' in response[0].tags
            assert 'color' in response[0].tags['tags']
            assert 'blue' not in response[0].tags['tags']['color']
            f.post(
                on='/delete',
                parameters={'filter': {'tags__greeting': {'$eq': 'hello'}}},
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
        self, metas, documents, setup_service_running, query, embedding, res_ids
    ):
        documents = DocumentArray([Document(chunks=[doc]) for doc in documents])
        with Flow().add(
            uses=NOWElasticIndexer,
            uses_with={
                "dim": len(embedding),
                'columns': [['title', 'str'], [TAG_INDEXER_DOC_HAS_TEXT, 'bool']],
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

    def test_curate_endpoint(self, metas, setup_service_running, random_index_name):
        """Test indexing does not return anything"""
        docs = self.get_docs(NUMBER_OF_DOCS)
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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
            f.index(docs)
            f.post(
                on='/curate',
                parameters={
                    'query_to_filter': {
                        'query_1': [
                            {'text': {'$eq': 'parent_1'}},
                            {'tags__color': {'$eq': 'red'}},
                        ],
                        'query_2': [
                            {'text': {'$eq': 'parent_2'}},
                        ],
                    }
                },
            )
            query_doc = self.get_query()
            result = f.search(
                inputs=query_doc,
                return_results=True,
            )

            assert len(result) == 1
            assert result[0].matches[0].title.chunks[0].text == 'parent_1'
            assert (
                result[0].matches[1].title.chunks[0].text != 'parent_1'
            )  # no duplicated results
            assert result[0].matches[1].tags['color'] == 'red'

            # not crashing in case of curated list + non-curated query
            non_curated_query = self.get_query()
            non_curated_query[0].query_text.text = 'parent_x'
            f.search(inputs=non_curated_query)

    def test_curate_endpoint_incorrect(
        self, metas, setup_service_running, random_index_name
    ):
        f = (
            Flow()
            .add(uses=DummyEncoder1, name='dummy_encoder1')
            .add(uses=DummyEncoder2, name='dummy_encoder2')
            .add(
                uses=NOWElasticIndexer,
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
            with pytest.raises(Exception):
                f.post(
                    on='/curate',
                    parameters={'queryfilter': {}},
                )
