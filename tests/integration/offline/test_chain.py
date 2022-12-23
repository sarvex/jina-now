import numpy as np

from deployment.bff.app.v1.models.search import IndexRequestModel, SearchRequestModel
from deployment.bff.app.v1.routers import helper
from deployment.bff.app.v1.routers.search import index, search
from now.executor.autocomplete import NOWAutoCompleteExecutor2
from now.executor.indexer.elastic import NOWElasticIndexer
from now.executor.preprocessor import NOWPreprocessor


def test_chain(monkeypatch, setup_service_running, base64_image_string):
    """
    Test all executors and bff together without creating a flow.
    The Clip Encoder is mocked because it is an external executor.
    Also, the network call for the bff is monkey patched.
    """
    monkeypatch.setattr(helper, 'get_jina_client', mocked_get_jina_client)
    index_result = index(
        IndexRequestModel(
            data=[({'text': 'test', 'image': base64_image_string}, {'price': '4.50'})],
        )
    )
    print(index_result)

    search_result = search(
        SearchRequestModel(
            query={'text': 'girl on motorbike'},
        )
    )
    print(search_result)


class MockedEncoder:
    def encode(self, docs, parameters, *args, **kwargs):
        docs = docs[parameters['access_paths']]
        for doc in docs:
            doc.embedding = np.array([1, 2, 3, 4, 5])


def mocked_get_jina_client():
    class Client:
        def post(self, endpoint, inputs, parameters, *args, **kwargs):
            # definition of executors:
            autocomplete = NOWAutoCompleteExecutor2()
            preprocessor = NOWPreprocessor()
            encoder = MockedEncoder()
            indexer = NOWElasticIndexer()

            # call executors:
            docs = autocomplete.autocomplete(inputs, parameters, *args, **kwargs)
            docs = preprocessor.preprocess(docs, parameters, *args, **kwargs)
            docs = encoder.encode(docs, parameters, *args, **kwargs)
            docs = getattr(indexer, endpoint.replace('/', ''))(
                docs, parameters, *args, **kwargs
            )
            return docs

    return Client
