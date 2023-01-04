import random
from typing import Dict

import numpy as np
from docarray import DocumentArray

from deployment.bff.app.v1.models.search import IndexRequestModel, SearchRequestModel
from deployment.bff.app.v1.routers import helper
from deployment.bff.app.v1.routers.search import index, search
from now.executor.autocomplete import NOWAutoCompleteExecutor2
from now.executor.indexer.elastic import NOWElasticIndexer
from now.executor.preprocessor import NOWPreprocessor


def test_offline_flow(monkeypatch, setup_service_running, base64_image_string):
    """
    Test all executors and bff together without creating a flow.
    The Clip Encoder is mocked because it is an external executor.
    Also, the network call for the bff is monkey patched.
    """
    offline_flow = OfflineFlow()
    offline_client = get_client(offline_flow)
    monkeypatch.setattr(helper, 'get_jina_client', lambda **kwargs: offline_client)

    index_result = index(
        IndexRequestModel(
            data=[
                (
                    {
                        'product_title': {'text': 'test'},
                        'product_image': {'blob': base64_image_string},
                    },
                    {'price': '4.50'},
                )
            ],
        )
    )
    assert index_result == []
    search_result = search(
        SearchRequestModel(
            query={'query': {'text': 'girl on motorbike'}},
        )
    )
    assert (
        '_ocr_detector_text_in_doc'
        in search_result[0].matches[0].product_image.chunks[0].tags
    )
    assert search_result[0].matches[0].product_title.chunks[0].content == 'fancy title'
    assert (
        search_result[0].matches[0].product_description.chunks[0].content
        == 'this is a product'
    )


class OfflineFlow:
    def __init__(self):
        # definition of executors:
        self.autocomplete = NOWAutoCompleteExecutor2()
        self.preprocessor = NOWPreprocessor()
        self.encoder = MockedEncoder()
        document_mappings = [
            ('clip', 5, ['product_title', 'product_image', 'product_description']),
        ]
        self.indexer = NOWElasticIndexer(
            document_mappings=document_mappings,
            hosts='http://localhost:9200',
            index_name=f"test-index-{random.randint(0, 10000)}",
        )

    def post(self, endpoint, inputs, parameters: Dict[str, str], *args, **kwargs):
        # call executors:
        docs = inputs if isinstance(inputs, DocumentArray) else DocumentArray(inputs)
        if 'search' in endpoint:
            docs = self.autocomplete.search_update(docs, parameters, *args, **kwargs)
        preprocessed_docs = self.preprocessor.preprocess(
            docs, parameters, *args, **kwargs
        )
        encoded_docs = self.encoder.encode(
            preprocessed_docs, parameters, *args, **kwargs
        )
        indexer_docs = getattr(self.indexer, endpoint.replace('/', ''))(
            {'clip': encoded_docs}, parameters, *args, **kwargs
        )
        return indexer_docs


def get_client(offline_flow):
    class Client:
        def post(self, endpoint, inputs, parameters, *args, **kwargs):
            # definition of executors:
            docs = offline_flow.post(endpoint, inputs, parameters, *args, **kwargs)
            return docs

    return Client()


class MockedEncoder:
    def encode(self, docs, parameters, *args, **kwargs):
        docs_encode = docs[parameters['access_paths']]
        for doc in docs_encode:
            doc.embedding = np.array([1, 2, 3, 4, 5])
        return docs
