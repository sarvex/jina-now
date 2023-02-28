from typing import Dict

import numpy as np
from docarray import DocumentArray

from now.executor.autocomplete import NOWAutoCompleteExecutor2
from now.executor.gateway.bff.app.v1.routers import helper
from now.executor.indexer.elastic import NOWElasticIndexer
from now.executor.preprocessor import NOWPreprocessor


class OfflineFlow:
    def __init__(self, monkeypatch, user_input_dict):
        # definition of executors:
        self.autocomplete = NOWAutoCompleteExecutor2(user_input_dict=user_input_dict)
        self.preprocessor = NOWPreprocessor(user_input_dict=user_input_dict)
        self.encoder = MockedEncoder()
        document_mappings = [
            [
                'clip',
                5,
                [
                    'product_title',
                    'product_image',
                    'product_description',
                ],
            ]
        ]
        self.indexer = NOWElasticIndexer(
            user_input_dict=user_input_dict,
            document_mappings=document_mappings,
        )
        self.mock_client(monkeypatch)

    def mock_client(self, monkeypatch):
        offline_client = get_client(self)
        monkeypatch.setattr(helper, 'get_jina_client', lambda **kwargs: offline_client)

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
        def post(self, on, inputs, parameters, *args, **kwargs):
            # definition of executors:
            docs = offline_flow.post(on, inputs, parameters, *args, **kwargs)
            return docs

    return Client()


class MockedEncoder:
    def encode(self, docs, parameters, *args, **kwargs):
        docs_encode = docs[parameters['access_paths']]
        for doc in docs_encode:
            doc.embedding = np.array([1, 2, 3, 4, 5])
        return docs
