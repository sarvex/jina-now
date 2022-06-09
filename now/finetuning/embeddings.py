""" This module implements functionality to fine tune on the music dataset """

import math
import tempfile

from docarray import DocumentArray
from jina import Client, Flow
from tqdm import tqdm

from now.dataclasses import UserInput
from now.deployment.flow import _ExecutorConfig, batch, deploy_k8s

_KS_NAMESPACE = 'embed-now'


def embed_now(user_input: UserInput, dataset: DocumentArray, kubectl_path: str):
    documents_without_embedding = DocumentArray(
        list(filter(lambda d: d.embedding is None, dataset))
    )

    flow = Flow(name=_KS_NAMESPACE, port_expose=8080, cors=True).add(
        **get_encoder_config(user_input)._asdict()
    )
    result = DocumentArray()
    with tempfile.TemporaryDirectory() as tmpdir:
        gateway_host, gateway_port, _, _ = deploy_k8s(
            flow, _KS_NAMESPACE, tmpdir, kubectl_path=kubectl_path
        )
        client = Client(host=gateway_host, port=gateway_port)
        print(f'▶ create embeddings for {len(documents_without_embedding)} documents')
        for x in tqdm(
            batch(documents_without_embedding, 16),
            total=math.ceil(len(documents_without_embedding) / 16),
        ):
            response = client.post('/index', request_size=16, inputs=x)
            result.extend(response)

    for doc in result:
        dataset[doc.id].embedding = doc.embedding


def get_encoder_config(encoder_uses: str, artifact: str) -> _ExecutorConfig:
    """
    Gets the correct Executor running the pre-trained model given the user configuration.
    :param user_input: Configures user input.
    :return: Small data-transfer-object with information about the executor
    """
    return _ExecutorConfig(
        name='encoder',
        uses=f'jinahub+docker://{encoder_uses}',
        uses_with={'pretrained_model_name_or_path': artifact},
    )
