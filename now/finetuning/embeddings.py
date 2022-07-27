""" This module implements functionality to fine tune on the music dataset """
from typing import Dict

from docarray import DocumentArray

from now.deployment.flow import deploy_flow
from now.log import time_profiler
from now.run_backend import call_index

_KS_NAMESPACE = 'embed-now'


@time_profiler
def embed_now(
    deployment_type: str,
    flow_yaml: str,
    env_dict: Dict,
    dataset: DocumentArray,
    kubectl_path: str,
):
    documents_without_embedding = DocumentArray(
        list(filter(lambda d: d.embedding is None, dataset))
    )

    client, _, _, _, _, = deploy_flow(
        deployment_type=deployment_type,
        flow_yaml=flow_yaml,
        ns=_KS_NAMESPACE,
        env_dict=env_dict,
        kubectl_path=kubectl_path,
    )
    print(f'▶ create embeddings for {len(documents_without_embedding)} documents')
    result = call_index(client=client, dataset=documents_without_embedding)

    for doc in result:
        dataset[doc.id].embedding = doc.embedding
