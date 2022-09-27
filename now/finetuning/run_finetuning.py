""" This module is the entry point to the finetuning package."""
import os
import random
import string
import sys
from copy import deepcopy
from time import sleep
from typing import Any, Dict, Tuple

import cowsay
import finetuner
import numpy as np
from docarray import DocumentArray
from finetuner.callback import EarlyStopping, EvaluationCallback

from now.apps.base.app import JinaNOWApp
from now.constants import ModelNames
from now.deployment.deployment import cmd, terminate_wolf
from now.deployment.flow import deploy_flow
from now.finetuning.dataset import FinetuneDataset, build_finetuning_dataset
from now.finetuning.settings import FinetuneSettings
from now.log import time_profiler, yaspin_extended
from now.now_dataclasses import UserInput
from now.run_backend import call_flow
from now.utils import sigmap


@time_profiler
def finetune(
    finetune_settings: FinetuneSettings,
    app_instance: JinaNOWApp,
    dataset: DocumentArray,
    user_input: UserInput,
    env_dict: Dict,
    kubectl_path: str,
) -> Tuple[str, str]:
    """If possible, applies finetuning and updates finetune_settings.finetuned_model_name accordingly.

    Performs the finetuning procedure:
     1. If embeddings are not present -> compute them using a k8s deployed flow
     2. If bi-modal, prepare the embeddings by concatenating zeros for the opposing modality
     3. Create model and run finetuning, get path to the tuned model and return

    Note, for music we use cached models because the datasets are too large and consume too much time

    :param user_input: The configured user input object
    :param env_dict: environment variables for flow.yml file
    :param dataset: The dataset with the finetuning labels on all documents. Embeddings are optional and can
        be computed on the fly
    :param finetune_settings: Mainly parameter configuration for the finetuner.fit
    :param kubectl_path: Path to the kubectl binary on the system

    :return: artifact of finetuned model and token required for FinetunerExecutor
    """
    if not finetune_settings.perform_finetuning:
        return

    print(f'🔧 Perform finetuning!')
    if finetune_settings.add_embeddings:
        dataset = _maybe_add_embeddings(
            app_instance=app_instance,
            user_input=user_input,
            env_dict=env_dict,
            dataset=dataset,
            kubectl_path=kubectl_path,
        )

    dataset = dataset.shuffle(42)

    if finetune_settings.bi_modal:
        _prepare_dataset_bi_modal(dataset)

    finetune_ds = build_finetuning_dataset(dataset, finetune_settings)

    if finetune_settings.add_embeddings:
        assert all([d.embedding is not None for d in finetune_ds.index])

    return _finetune_model(finetune_ds, finetune_settings)


def _get_model_options(finetune_settings: FinetuneSettings) -> Dict[str, Any]:
    """
    Returns additional model options for fine-tuning specific for each model.

    :param finetune_settings: Fine-tuning settings.
    :return: Dictionary of model parameters.
    """
    if finetune_settings.model_name == 'mlp':
        input_size = (
            finetune_settings.pre_trained_embedding_size
            if not finetune_settings.bi_modal
            else finetune_settings.pre_trained_embedding_size * 2
        )
        return {
            'input_size': input_size,
            'hidden_sizes': finetune_settings.hidden_sizes,
            'l2': True,
            'bias': False if finetune_settings.bi_modal else True,
        }
    return {}


@time_profiler
def _finetune_model(
    finetune_ds: FinetuneDataset,
    finetune_settings: FinetuneSettings,
) -> Tuple[str, str]:
    print('💪 fine-tuning:')
    finetuner.login()

    callbacks = [
        EarlyStopping(
            monitor='ndcg',
            patience=finetune_settings.early_stopping_patience,
        ),
    ]
    if finetune_settings.model_name != ModelNames.CLIP:
        callbacks.append(
            EvaluationCallback(
                finetune_ds.val_query,
                finetune_ds.val_index,
                limit=finetune_settings.eval_match_limit,
            )
        )
    if 'NOW_CI_RUN' in os.environ:
        experiment_name = 'now-ci-finetuning-' + _get_random_string(8)
    else:
        experiment_name = 'now-finetuning-' + _get_random_string(8)
    print(f'🧪 Creating finetune experiment ({experiment_name})')
    finetuner.create_experiment(experiment_name)

    run = finetuner.fit(
        model=finetune_settings.model_name,
        model_options=_get_model_options(finetune_settings),
        loss=finetune_settings.loss,
        train_data=finetune_ds.train,
        experiment_name=experiment_name,
        eval_data=finetune_ds.val,
        callbacks=callbacks,
        learning_rate=finetune_settings.learning_rate,
        run_name=experiment_name,
        epochs=finetune_settings.epochs,
    )

    run_failed = False
    with yaspin_extended(
        sigmap=sigmap, text='Waiting for finetune job to be assigned', color='green'
    ) as spinner:
        while run.status()['status'] == 'CREATED' and not run_failed:
            run_failed = run.status()['status'] == 'FAILED'
            sleep(1)
        if run_failed:
            spinner.fail('👎')
            print(run.logs())
            sys.exit(0)
        else:
            spinner.ok('👍')

    with yaspin_extended(
        sigmap=sigmap, text='Running finetune job', color='green'
    ) as spinner:
        while run.status()['status'] == 'STARTED' and not run_failed:
            run_failed = run.status()['status'] == 'FAILED'
            sleep(1)
        if run_failed:
            spinner.fail('👎')
            print(run.logs())
            sys.exit(0)
        else:
            spinner.ok('👍')

    if run.status()['status'] == 'FAILED':
        print('❌ finetune failed. See logs for details')
        print(run.status())
        print(run.logs())
        sys.exit(0)

    print('🧠 Perfect! Early stopping triggered since accuracy is great already')

    finetune_artifact = run.artifact_id
    token = finetuner.get_token()

    return finetune_artifact, token


def get_bi_modal_embedding(doc) -> np.ndarray:
    attributes = [doc.text, doc.blob]
    if not any(attributes) or all(attributes):
        raise ValueError(
            f'Received doc (id={doc.id}) with either no text and blob or both.'
        )
    zeros = np.zeros(doc.embedding.shape)
    if doc.text:
        order = (zeros, doc.embedding)
    else:
        order = (doc.embedding, zeros)
    return np.concatenate(order, dtype=np.float32)


def _prepare_dataset_bi_modal(dataset: DocumentArray):
    for doc in dataset:
        doc.embedding = get_bi_modal_embedding(doc)


def _get_random_string(length) -> str:
    import time

    t = 1000 * time.time()  # current time in milliseconds
    random.seed(int(t) % 2**32)
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


_KS_NAMESPACE = 'embed-now'


@time_profiler
def _maybe_add_embeddings(
    app_instance: JinaNOWApp,
    user_input: UserInput,
    env_dict: Dict,
    dataset: DocumentArray,
    kubectl_path: str,
):
    with yaspin_extended(
        sigmap=sigmap, text="Check if embeddings already exist", color="green"
    ) as spinner:
        if all([d.embedding is not None for d in dataset]):
            spinner.ok('👍')
            return dataset
        else:
            spinner.fail('👎')

    # creates list of indices of documents without embedding
    documents_without_embedding = DocumentArray(
        list(filter(lambda d: d.embedding is None, dataset))
    )

    app_instance.set_flow_yaml()
    client, _, _, gateway_host_internal, _, = deploy_flow(
        deployment_type=user_input.deployment_type,
        flow_yaml=app_instance.flow_yaml,
        ns=_KS_NAMESPACE,
        env_dict=env_dict,
        kubectl_path=kubectl_path,
    )
    print(f'▶ create embeddings for {len(documents_without_embedding)} documents')
    result = call_flow(
        client=client,
        dataset=documents_without_embedding,
        max_request_size=app_instance.max_request_size,
        endpoint='/encode',
        parameters={'user_input': deepcopy(user_input.__dict__)},
        return_results=True,
    )

    for doc in result:
        dataset[doc.id].embedding = doc.embedding

    if not all([d.embedding is not None for d in dataset]):
        print(
            "Some docs slipped through and still have no embeddings. Re-run the program or continue with "
            "the next step."
        )

    # removes normal flow as it is unused from now on
    if user_input.deployment_type == 'local':
        with yaspin_extended(
            sigmap=sigmap,
            text=f"Remove encoding namespace ({_KS_NAMESPACE}) NOW from kind-jina-now",
            color="green",
        ) as spinner:
            cmd(f'{kubectl_path} delete ns {_KS_NAMESPACE}')
            spinner.ok('💀')
        cowsay.cow(f'{_KS_NAMESPACE} namespace removed from kind-jina-now')
    elif user_input.deployment_type == 'remote':
        flow_id = gateway_host_internal.replace('grpcs://nowapi-', '').replace(
            '.wolf.jina.ai', ''
        )
        terminate_wolf(flow_id=flow_id)

    return dataset
