""" This module is the entry point to the finetuning package."""
import os
import sys
import tempfile
from contextlib import contextmanager
from os.path import join as osp
from time import sleep
from typing import Dict

import finetuner
import numpy as np
from docarray import DocumentArray
from finetuner.callback import BestModelCheckpoint, EarlyStopping, EvaluationCallback
from yaspin import yaspin

from now.dataclasses import UserInput
from now.finetuning.dataset import FinetuneDataset, build_finetuning_dataset
from now.finetuning.embeddings import embed_now
from now.finetuning.settings import FinetuneSettings
from now.hub.hub import push_to_hub
from now.log import yaspin_extended
from now.utils import sigmap

_BASE_SAVE_DIR = 'now/hub/head_encoder'


def finetune_now(
    user_input: UserInput,
    dataset: DocumentArray,
    finetune_settings: FinetuneSettings,
    pre_trained_head_map: Dict[str, str],
    kubectl_path: str,
    encoder_uses: str,
    encoder_uses_with: Dict,
):
    """
    Performs the finetuning procedure:
     1. If embeddings are not present -> compute them using a k8s deployed flow
     2. If bi-modal, prepare the embeddings by concatenating zeros for the opposing modality
     3. Create model and run finetuning, get path to the tuned model and return

    Note, for music we use cached models because the datasets are too large and consume too much time

    :param user_input: The configured user input object
    :param dataset: The dataset with the finetuning labels on all documents. Embeddings are optional and can
        be computed on the fly
    :param finetune_settings: Mainly parameter configuration for the finetuner.fit
    :param kubectl_path: Path to the kubectl binary on the system

    :return: Path to the tuned model.
    """
    if pre_trained_head_map is not None and user_input.data in pre_trained_head_map:
        print(f'⚡️ Using cached hub model for speed')
        return pre_trained_head_map[user_input.data]
    dataset = _maybe_add_embeddings(
        encoder_uses, encoder_uses_with, dataset, kubectl_path
    )

    dataset = dataset.shuffle(42)

    if finetune_settings.bi_modal:
        _prepare_dataset_bi_modal(dataset)

    finetune_ds = build_finetuning_dataset(dataset, finetune_settings)

    with _finetune_dir() as save_dir:

        finetuned_model_path = _finetune_layer(
            user_input.data, finetune_ds, finetune_settings, save_dir
        )

        executor_name = push_to_hub(save_dir)
    return executor_name


def _finetune_layer(
    data_name: str,
    finetune_ds: FinetuneDataset,
    finetune_settings: FinetuneSettings,
    save_dir: str,
) -> str:
    for ds_name, ds in finetune_ds.as_dict().items():
        for doc in ds:
            doc.tensor = doc.embedding.astype('float32')
            doc.embedding = None

    assert all([d.embedding is not None for d in finetune_ds.index])

    save_dir = os.path.join(save_dir, 'now', 'hub', 'head_encoder')
    os.makedirs(save_dir, exist_ok=True)

    print('💪 fine-tuning:')
    input_size = (
        finetune_settings.pre_trained_embedding_size
        if not finetune_settings.bi_modal
        else finetune_settings.pre_trained_embedding_size * 2
    )
    finetuner.login()

    experiment_name = 'finetune_' + data_name
    experiment_exists = False
    for exp in finetuner.list_experiments():
        if experiment_name == exp.name:
            experiment_exists = True
    if not experiment_exists:
        finetuner.create_experiment(experiment_name)

    callbacks = [
        EvaluationCallback(
            finetune_ds.val_query,
            finetune_ds.val_index,
            limit=finetune_settings.eval_match_limit,
            metrics=['ndcg'],
        ),
        BestModelCheckpoint(monitor='ndcg', save_dir=save_dir, verbose=True),
        EarlyStopping(
            monitor='ndcg',
            verbose=False,
            patience=finetune_settings.early_stopping_patience,
        ),
    ]

    run = finetuner.fit(
        model='mlp',
        model_options={
            'input_size': input_size,
            'hidden_sizes': finetune_settings.hidden_sizes,
            'l2': True,
            'bias': False if finetune_settings.bi_modal else True,
        },
        experiment_name=experiment_name,
        train_data=finetune_ds.train,
        eval_data=finetune_ds.val,
        callbacks=callbacks,
    )

    run_failed = False
    with yaspin(
        sigmap=sigmap, text='Waiting for finetune job to be assigned', color='green'
    ) as spinner:
        while run.status()['status'] == 'CREATED' and not run_failed:
            run_failed = run.status()['status'] == 'FAILED'
            sleep(1)
        if run_failed:
            spinner.fail('👎')
        else:
            spinner.ok('👍')
            print(run.logs())
            sys.exit(0)

    with yaspin(sigmap=sigmap, text='Running finetune job', color='green') as spinner:
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
        print(run.logs())
        sys.exit(0)

    print('🧠 Perfect! Early stopping triggered since accuracy is great already')

    save_path = os.path.join(save_dir, 'best_model_ndcg')
    run.save_model(save_path)

    return save_path


@contextmanager
def _finetune_dir() -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        full_save_path = osp(tmpdir, _BASE_SAVE_DIR)
        os.makedirs(full_save_path, exist_ok=True)
        yield full_save_path


def _maybe_add_embeddings(
    encoder_uses: str,
    encoder_uses_with: Dict,
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

    embed_now(encoder_uses, encoder_uses_with, dataset, kubectl_path=kubectl_path)

    assert all([d.embedding is not None for d in dataset]), (
        "Some docs slipped through and" " still have no embedding..."
    )


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
    return np.concatenate(order)


def _prepare_dataset_bi_modal(dataset: DocumentArray):
    for doc in dataset:
        doc.embedding = get_bi_modal_embedding(doc)
