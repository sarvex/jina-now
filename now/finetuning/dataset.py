""" Module contains data-transfer-object for finetune datasets. """
from dataclasses import dataclass
from typing import Dict, Optional

from docarray import Document, DocumentArray

from now.finetuning.settings import FinetuneSettings

_SEED = 42
MAX_VAL_SET_SIZE = 5000


@dataclass
class FinetuneDataset:
    index: DocumentArray

    train: Optional[DocumentArray] = None
    val: Optional[DocumentArray] = None
    val_query: Optional[DocumentArray] = None
    val_index: Optional[DocumentArray] = None

    def as_dict(self) -> Dict[str, Optional[DocumentArray]]:
        return {
            'train': self.train,
            'val': self.val,
            'val_query': self.val_query,
            'val_index': self.val_index,
        }


def build_finetuning_dataset(
    dataset: DocumentArray, finetune_settings: FinetuneSettings
) -> FinetuneDataset:
    ds = FinetuneDataset(index=dataset)

    split_index = max(
        int(len(dataset) * finetune_settings.train_val_split_ration),
        len(dataset) - MAX_VAL_SET_SIZE,
    )

    def _create_finetune_subset(subset: DocumentArray) -> DocumentArray:
        if finetune_settings.add_embeddings:
            return DocumentArray(
                [
                    Document(
                        tensor=doc.embedding.astype('float32'),
                        tags={'finetuner_label': doc.tags['finetuner_label']},
                    )
                    for doc in subset
                ]
            )
        else:
            return subset

    ds.train = _create_finetune_subset(dataset[:split_index])
    ds.val = _create_finetune_subset(dataset[split_index:])

    ds.val_index = _create_finetune_subset(dataset[split_index:])
    ds.val_query = _create_finetune_subset(
        dataset[split_index:].sample(k=finetune_settings.num_val_queries, seed=_SEED)
    )

    return ds
