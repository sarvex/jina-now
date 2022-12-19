from typing import Dict

import numpy as np
from docarray import Document, DocumentArray
from docarray.array.chunk import ChunkArray


def merge_subdocuments(docs_map, encoder_to_fields) -> Dict[str, DocumentArray]:
    """
    Preprocess the docs_map to make it convertible to ES. Each field of a
    document should have only one subdocument. If the field document is a
    ChunkArray, only one embedding will be taken/created, and if the
    ChunkArray contains texts, these will be concatenated into one text.
    As for URIs, only the first one will be taken.

    :param docs_map: dictionary mapping encoder to DocumentArray.
    :param encoder_to_fields: dictionary mapping encoder to fields.
    :return: a dictionary mapping encoder to (processed) DocumentArray.
    """
    for executor_name, da in docs_map.items():
        field_names = encoder_to_fields[executor_name]
        for doc in da:
            for field_name in field_names:
                field_doc = getattr(doc, field_name)
                if isinstance(field_doc, ChunkArray):
                    # average the embeddings of the subdocuments
                    # TODO: use a better way to aggregate embeddings (eg. k-means)
                    embedding = average_embeddings_of_subdocuments(field_doc)
                    merged_doc = Document(
                        text=' '.join(
                            field_doc.texts
                        ).strip(),  # concatenate texts of subdocuments
                        embedding=embedding,
                    )
                    if field_doc[0].uri:
                        merged_doc.uri = field_doc[0].uri
                    merged_doc.tags['embeddings'] = {}
                    merged_doc.tags['embeddings'][
                        f'{field_name}-{executor_name}'
                    ] = field_doc[
                        ..., 'embedding'
                    ]  # will be a matrix, stacked
                    # replace ChunkArray with Document
                    setattr(doc, field_name, merged_doc)

    return docs_map


def average_embeddings_of_subdocuments(field_doc):
    return np.mean([chunk.embedding for chunk in field_doc], axis=0)
