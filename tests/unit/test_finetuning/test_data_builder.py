import json

import pytest
from docarray import Document, DocumentArray

from now.finetuning.data_builder import DataBuilder
from now.finetuning.generation_fns import ImageNormalizer, TextProcessor
from now.now_dataclasses import Task


def test_data_generation(get_config_path):
    config_path = get_config_path
    with open(config_path) as f:
        dct = json.load(f)
        task = Task(**dct)

    print(task)
    dataset = DocumentArray.pull(task.extracted_dataset)
    data = DataBuilder(dataset=dataset, config=task).build()
    print(data)


def test_image_normalizer():
    uri = 'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg'
    document = {
        'other_attr': 10,
        'uris': [uri],
        'better_uris': [uri, uri],
    }
    scope = ['uris', 'better_uris']
    img_proc = ImageNormalizer(scope=scope)
    processed_imgs = img_proc.process(document=document)
    assert len(processed_imgs) == 3
    assert type(processed_imgs[0]) == Document


@pytest.mark.parametrize(
    'scope, permute, powerset, example, length',
    [
        (['attr1', 'attr2'], True, True, 'hello', 9),
        (['attr1', 'attr2'], True, False, 'hey hi hello', 6),
        (['attr1', 'attr2'], False, True, 'hi hey', 3),
        (['attr1'], True, True, 'hello', 1),
        (['attr1'], False, False, 'hello', 1),
        (['attr2'], True, True, 'hey hi', 2),
        (['attr2'], False, False, 'hi hey', 1),
    ],
)
def test_text_processor(scope, permute, powerset, example, length):
    document = {'attr1': ['hello'], 'attr2': ['hi', 'hey'], 'irrelevant_attr': ['bye']}
    text_proc = TextProcessor(scope=scope, powerset=powerset, permute=permute)
    processed_docs = text_proc.process(document)
    assert len(processed_docs) == length
    assert example in [doc.text for doc in processed_docs]
