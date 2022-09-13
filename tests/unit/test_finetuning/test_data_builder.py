import json

import pytest
from docarray import Document

from now.apps.text_to_text_and_image.app import TextToTextAndImage
from now.constants import DemoDatasets
from now.data_loading.data_loading import load_data
from now.finetuning.data_builder import DataBuilder
from now.finetuning.generation_fns import ImageNormalizer, TextProcessor
from now.now_dataclasses import Task, UserInput


def test_data_generation(get_nest_config_path):
    # read task config
    config_path = get_task_config_path
    with open(config_path) as f:
        dct = json.load(f)
        task = Task(**dct)

    # load dataset
    user_input = UserInput()
    user_input.data = DemoDatasets.ES_ONLINE_SHOP_50
    user_input.quality = None
    dataset = load_data(TextToTextAndImage(), user_input)

    initial_length = len(dataset)
    number_of_uris = 66  # pre-computed
    data = DataBuilder(dataset=dataset, config=task).build()
    assert len(data) == 2
    text_dataset, encoder_type = data[0]
    assert encoder_type == 'text-to-text'
    # for queries, we do powerset and permutations for 2 fields - >
    # 4 combinations. plus number of target values. In the end we,
    # should get (5 * initial length) examples.
    assert len(text_dataset) == 5 * initial_length
    assert text_dataset[0].text
    vision_datset, encoder_type = data[1]
    assert encoder_type == 'text-to-image'
    # for queries, we generate powerset of 2 fields -> 3 combinations
    # and we do product with number of uris for image data.
    # in the end we should get (3 * number of uris) examples.
    assert len(vision_datset) == 3 * number_of_uris
    assert vision_datset[0].chunks[0].text
    assert vision_datset[0].chunks[1].tensor.any()


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
