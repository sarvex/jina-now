import json

import pytest
from docarray import Document, DocumentArray

from now.app.text_to_text_and_image.app import TextToTextAndImage
from now.constants import DatasetTypes
from now.data_loading.data_loading import load_data
from now.demo_data import DemoDatasetNames
from now.finetuning.data_builder import DataBuilder
from now.finetuning.generation_fns import ImageNormalizer, TextProcessor

# this test was failing because of the following error: TypeError: __init__() got an unexpected keyword argument 'data'
#
from now.now_dataclasses import UserInput


def test_data_generation():
    # load dataset
    user_input = UserInput()
    user_input.dataset_type = DatasetTypes.DEMO
    user_input.dataset_name = DemoDatasetNames.ES_ONLINE_SHOP_50
    dataset = load_data(TextToTextAndImage(), user_input)

    initial_length = len(dataset)
    task_config = TextToTextAndImage._create_task_config(user_input, dataset[0])
    data = DataBuilder(dataset=dataset, config=task_config).build()
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
    assert len(vision_datset) == 3 * initial_length
    assert vision_datset[0].chunks[0].text
    assert vision_datset[0].chunks[1].tensor.any()


def test_image_normalizer():
    uri = 'https://product-finder.wordlift.io/wp-content/uploads/2021/06/93217825.jpeg'
    document = Document(
        chunks=[
            Document(tags={'field_name': 'other_attr'}, content=[10]),
            Document(tags={'field_name': 'uris'}, uri=uri),
            Document(tags={'field_name': 'better_uri'}, uri=uri),
        ]
    )
    scope = ['uris', 'better_uri']
    img_proc = ImageNormalizer(scope=scope)
    processed_imgs = img_proc.process(document=document)
    assert len(processed_imgs) == 2
    assert type(processed_imgs[0]) == Document


@pytest.mark.parametrize(
    'scope, permute, powerset, example, length',
    [
        (['attr1', 'attr2'], True, True, 'hello', 4),
        (['attr1', 'attr2'], True, False, 'hi hello', 2),
        (['attr1', 'attr2'], False, True, 'hello hi', 3),
        (['attr1'], True, True, 'hello', 1),
        (['attr2'], True, True, 'hi', 1),
    ],
)
def test_text_processor(scope, permute, powerset, example, length):
    document = Document(
        chunks=[
            Document(tags={'field_name': 'attr1'}, content='hello'),
            Document(tags={'field_name': 'attr2'}, content='hi'),
            Document(tags={'field_name': 'irrelevant_attr'}, content='bye'),
        ]
    )
    text_proc = TextProcessor(scope=scope, powerset=powerset, permute=permute)
    processed_docs = text_proc.process(document)
    assert len(processed_docs) == length
    assert example in [doc.text for doc in processed_docs]
