import pytest
from docarray import Document

from now.finetuning.generation_fns import ImageNormalizer, TextProcessor


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
