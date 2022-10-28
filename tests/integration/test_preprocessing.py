import os

from docarray import Document
from jina import Flow

from now.app.text_to_image.app import TextToImage
from now.data_loading.utils import transform_uni_modal_data
from tests.executor.indexer.base.in_memory_indexer import InMemoryIndexer


def test_indexing(preprocess_and_encode):
    data, user_input = preprocess_and_encode

    app_instance = TextToImage()
    f2 = Flow().add(
        uses=InMemoryIndexer,
        uses_with={
            'columns': [
                'split',
                'str',
                'finetuner_label',
                'str',
                'content_type',
                'str',
            ],
            'dim': 512,
        },
    )
    with f2:
        f2.post(
            on='/index',
            inputs=data,
            parameters={
                'user_input': user_input.__dict__,
                'access_paths': app_instance.index_query_access_paths(
                    user_input.search_fields
                ),
                'traversal_paths': app_instance.index_query_access_paths(
                    user_input.search_fields
                ),
            },
        )

        query_res = f2.post(
            on='/search',
            inputs=data,
            parameters={
                'user_input': user_input.__dict__,
                'access_paths': app_instance.index_query_access_paths(
                    user_input.search_fields
                ),
                'traversal_paths': app_instance.index_query_access_paths(
                    user_input.search_fields
                ),
            },
            return_results=True,
        )
    query_res.summary()


def test_uni_to_multi_modal(resources_folder_path, single_modal_data):
    data = single_modal_data
    data.append(
        Document(
            uri=os.path.join(resources_folder_path, 'image', '5109112832.jpg'),
            tags={'color': 'red'},
        )
    )
    transformed_data = transform_uni_modal_data(documents=data, filter_fields=['color'])

    assert len(transformed_data) == len(data)
    assert 'color' in transformed_data[0].tags['filter_fields']
    assert len(transformed_data[1].chunks) == 1
