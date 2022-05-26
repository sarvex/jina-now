from docarray import DocumentArray

from now.constants import BASE_STORAGE_URL, Modalities
from now.datasource.datasource import Datasource


class ExampleDatasource(Datasource):
    def __init__(self, ds_id, display_name, modality):
        self.ds_id = ds_id
        self.display_name = display_name
        self.modality = modality

    def get_data(self, quality: str) -> DocumentArray:
        url = f'{BASE_STORAGE_URL}/{self.modality}/{self.ds_id}{("." + quality) if quality is not None else ""}.bin'
        # TODO  return document array from url
        raise NotImplementedError()


example_datasources = [
    ExampleDatasource('bird-species', '🦆 birds (≈12K docs)', Modalities.TEXT_TO_IMAGE),
    ...,
]
