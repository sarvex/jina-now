import abc
from typing import List

from docarray import DocumentArray

from now.constants import Modalities
from now.data_loading.utils import _fetch_da_from_url, get_dataset_url
from now.utils import BetterEnum


class DatasourceType(BetterEnum):
    DEMO = 'demo'
    CUSTOM_DOCARRAY = 'custom-docarray'
    LOCAL_FILES = 'local-files'
    DATABASE_CONNECTOR = 'db-connector'


class Datasource:
    @property
    @abc.abstractmethod
    def type(self) -> DatasourceType:
        raise NotImplementedError('type')

    @property
    @abc.abstractmethod
    def modalities(self) -> List[Modalities]:
        raise NotImplementedError('modalities')

    @abc.abstractmethod
    def get_data(self, *args, **kwargs) -> DocumentArray:
        raise NotImplementedError('get_data')


class DemoDatasource(Datasource):
    def __init__(
        self,
        id_: str,
        display_name: str,
        modality_folder: Modalities,
    ):
        self.id = id_
        self.display_name = display_name
        self.modalities_folder = modality_folder

    @property
    def type(self) -> DatasourceType:
        return DatasourceType.DEMO

    @property
    def modalities(self) -> List[Modalities]:
        return [Modalities.TEXT, Modalities.IMAGE]

    def get_data(self) -> DocumentArray:
        url = get_dataset_url(self.id, self.modalities_folder)
        return _fetch_da_from_url(url)
