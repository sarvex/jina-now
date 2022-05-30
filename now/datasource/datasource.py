import abc

from docarray import DocumentArray


# TODO just a prototype - needs to be implemented in the future
class Datasource:
    @abc.abstractmethod
    def get_data(self, mapping, *args, **kwargs) -> DocumentArray:
        raise NotImplementedError()
