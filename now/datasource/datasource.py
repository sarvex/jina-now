from docarray import DocumentArray


class Datasource:
    def get_data(self, mapping, *args, **kwargs) -> DocumentArray:
        raise NotImplementedError()
