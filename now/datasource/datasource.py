from docarray import DocumentArray


class Datasource:
    def get_data(self, *args, **kwargs) -> DocumentArray:
        raise NotImplementedError()
