import tempfile

from jina import Document, DocumentArray

from now.app.base.app import JinaNOWApp
from now.constants import DatasetTypes
from now.executor.abstract.auth import (
    SecurityLevel,
    get_auth_executor_class,
    secure_request,
)
from now.executor.preprocessor.s3_download import maybe_download_from_s3

Executor = get_auth_executor_class()


def move_uri(d: Document) -> Document:
    cloud_uri = d.tags.get('uri')
    if isinstance(cloud_uri, str) and cloud_uri.startswith('s3://'):
        d.uri = cloud_uri
        d.chunks[:, 'uri'] = cloud_uri
    return d


class NOWPreprocessor(Executor):
    """Applies preprocessing to documents for encoding, indexing and searching as defined by app.
    If necessary, downloads files for that from cloud bucket.
    """

    def __init__(self, max_workers: int = 15, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.app: JinaNOWApp = JinaNOWApp()
        self.max_workers = max_workers

    @secure_request(on=None, level=SecurityLevel.USER)
    def preprocess(self, docs: DocumentArray, *args, **kwargs) -> DocumentArray:
        """If necessary downloads data from cloud bucket. Applies preprocessing to document as defined by apps.

        :param docs: loaded data but not preprocessed
        :return: preprocessed documents which are ready to be encoded and indexed
        """
        if len(docs) > 0 and not docs[0].chunks:
            raise ValueError(
                'Documents are not in multi modal format. Please check documentation'
                'https://docarray.jina.ai/datatypes/multimodal/'
            )
        with tempfile.TemporaryDirectory() as tmpdir:
            index_fields = []
            if self.user_input:
                for index_field in self.user_input.index_fields:
                    index_fields.append(
                        self.user_input.field_names_to_dataclass_fields[index_field]
                        if index_field
                        in self.user_input.field_names_to_dataclass_fields
                        else index_field
                    )

            if (
                self.user_input
                and self.user_input.dataset_type == DatasetTypes.S3_BUCKET
            ):
                maybe_download_from_s3(
                    docs=docs,
                    tmpdir=tmpdir,
                    user_input=self.user_input,
                    max_workers=self.max_workers,
                )

            docs = self.app.preprocess(docs)

            # As _maybe_download_from_s3 moves S3 URI to tags['uri'], need to move it back for post-processor & accurate
            # results.
            if (
                self.user_input
                and self.user_input.dataset_type == DatasetTypes.S3_BUCKET
            ):

                for d in docs:
                    for c in d.chunks:
                        # TODO please fix this hack - uri should not be in tags
                        move_uri(c)
        return docs
