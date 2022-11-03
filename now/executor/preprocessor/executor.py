import json
import os
import tempfile
from typing import Dict, Optional

import boto3
from jina import Document, DocumentArray

from now.app.base.app import JinaNOWApp
from now.common.options import construct_app
from now.constants import Apps, DatasetTypes
from now.executor.abstract.auth import NOWAuthExecutor as Executor
from now.executor.abstract.auth import SecurityLevel, secure_request
from now.now_dataclasses import UserInput
from now.utils import _maybe_download_from_s3


class NOWPreprocessor(Executor):
    """Applies preprocessing to documents for encoding, indexing and searching as defined by app. If necessary,
    downloads files for that from cloud bucket.

    Also, provides an endpoint to download data from S3 bucket without requiring credentials for that.

    To update user_input, set the 'user_input' key in parameters dictionary.
    """

    def __init__(self, app: str, max_workers: int = 15, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.app: JinaNOWApp = construct_app(app)
        self.max_workers = max_workers

        self.user_input_path = (
            os.path.join(self.workspace, 'user_input.json') if self.workspace else None
        )
        if self.user_input_path and os.path.exists(self.user_input_path):
            with open(self.user_input_path, 'r') as fp:
                user_input = json.load(fp)
            self._set_user_input({'user_input': {**user_input}})
        else:
            self.user_input = None

    def _set_user_input(self, parameters: Dict):
        """Sets user_input attribute and deletes used attributes from dictionary."""
        if 'user_input' in parameters.keys():
            self.user_input = UserInput()
            for attr_name, prev_value in self.user_input.__dict__.items():
                setattr(
                    self.user_input,
                    attr_name,
                    parameters['user_input'].pop(attr_name, prev_value),
                )
            if self.user_input_path:
                with open(self.user_input_path, 'w') as fp:
                    json.dump(self.user_input.__dict__, fp)

    def _preprocess_maybe_cloud_download(
        self,
        docs: DocumentArray,
        is_indexing,
        encode: bool = False,
    ) -> DocumentArray:
        with tempfile.TemporaryDirectory() as tmpdir:
            if (
                self.user_input
                and self.user_input.dataset_type == DatasetTypes.S3_BUCKET
            ):
                _maybe_download_from_s3(
                    docs=docs,
                    tmpdir=tmpdir,
                    user_input=self.user_input,
                    max_workers=self.max_workers,
                )

            pre_docs = self.app.preprocess(
                docs, self.user_input, is_indexing=is_indexing
            )
            if encode:
                remaining_docs = self.app.preprocess(
                    docs, self.user_input, is_indexing=not is_indexing
                )
                pre_docs.extend(remaining_docs)
            docs = pre_docs

            # as _maybe_download_from_s3 moves S3 URI to tags['uri'], need to move it back for post-processor & accurate
            # results
            if (
                self.user_input
                and self.user_input.dataset_type == DatasetTypes.S3_BUCKET
            ):

                def move_uri(d: Document) -> Document:
                    cloud_uri = d.tags.get('uri')
                    if isinstance(cloud_uri, str) and cloud_uri.startswith('s3://'):
                        d.uri = cloud_uri
                        if self.app.app_name == Apps.TEXT_TO_VIDEO:
                            d.chunks[:, 'uri'] = cloud_uri
                    return d

                for d in docs:
                    move_uri(d)

        return docs

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index(
        self, docs: DocumentArray, parameters: Optional[Dict] = None, *args, **kwargs
    ) -> DocumentArray:
        """If necessary downloads data from cloud bucket. Applies preprocessing to documents as defined by apps.

        :param docs: loaded data but not preprocessed
        :param parameters: user input, used to construct UserInput object
        :return: preprocessed documents which are ready to be encoded and indexed
        """
        self._set_user_input(parameters=parameters)
        return self._preprocess_maybe_cloud_download(docs=docs, is_indexing=True)

    @secure_request(on='/encode', level=SecurityLevel.USER)
    def encode(
        self, docs: DocumentArray, parameters: Optional[Dict] = None, *args, **kwargs
    ):
        """Encodes the documents and returns the embeddings. It merges index and search endpoint results as the
        documents for encoding can be multimodal.

        :param docs: loaded data but not preprocessed
        :param parameters: user input, used to construct UserInput object
        :return: preprocessed documents which are ready to be encoded and indexed
        """
        self._set_user_input(parameters=parameters)
        return self._preprocess_maybe_cloud_download(
            docs=docs, is_indexing=True, encode=True
        )

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search(
        self, docs: DocumentArray, parameters: Optional[Dict] = None, *args, **kwargs
    ) -> DocumentArray:
        """If necessary downloads data from cloud bucket. Applies preprocessing to documents as defined by apps.

        :param docs: loaded data but not preprocessed
        :param parameters: user input, used to construct UserInput object
        :return: preprocessed documents which are ready to be used for search
        """
        self._set_user_input(parameters=parameters)
        return self._preprocess_maybe_cloud_download(docs=docs, is_indexing=False)

    @secure_request(on='/temp_link_cloud_bucket', level=SecurityLevel.USER)
    def temporary_link_from_cloud_bucket(
        self, docs: DocumentArray, parameters: Optional[Dict] = None, *args, **kwargs
    ) -> DocumentArray:
        """Downloads files from cloud bucket and loads them as temporary link into the URI.

        :param docs: documents which contain URIs to the documents which shall be downloaded
        :param parameters: user input, used to construct UserInput object
        :return: files as temporary available link in URI attribute
        """
        if docs is None or len(docs) == 0:
            return
        self._set_user_input(parameters=parameters)

        if self.user_input and self.user_input.dataset_type == DatasetTypes.S3_BUCKET:

            def convert_fn(d: Document) -> Document:
                if isinstance(d.uri, str) and d.uri.startswith('s3://'):
                    session = boto3.session.Session(
                        aws_access_key_id=self.user_input.aws_access_key_id,
                        aws_secret_access_key=self.user_input.aws_secret_access_key,
                        region_name=self.user_input.aws_region_name,
                    )
                    s3_client = session.client('s3')
                    bucket_name = d.uri.split('/')[2]
                    path_s3 = '/'.join(d.uri.split('/')[3:])
                    temp_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': bucket_name, 'Key': path_s3},
                        ExpiresIn=300,
                    )
                    d.uri = temp_url
                return d

        for d in docs:
            convert_fn(d)

        return docs


if __name__ == '__main__':

    from jina import Flow

    app = Apps.TEXT_TO_VIDEO

    user_inpuT = UserInput()
    user_inpuT.app_instance = construct_app(app)
    user_inpuT.dataset_type = DatasetTypes.S3_BUCKET
    user_inpuT.dataset_path = 's3://bucket/folder'

    text_docs = DocumentArray(
        [
            Document(chunks=DocumentArray([Document(text='hi')])),
        ]
    )
    executor = NOWPreprocessor(app=app)
    result = executor.search(
        docs=text_docs, parameters={'app': user_inpuT.app_instance.app_name}
    )
    f = Flow().add(uses=NOWPreprocessor, uses_with={'app': app})
    with f:
        result = f.post(
            on='/search',
            inputs=text_docs,
            show_progress=True,
        )

        result = DocumentArray.from_json(result.to_json())
