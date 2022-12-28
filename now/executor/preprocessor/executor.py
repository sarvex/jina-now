import io
import json
import os
import random
import string
import tempfile
import urllib
from typing import Dict, Optional

import boto3
from jina import Document, DocumentArray
from paddleocr import PaddleOCR

from now.app.base.app import JinaNOWApp
from now.app.base.transform_docarray import transform_docarray
from now.constants import (
    ACCESS_PATHS,
    TAG_OCR_DETECTOR_TEXT_IN_DOC,
    DatasetTypes,
    Modalities,
)
from now.executor.abstract.auth import (
    SecurityLevel,
    get_auth_executor_class,
    secure_request,
)
from now.now_dataclasses import UserInput
from now.utils import maybe_download_from_s3

Executor = get_auth_executor_class()


class NOWPreprocessor(Executor):
    """Applies preprocessing to documents for encoding, indexing and searching as defined by app.
    If necessary, downloads files for that from cloud bucket.

    Also, provides an endpoint to download data from S3 bucket without requiring credentials for that.

    To update user_input, set the 'user_input' key in parameters dictionary.
    """

    def __init__(self, max_workers: int = 15, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.app: JinaNOWApp = JinaNOWApp()
        self.max_workers = max_workers
        self.paddle_ocr = PaddleOCR(use_angle_cls=True, lang='en', show_log=False)

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
        """Sets user_input attribute and deletes used attributes from dictionary"""
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

    def _ocr_detect_text(self, docs: DocumentArray):
        """Iterates over all documents, detects text in images and saves it into the tags of the document."""
        for doc in docs[ACCESS_PATHS]:
            if doc.modality == Modalities.IMAGE:
                ocr_result = self.paddle_ocr.ocr(doc.blob)
                text_list = [text for _, (text, _) in ocr_result[0]]
                doc.tags[TAG_OCR_DETECTOR_TEXT_IN_DOC] = ' '.join(text_list)

    @staticmethod
    def _save_uri_to_tmp_file(uri, tmpdir) -> str:
        """Saves URI to a temporary file and returns the path to that file."""
        req = urllib.request.Request(uri, headers={'User-Agent': 'Mozilla/5.0'})
        tmp_fn = os.path.join(
            tmpdir,
            ''.join([random.choice(string.ascii_lowercase) for i in range(10)])
            + '.png',
        )
        with urllib.request.urlopen(req, timeout=10) as fp:
            buffer = fp.read()
            binary_fn = io.BytesIO(buffer)
            with open(tmp_fn, 'wb') as f:
                f.write(binary_fn.read())
        return tmp_fn

    def _preprocess_maybe_cloud_download(self, docs: DocumentArray) -> DocumentArray:
        with tempfile.TemporaryDirectory() as tmpdir:
            docs = transform_docarray(
                documents=docs,
                search_fields=self.user_input.search_fields
                if self.user_input and self.user_input.search_fields
                else [],
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
            self._ocr_detect_text(docs)

            # as _maybe_download_from_s3 moves S3 URI to tags['uri'], need to move it back for post-processor & accurate
            # results.
            if (
                self.user_input
                and self.user_input.dataset_type == DatasetTypes.S3_BUCKET
            ):

                def move_uri(d: Document) -> Document:
                    cloud_uri = d.tags.get('uri')
                    if isinstance(cloud_uri, str) and cloud_uri.startswith('s3://'):
                        d.uri = cloud_uri
                        d.chunks[:, 'uri'] = cloud_uri
                    return d

                for d in docs:
                    for c in d.chunks:
                        # TODO please fix this hack - uri should not be in tags
                        move_uri(c)

        return docs

    @secure_request(on=None, level=SecurityLevel.USER)
    def preprocess(
        self, docs: DocumentArray, parameters: Optional[Dict] = None, *args, **kwargs
    ) -> DocumentArray:
        """If necessary downloads data from cloud bucket. Applies preprocessing to documents as defined by apps.

        :param docs: loaded data but not preprocessed
        :param parameters: user input, used to construct UserInput object
        :return: preprocessed documents which are ready to be encoded and indexed
        """
        # TODO remove set user input. Should be only set once in constructor use api key instead of user token
        self._set_user_input(parameters=parameters)
        return self._preprocess_maybe_cloud_download(docs=docs)

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
