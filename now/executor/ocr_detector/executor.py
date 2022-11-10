import gc
from collections import defaultdict
from io import BytesIO
from typing import Optional

import keras_ocr
import pytesseract
import tensorflow as tf
from jina import DocumentArray, Executor, requests
from PIL import Image

from now.constants import TAG_OCR_DETECTOR_TEXT_IN_DOC

keras_ocr.config.configure()


class NOWOCRDetector9(Executor):
    """Uses keras-ocr to detect text in images"""

    def __init__(self, traversal_paths: Optional[str] = '@r', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.traversal_paths = traversal_paths
        self.keras_ocr_pipeline = keras_ocr.pipeline.Pipeline()
        # we need this to occasionally reset the keras-ocr pipeline to avoid memory leaks due to tensorflow
        self.cnt_images_inference = 0

    @requests(on=['/detect_text', '/index'])
    def detect_text(self, docs: DocumentArray, parameters: dict = {}, **kwargs):
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        flat_docs = docs[traversal_paths]

        if len(flat_docs) == 0:
            return

        id_to_text = defaultdict(str)
        # select documents whose mime_type starts with 'image'
        flat_docs = [doc for doc in flat_docs if doc.mime_type.startswith('image')]
        # use pytesseract to detect text in images
        for doc in flat_docs:
            text_in_doc = (
                pytesseract.image_to_string(Image.open(BytesIO(doc.blob)))
                .lower()
                .strip()
            )
            if traversal_paths == '@c':
                id_to_text[doc.parent_id] += text_in_doc + ' '
            else:
                id_to_text[doc.id] += text_in_doc + ' '
        # use keras-ocr to detect text in images
        images = [keras_ocr.tools.read(BytesIO(doc.blob)) for doc in flat_docs]
        self.cnt_images_inference += len(images)
        if images:
            predictions_keras = self.keras_ocr_pipeline.recognize(images)
            for doc, prediction in zip(flat_docs, predictions_keras):
                text_in_doc = ' '.join([word[0] for word in prediction])
                if traversal_paths == '@c':
                    id_to_text[doc.parent_id] += text_in_doc + ' '
                else:
                    id_to_text[doc.id] += text_in_doc + ' '
        # update the tags of the documents to include the detected text
        for doc in flat_docs:
            text_in_doc = id_to_text[
                doc.parent_id if traversal_paths == '@c' else doc.id
            ]
            doc.tags[TAG_OCR_DETECTOR_TEXT_IN_DOC] = text_in_doc.strip()
        # reset the keras-ocr pipeline every 100 images to avoid memory leaks
        if self.cnt_images_inference > 100:
            self.keras_ocr_pipeline = None
            # call garbage collector to free memory
            gc.collect()
            # if using gpu, call tensorflow to free memory
            tf.keras.backend.clear_session()
            self.keras_ocr_pipeline = keras_ocr.pipeline.Pipeline()
            self.cnt_images_inference = 0
