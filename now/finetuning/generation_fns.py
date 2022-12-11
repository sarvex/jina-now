from abc import ABC, abstractmethod
from itertools import chain, combinations, permutations
from typing import List

from docarray import Document, DocumentArray


class GeneratorFunction(ABC):
    @abstractmethod
    def process(self, document: Document) -> DocumentArray:
        """Generation function that produces a `DocumentArray` given a `Document`."""
        pass

    @staticmethod
    @abstractmethod
    def name() -> str:
        """Name for the task config."""
        pass


class ImageNormalizer(GeneratorFunction):
    def __init__(self, scope: List[str]):
        """
        Class for image processing and normalization.

        :param scope: List of fields to consider for processing.
        """
        self._scope = scope

    def process(self, document: Document) -> DocumentArray:
        """
        Processes a `Document` containing image information.

        :param document: Document containing image data (uris).
        :return: `DocumentArray` of processed images.
        """
        normalized_imgs = DocumentArray()
        for chunk in document.chunks:
            if chunk.tags['field_name'] in self._scope:
                normalized_imgs.append(Document(uri=chunk.uri))
        normalized_imgs.apply(self._process)
        return normalized_imgs

    @staticmethod
    def _process(doc: Document) -> Document:
        """Loads uri as image and applies normalization."""
        doc.load_uri_to_image_tensor().set_image_tensor_normalization()
        return doc

    @staticmethod
    def name() -> str:
        return 'image_normalization'


class TextProcessor(GeneratorFunction):
    def __init__(self, scope: List[str], powerset: bool = False, permute: bool = False):
        """
        Class for text processing with subsets and permutations.

        :param scope: List of fields to consider for processing.
        :param powerset: Creates subsets of text fields if set to `True`.
        :param permute: Creates permutations of fields if set to `True`.
        """
        self._scope = scope
        self._powerset = powerset
        self._permute = permute

    def process(self, document: Document) -> DocumentArray:
        """
        Processes a `Document` containing text information.

        :param document: Document containing text data.
        :param scope: List of fields to consider for processing.
        :return: `DocumentArray` of processed (and concatenated) text data.
        """
        document = {
            chunk.tags['field_name']: chunk.content
            for chunk in document.chunks
            if chunk.tags['field_name'] in self._scope
        }
        if self._powerset:
            key_sets = chain.from_iterable(
                combinations(self._scope, r) for r in range(1, len(self._scope) + 1)
            )
        else:
            key_sets = [self._scope]

        attribute_sets = [[document[key] for key in key_set] for key_set in key_sets]

        if self._permute:
            return DocumentArray(
                [
                    Document(content=' '.join(attr_seq), modality='text')
                    for attributes in attribute_sets
                    for attr_seq in permutations(attributes)
                ]
            )
        else:
            return DocumentArray(
                [
                    Document(content=' '.join(attributes), modality='text')
                    for attributes in attribute_sets
                ]
            )

    @staticmethod
    def name() -> str:
        return 'text_processing'
