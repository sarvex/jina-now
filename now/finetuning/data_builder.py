from typing import Any, Dict, List

from docarray import Document, DocumentArray

class ESDataTransformer:
    @classmethod
    def transform(cls, data: DocumentArray) -> List[Dict[str, Any]]:
        """
        Transform data extracted from Elasticsearch to a more convenient form.

        :param data: Loaded `DocumentArray` containing ES data.
        :return: List of data examples as dictionaries.
        """
        transformed_data = []
        for document in data:
            attributes = {}
            cls._transform_doc(document, attributes, [])
            transformed_data.append(attributes)
        return transformed_data

    @classmethod
    def _transform_doc(
        cls, document: Document, attributes: Dict[str, Any], names: List[str]
    ):
        """
        Extract attributes from a `Document` and store it as a dictionary.

        Recursively iterates across different chunks of the `Document` and collects
        attributes with their corresponding values.

        :param document: `Document` we want to transform.
        :param attributes: Dictionary of attributes extracted from the document.
        :param names: Name of an attribute (attribute names may be nested, e.g.
            info.cars, and we need to store name(s) on every level of recursion).
        """
        if not document.chunks:
            names.append(document.tags['field_name'])
            attr_name = '.'.join(names)
            attr_val = (
                document.text if document.tags['modality'] == 'text' else document.uri
            )
            if attr_name not in attributes:
                attributes[attr_name] = []
            attributes[attr_name].append(attr_val)
        else:
            if 'field_name' in document.tags:
                names.append(document.tags['field_name'])
            for doc in document.chunks:
                cls._transform_doc(doc, attributes, names[:])