from typing import Dict, List, Union

from docarray import Document, DocumentArray
from docarray.score import NamedScore
from numpy import dot
from numpy.linalg import norm


class ESConverter:
    def __init__(self):
        pass

    def convert_doc_map_to_es(
        self,
        docs_map: Dict[str, DocumentArray],
        index_name: str,
        encoder_to_fields: dict,
    ) -> List[Dict]:
        """
        Transform a dictionary (mapping encoder to DocumentArray) into a list of Elasticsearch documents.
        The `docs_map` dictionary is expected to have the following structure:
        {
            'encoder1': DocumentArray([...]),
            'encoder2': DocumentArray([...]), # same number of documents as encoder1
            ...
        }

        :param docs_map: dictionary mapping encoder to DocumentArray.
        :param index_name: name of the index to be used in Elasticsearch.
        :param encoder_to_fields: dictionary mapping encoder to fields.
        :return: a list of Elasticsearch documents as dictionaries ready to be indexed.
        """
        es_docs = {}
        for executor_name, documents in docs_map.items():
            for doc in documents:
                if doc.id not in es_docs:
                    es_docs[doc.id] = self.get_base_es_doc(doc, index_name)
                    _doc = DocumentArray(Document(doc, copy=True))
                    # remove embeddings from serialized doc
                    _doc[..., 'embedding'] = None
                    _doc[..., 'tensor'] = None
                    _doc[..., 'blob'] = None
                    es_docs[doc.id]['serialized_doc'] = _doc[0].to_base64()
                es_doc = es_docs[doc.id]
                for encoded_field in encoder_to_fields[executor_name]:
                    field_doc = getattr(doc, encoded_field)
                    es_doc[
                        f'{encoded_field}-{executor_name}.embedding'
                    ] = field_doc.embedding
                    if hasattr(field_doc, 'text') and field_doc.text:
                        es_doc['bm25_text'] += field_doc.text + ' '
                        es_doc['text'] = field_doc.text
                    if hasattr(field_doc, 'uri') and field_doc.uri:
                        es_doc['uri'] = field_doc.uri
        return list(es_docs.values())

    def get_base_es_doc(self, doc: Document, index_name: str) -> Dict:
        es_doc = {k: v for k, v in doc.to_dict().items() if v}
        es_doc.pop('chunks', None)
        es_doc.pop('_metadata', None)
        es_doc['bm25_text'] = self.get_bm25_fields(doc)
        es_doc['_op_type'] = 'index'
        es_doc['_index'] = index_name
        es_doc['_id'] = doc.id
        doc.tags['embeddings'] = {}
        return es_doc

    def get_bm25_fields(self, doc: Document) -> str:
        try:
            return doc.bm25_text.text
        except AttributeError:
            return ''

    def convert_es_to_da(
        self, result: Union[Dict, List[Dict]], get_score_breakdown: bool
    ) -> DocumentArray:
        """
        Transform Elasticsearch documents into DocumentArray. Assumes that all Elasticsearch
        documents have a 'text' field. It returns embeddings as part of the tags for each field that is encoded.

        :param result: results from an Elasticsearch query.
        :param get_score_breakdown: whether to return the embeddings as tags for each document.
        :return: a DocumentArray containing all results.
        """
        if isinstance(result, Dict):
            result = [result]
        da = DocumentArray()
        for es_doc in result:
            doc = Document.from_base64(es_doc['_source']['serialized_doc'])
            for k, v in es_doc['_source'].items():
                if (
                    k.startswith('embedding') or k.endswith('embedding')
                ) and get_score_breakdown:
                    if 'embeddings' not in doc.tags:
                        doc.tags['embeddings'] = {}
                    doc.tags['embeddings'][k] = v
            da.append(doc)
        return da

    def convert_es_results_to_matches(
        self,
        query_doc: Document,
        es_results: List[Dict],
        get_score_breakdown: bool,
        metric: str,
        semantic_scores,
    ) -> DocumentArray:
        """
        Transform a list of results from Elasticsearch into a matches in the form of a `DocumentArray`.

        :param es_results: List of dictionaries containing results from Elasticsearch querying.
        :param get_score_breakdown: whether to calculate the score breakdown for matches.
        :return: `DocumentArray` that holds all matches in the form of `Document`s.
        """
        matches = DocumentArray()
        for result in es_results:
            d = self.convert_es_to_da(result, get_score_breakdown)[0]
            d.scores[metric] = NamedScore(value=result['_score'])
            if get_score_breakdown:
                d = self.calculate_score_breakdown(
                    query_doc, d, semantic_scores, metric
                )
            d.embedding = None
            d.tensor = None
            d.blob = b''
            matches.append(d)
        return matches

    def calculate_score_breakdown(
        self, query_doc: Document, retrieved_doc: Document, semantic_scores, metric
    ) -> Document:
        """
        Calculate the score breakdown for a given retrieved document. Each SemanticScore in the indexers
        `default_semantic_scores` should have a corresponding value, returned inside a list of scores in the documents
        tags under `score_breakdown`.

        :param query_doc: The query document. Contains embeddings for the semantic score calculation at tag level.
        :param retrieved_results: The Elasticsearch results, containing embeddings inside the `_source` field.
        :return: List of integers representing the score breakdown.
        """
        for (
            query_field,
            document_field,
            encoder,
            linear_weight,
        ) in semantic_scores:
            if encoder == 'bm25':
                continue
            q_emb = query_doc.tags['embeddings'][f'{query_field}-{encoder}']
            d_emb = retrieved_doc.tags['embeddings'][
                f'{document_field}-{encoder}.embedding'
            ]
            if metric == 'cosine':
                score = self.calculate_cosine(d_emb, q_emb) * linear_weight
            elif metric == 'l2_norm':
                score = self.calculate_l2_norm(d_emb, q_emb) * linear_weight
            else:
                raise ValueError(f'Invalid metric {metric}')
            retrieved_doc.scores[
                '-'.join(
                    [
                        query_field,
                        document_field,
                        encoder,
                        str(linear_weight),
                    ]
                )
            ] = NamedScore(value=round(score, 6))

        # calculate bm25 score
        vector_total = sum(
            [v.value for k, v in retrieved_doc.scores.items() if k != metric]
        )
        overall_score = retrieved_doc.scores[metric].value
        bm25_normalized = overall_score - vector_total
        bm25_raw = (bm25_normalized - 1) * 10

        retrieved_doc.scores['bm25_normalized'] = NamedScore(
            value=round(bm25_normalized, 6)
        )
        retrieved_doc.scores['bm25_raw'] = NamedScore(value=round(bm25_raw, 6))

        # remove embeddings from document
        retrieved_doc.tags.pop('embeddings', None)
        return retrieved_doc

    def calculate_l2_norm(self, d_emb, q_emb):
        return norm(q_emb - d_emb)

    def calculate_cosine(self, d_emb, q_emb):
        return dot(q_emb, d_emb) / (norm(q_emb) * norm(d_emb))
