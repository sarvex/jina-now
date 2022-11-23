from collections import namedtuple
from typing import List

SemanticScore = namedtuple(
    'SemanticScores',
    [
        'query_field',
        'query_encoder',
        'document_field',
        'document_encoder',
        'linear_weight',
    ],
)


class Scores:
    def __init__(self, semantic_scores: List[SemanticScore]) -> None:
        self.semantic_scores = semantic_scores

    def get_scores(self, query_encoder):
        for (
            query_field,
            _query_encoder,
            document_field,
            document_encoder,
            linear_weight,
        ) in self.semantic_scores:
            if query_encoder == _query_encoder:
                yield query_field, document_field, document_encoder, linear_weight
