import os

from now.apps.base.app import JinaNOWApp
from now.constants import Apps, Modalities
from typing import Dict, List, Optional

from now.apps.base.app import JinaNOWApp
from now.constants import Apps, Modalities
from now.now_dataclasses import UserInput
from now_common.utils import preprocess_nested_docs, preprocess_text
from docarray import DocumentArray

class TextToTextAndImage(JinaNOWApp):
    """
    Hybrid text to text+image search combining symbolic and neural IR approaches.
    """

    def __init__(self):
        super().__init__()

    @property
    def app_name(self) -> Apps:
        return Apps.TEXT_TO_TEXT_AND_IMAGE

    @property
    def is_enabled(self) -> bool:
        return False

    @property
    def description(self) -> str:
        return (
            'Text to text+image search app combining symbolic and neural IR approaches.'
        )

    def set_flow_yaml(self, **kwargs):
        """configure the flow yaml in the Jina NOW app."""
        flow_dir = os.path.abspath(os.path.join(__file__, '..'))
        self.flow_yaml = os.path.join(flow_dir, 'flow.yml')

    @property
    def input_modality(self) -> Modalities:
        return Modalities.TEXT

    @property
    def output_modality(self) -> Modalities:
        return Modalities.TEXT_AND_IMAGE

    @property
    def preprocess(self, da: DocumentArray, user_input: UserInput, is_indexing: False) -> DocumentArray:
        # Indexing
        if is_indexing:
            return preprocess_nested_docs(da=da)
        # Query
        else:
            return preprocess_text(da=da, split_by_sentences=False)
