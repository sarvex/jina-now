from typing import Optional

from better_profanity import profanity
from docarray import DocumentArray
from fast_autocomplete import AutoComplete

from now.executor.abstract.auth.auth import NOWAuthExecutor as Executor
from now.executor.abstract.auth.auth import SecurityLevel, secure_request


class NOWAutoCompleteExecutor(Executor):
    def __init__(self, search_traversal_paths: str = '@r', words=None, *args, **kwargs):
        self.words = words if words else {}
        self.search_traversal_paths = search_traversal_paths
        self.autocomplete = None

        super().__init__(*args, **kwargs)

    @staticmethod
    def contains_profanity(text: str):
        """Helper function that wraps checking for profanities.
        :param text: Text to be checked for profanities
        :returns: Boolean values signifying whether or not profanity
            was present in the text
        """
        return profanity.contains_profanity(text)

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search_update(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        traversal_paths = parameters.get('traversal_paths', self.search_traversal_paths)
        flat_docs = docs[traversal_paths]
        if len(flat_docs) == 0:
            return

        for doc in flat_docs:
            if doc.text:
                search_words = doc.text.split(' ')
                # in case query is composed of two words
                for word in search_words:
                    if not NOWAutoCompleteExecutor.contains_profanity(word):
                        if word in self.words:
                            self.words[word]['count'] += 1
                        else:
                            self.words[word] = {'count': 1}

        self.auto_complete = AutoComplete(words=self.words)

    @secure_request(on='/suggestion', level=SecurityLevel.USER)
    def get_suggestion(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        if len(docs) and docs[0].text:
            docs[0].tags['suggestions'] = self.auto_complete.search(
                docs[0].text, max_cost=3, size=5
            )
            return docs
        return
