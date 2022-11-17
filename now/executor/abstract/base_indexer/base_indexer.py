from collections import defaultdict
from copy import deepcopy
from sys import maxsize
from typing import List, Optional

from docarray import Document, DocumentArray

from now.constants import TAG_INDEXER_DOC_HAS_TEXT, TAG_OCR_DETECTOR_TEXT_IN_DOC
from now.executor.abstract.auth import (
    SecurityLevel,
    get_auth_executor_class,
    secure_request,
)
from now.executor.abstract.base_indexer.ranking import merge_matches_sum

CLOUD_BUCKET_PREFIXES = ['s3://']

Executor = get_auth_executor_class()


class NOWBaseIndexer(Executor):
    def __init__(
        self,
        dim: int,
        columns: Optional[List] = None,
        metric: str = 'cosine',
        limit: int = 10,
        traversal_paths: str = '@r',
        max_values_per_tag: int = 10,
        *args,
        **kwargs,
    ):
        """
        :param dim: Dimensionality of vectors to index
        :param columns: List of tuples of the form (column_name, str_type). Here str_type must be a string that can be
        parsed as a valid Python type.
        :param metric: Distance metric type. Can be 'euclidean', 'inner_product', or 'cosine'
        :param limit: Number of results to get for each query document in search
        :param traversal_paths: Default traversal paths on docs
        :param max_values_per_tag: Maximum number of values per tag
        (used for search), e.g. '@r', '@c', '@r,c'
        """

        super().__init__(*args, **kwargs)
        self.columns = self.parse_columns(columns)
        self.dim = dim
        self.metric = metric
        self.limit = limit
        self.traversal_paths = traversal_paths
        self.max_values_per_tag = max_values_per_tag
        self.construct()
        self.doc_id_tags = {}
        self.document_list = DocumentArray()
        self.load_document_list()

    @secure_request(on='/tags', level=SecurityLevel.USER)
    def get_tags_and_values(self, **kwargs):
        """Returns tags and their possible values

        for example if indexed docs are the following:
            docs = DocumentArray([
                Document(.., tags={'color':'red'}),
                Document(.., tags={'color':'blue'}),
                Document(.., tags={'greeting':'hello'}),
            ])

        the resulting response would be a document array with
        one document containing a dictionary in tags like the following:
        {'tags':{'color':['red', 'blue'], 'greeting':['hello']}}
        """

        count_dict = defaultdict(lambda: defaultdict(int))
        for tags in self.doc_id_tags.values():
            for key, value in tags.items():
                count_dict[key][value] += 1

        tag_to_values = dict()
        for key, value_to_count in count_dict.items():
            sorted_values = sorted(
                value_to_count.items(), reverse=True, key=lambda item: item[1]
            )
            tag_to_values[key] = [
                value
                for (value, _), _ in zip(sorted_values, range(self.max_values_per_tag))
            ]
        return DocumentArray([Document(text='tags', tags={'tags': tag_to_values})])

    @secure_request(on='/list', level=SecurityLevel.USER)
    def list(self, parameters: dict = {}, **kwargs):
        """List all indexed documents.
        :param parameters: dictionary with limit and offset
        - offset (int): number of documents to skip
        - limit (int): number of retrieved documents
        """
        limit = int(parameters.get('limit', maxsize))
        offset = int(parameters.get('offset', 0))
        # add removal of duplicates
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        if traversal_paths == '@c':
            docs = DocumentArray()
            chunks_size = int(parameters.get('chunks_size', 3))
            parent_ids = set()
            for d in self.document_list[offset * chunks_size :]:
                if len(parent_ids) == limit:
                    break
                if d.parent_id in parent_ids:
                    continue
                parent_ids.add(d.parent_id)
                docs.append(d)
        else:
            docs = self.document_list[offset : offset + limit]
        return docs

    @staticmethod
    def set_doc_has_text_tag(docs: DocumentArray):
        # update the tags of the documents to include the detected text
        for doc in docs:
            text_in_doc = doc.tags.pop(TAG_OCR_DETECTOR_TEXT_IN_DOC, '')
            text_in_doc = text_in_doc.split(' ')
            text_in_doc = filter(lambda s: len(s) > 1 and s.isalnum(), text_in_doc)
            doc.tags[TAG_INDEXER_DOC_HAS_TEXT] = len(list(text_in_doc)) > 0

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index_endpoint(
        self,
        docs: Optional[DocumentArray] = None,
        parameters: dict = {},
        docs_matrix: Optional[List[DocumentArray]] = None,
        **kwargs,
    ):
        """Base function for indexing documents. Handles the data management for the index and list endpoints.

        :param docs: the Documents to index
        :param parameters: dictionary with options for indexing
        """
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        flat_docs = docs[traversal_paths]
        if len(flat_docs) == 0:
            return

        self.set_doc_has_text_tag(flat_docs)

        flat_docs = self.maybe_drop_blob_tensor(flat_docs)
        self.index(flat_docs, parameters, **kwargs)
        self.extend_inmemory_docs_and_tags(flat_docs)
        return DocumentArray([])

    @secure_request(on='/delete', level=SecurityLevel.USER)
    def delete_endpoint(self, parameters: dict = {}, **kwargs):
        """
        Delete endpoint to delete document/documents from the index.
        Filter conditions can be passed to select documents for deletion.
        """
        filter = parameters.get("filter", {})
        if filter:
            filtered_docs = deepcopy(self.document_list.find(filter=filter))
            self.delete_inmemory_docs_and_tags(filtered_docs)
            self.delete(filtered_docs, parameters, **kwargs)

        return DocumentArray()

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search_endpoint(
        self, docs: Optional[DocumentArray] = None, parameters: dict = {}, **kwargs
    ):
        """Perform a vector similarity search and retrieve Document matches"""
        limit = int(parameters.get('limit', self.limit))
        search_filter_raw = parameters.get('filter', {})
        search_filter_orig = deepcopy(search_filter_raw)
        traversal_paths = parameters.get('traversal_paths', self.traversal_paths)
        docs = docs[traversal_paths][:1]  # only search on the first document for now

        if traversal_paths == '@c':
            retrieval_limit = limit * 3
        else:
            retrieval_limit = limit

        # if OCR detector was used to check if documents contain text in indexed image modality adjust retrieval step
        if self.columns and TAG_INDEXER_DOC_HAS_TEXT in [
            col[0] for col in self.columns
        ]:
            # enhance filter for one word queries to include the title
            if (
                len(docs[0].text.split()) == 1
                and not search_filter_raw
                and 'title' in [col[0] for col in self.columns]
            ):
                search_filter_raw = {'title': {'$regex': docs[0].text.lower()}}
            # search for documents which don't contain text
            search_filter_raw[TAG_INDEXER_DOC_HAS_TEXT] = {'$eq': False}
            search_filter = self.convert_filter_syntax(search_filter_raw)
            docs_with_matches_no_text = self.create_matches(
                docs,
                parameters,
                traversal_paths,
                limit,
                retrieval_limit,
                search_filter,
            )
            # search for documents which contain text
            search_filter_raw[TAG_INDEXER_DOC_HAS_TEXT] = {'$eq': True}
            search_filter = self.convert_filter_syntax(search_filter_raw)
            docs_with_matches_filter_title = self.create_matches(
                docs,
                parameters,
                traversal_paths,
                limit,
                retrieval_limit,
                search_filter,
            )
            # merge the results such that documents with text are retrieved at a later stage
            self.merge_matches_by_score_after_half(
                docs_with_matches_no_text,
                docs_with_matches_filter_title,
                limit,
            )
            docs_with_matches = docs_with_matches_no_text
            # if we didn't retrieve enough results, try to fetch more
            if len(docs_with_matches) < limit:
                search_filter = self.convert_filter_syntax(search_filter_orig)
                docs_with_additonal_matches = self.create_matches(
                    docs,
                    parameters,
                    traversal_paths,
                    limit,
                    retrieval_limit,
                    search_filter,
                )
                self.append_matches_if_not_exists(
                    docs_with_matches, docs_with_additonal_matches, limit
                )
        else:
            search_filter = self.convert_filter_syntax(search_filter_orig)
            docs_with_matches = self.create_matches(
                docs, parameters, traversal_paths, limit, retrieval_limit, search_filter
            )

        self.clean_response(docs_with_matches)
        return docs_with_matches

    def create_matches(
        self, docs, parameters, traversal_paths, limit, retrieval_limit, search_filter
    ):
        docs_copy = deepcopy(docs)
        self.search(docs_copy, parameters, retrieval_limit, search_filter)
        if traversal_paths == '@c':
            merge_matches_sum(docs_copy, limit)
        return docs_copy

    def append_matches_if_not_exists(
        self, docs_with_matches, docs_with_matches_to_add, limit
    ):
        # get all parent_ids of the matches of the docs_with_matches
        parent_ids = set()
        for doc, doc_to_add in zip(docs_with_matches, docs_with_matches_to_add):
            for match in doc.matches:
                parent_ids.add(match.parent_id)

            # append matches to docs_with_matches if they are not already in the matches
            for match in doc_to_add.matches:
                if match.parent_id not in parent_ids:
                    if len(doc.matches) >= limit:
                        break
                    doc.matches.append(match)

    def merge_matches_by_score_after_half(
        self,
        docs_with_matches: DocumentArray,
        docs_with_matches_to_add: DocumentArray,
        limit: int,
    ):
        # get all parent_ids of the matches of the docs_with_matches
        parent_ids = set()
        for doc, doc_to_add in zip(docs_with_matches, docs_with_matches_to_add):
            score_name = (
                list(doc.matches[0].scores.keys())[0]
                if len(doc.matches) > 0
                else self.metric
            )
            for match in doc.matches[: limit // 2]:
                parent_ids.add(match.parent_id)

            possible_matches = deepcopy(doc.matches[limit // 2 :]) + doc_to_add.matches
            doc.matches = doc.matches[: limit // 2]
            ids_scores = [
                (match.id, match.scores[score_name].value) for match in possible_matches
            ]
            ids_scores.sort(key=lambda x: x[1], reverse='similarity' in score_name)
            for id, _ in ids_scores:
                _match = possible_matches[id]
                if _match.id not in parent_ids:
                    if len(doc.matches) >= limit:
                        break
                    doc.matches.append(_match)

    @staticmethod
    def maybe_drop_blob_tensor(docs: DocumentArray):
        """Drops `blob` or `tensor` from documents which have `uri` attribute set and
        whose 'uri' is a data-uri or is either in the cloud(S3) or can be loaded."""
        for doc in docs:
            if doc.uri:
                if doc.text:
                    continue
                else:
                    try:
                        if not doc.uri.startswith(f'data:{doc.mime_type}') and not any(
                            [
                                doc.uri.startswith(cloud_bucket_prefix)
                                for cloud_bucket_prefix in CLOUD_BUCKET_PREFIXES
                            ]
                        ):
                            doc.load_uri_to_blob(timeout=10)
                        doc.blob = None
                        doc.tensor = None
                        doc.mime_type = None
                    except Exception as e:  # noqa E722
                        continue
        return docs

    @staticmethod
    def clean_response(docs):
        """Removes embedding & OCR tags from the root level and also from the matches."""

        def _clean_response(doc: Document):
            """Inplace removes embedding & tags associated with OCR detection."""
            doc.embedding = None
            for _tag in [TAG_INDEXER_DOC_HAS_TEXT, TAG_OCR_DETECTOR_TEXT_IN_DOC]:
                doc.tags.pop(_tag, None)

        for doc in docs:
            _clean_response(doc)
            for match in doc.matches:
                _clean_response(match)

    @staticmethod
    def parse_columns(columns):
        """Parse the columns to index"""
        valid_input_columns = ['str', 'float', 'int', 'bool']
        if columns:
            corrected_list = []
            for i in range(0, len(columns), 2):
                corrected_list.append((columns[i], columns[i + 1]))
            columns = corrected_list
            for n, t in columns:
                assert (
                    t in valid_input_columns
                ), f'column of type={t} is not supported. Supported types are {valid_input_columns}'
        return columns

    def load_document_list(self):
        """is needed for the list endpoint"""
        document_list = DocumentArray()
        for batch in self.batch_iterator():
            self.extend_inmemory_docs_and_tags(batch)
        self.document_list = DocumentArray(
            sorted([d for d in document_list], key=lambda x: x.id)
        )

    def extend_inmemory_docs_and_tags(self, batch):
        """Extend the in-memory DocumentArray with new documents"""
        for d in batch:
            tags = deepcopy(d.tags)
            for _tag in [TAG_INDEXER_DOC_HAS_TEXT, TAG_OCR_DETECTOR_TEXT_IN_DOC]:
                tags.pop(_tag, None)
            self.document_list.append(
                Document(id=d.id, uri=d.uri, tags=tags, parent_id=d.parent_id)
            )
            self.doc_id_tags[d.id] = tags

    def delete_inmemory_docs_and_tags(self, docs):
        """Delete documents from the in-memory DocumentArray"""
        for d in docs:
            del self.document_list[d.id]
            self.doc_id_tags.pop(d.id)

    def update_inmemory_docs_and_tags(self, docs):
        """Update documents in the in-memory DocumentArray"""
        self.delete_inmemory_docs_and_tags(docs)
        self.extend_inmemory_docs_and_tags(docs)

    def construct(self, **kwargs):
        """Calls the constructor of the specialized indexer"""
        raise NotImplementedError

    def batch_iterator(self):
        """Needs to be implemented in derived classes. Iterates over all documents in batches and yields them"""
        raise NotImplementedError

    def convert_filter_syntax(self, search_filter={}, search_filter_not={}):
        """Needs to be implemented in derived classes. Converts the filter syntax to the syntax of the specific indexer"""
        raise NotImplementedError

    def index(self, docs: DocumentArray, parameters: dict, **kwargs):
        """Needs to be implemented in derived classes. Indexes the documents"""
        raise NotImplementedError

    def delete(self, docs_to_delete, parameters: dict, **kwargs):
        """Needs to be implemented in derived classes. Deletes the documents"""
        raise NotImplementedError

    def search(
        self,
        docs: DocumentArray,
        parameters: dict,
        limit: int,
        search_filter: dict,
        **kwargs,
    ):
        """Needs to be implemented in derived classes. Searches the documents"""
        raise NotImplementedError
