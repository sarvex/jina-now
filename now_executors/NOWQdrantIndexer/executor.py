import os
import subprocess
from collections import defaultdict
from copy import deepcopy
from time import sleep
from typing import Dict, List, Optional, Tuple

import yaml
from docarray import DocumentArray
from jina.logging.logger import JinaLogger
from now_executors import NOWAuthExecutor as Executor
from now_executors import SecurityLevel, secure_request

QDRANT_CONFIG_PATH = '/qdrant/config/production.yaml'


class NOWQdrantIndexer6(Executor):
    """NOWQdrantIndexer6 indexes Documents into a Qdrant server using DocumentArray  with `storage='qdrant'`"""

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6333,
        collection_name: str = 'Persisted',
        distance: str = 'cosine',
        dim: int = 512,
        ef_construct: Optional[int] = None,
        full_scan_threshold: Optional[int] = None,
        m: Optional[int] = None,
        scroll_batch_size: int = 64,
        serialize_config: Optional[Dict] = None,
        columns: List[Tuple[str, str]] = None,
        traversal_paths: Tuple[str] = ('r',),
        **kwargs,
    ):
        """
        :param host: Hostname of the Qdrant server
        :param port: port of the Qdrant server
        :param collection_name: Qdrant Collection name used for the storage
        :param distance: The distance metric used for the vector index and vector search
        :param n_dim: number of dimensions
        :param ef_construct: The size of the dynamic list for the nearest neighbors (used during the construction).
            Controls index search speed/build speed tradeoff. Defaults to the default `ef_construct` in the Qdrant
            server.
        :param full_scan_threshold: Minimal amount of points for additional payload-based indexing. Defaults to the
            default `full_scan_threshold` in the Qdrant server.
        :param scroll_batch_size: batch size used when scrolling over the storage.
        :param serialize_config: DocumentArray serialize configuration.
        :param m: The maximum number of connections per element in all layers. Defaults to the default
            `m` in the Qdrant server.
        :param columns: precise columns for the Indexer (used for filtering).
        """
        super().__init__(**kwargs)

        if self.workspace:
            # set new storage to network file system location in WOLF
            qdrant_config = yaml.safe_load(open(QDRANT_CONFIG_PATH))
            qdrant_config['storage']['storage_path'] = os.path.join(
                self.workspace, 'user_input.json'
            )
            yaml.safe_dump(qdrant_config, open(QDRANT_CONFIG_PATH, 'w'))
            # if qdrant exists, then start it
        try:
            subprocess.Popen(['./run-qdrant.sh'])
            sleep(3)
            self.logger.info('Qdrant server started')
        except FileNotFoundError:
            self.logger.info('Qdrant not found, locally. So it won\'t be started.')

        # TODO make original work columns = {'title': '<this value is not used>'}
        column_dict = {[c[0]]: c[1] for c in columns}
        self._index = DocumentArray(
            storage='qdrant',
            config={
                'collection_name': collection_name,
                'host': host,
                'port': port,
                'n_dim': dim,
                'distance': distance,
                'ef_construct': ef_construct,
                'm': m,
                'scroll_batch_size': scroll_batch_size,
                'full_scan_threshold': full_scan_threshold,
                'serialize_config': serialize_config or {},
                'columns': column_dict,
            },
        )

        self.logger = JinaLogger(self.metas.name)

    @secure_request(on='/index', level=SecurityLevel.USER)
    def index(self, docs: DocumentArray, **kwargs):
        """Index new documents
        :param docs: the Documents to index
        """
        # for the experiment, we don't need blobs in the root and chunk level also, we set traversal_paths to '@c'
        docs = docs['@c']
        for d in docs:
            d.blob = None
        # qdrant needs a list of values when filtering on sentences
        for d in docs:
            d.tags['title'] = d.tags['title'].lower().split()
        self._index.extend(docs)
        #  prevent sending the data back by returning an empty DocumentArray
        return DocumentArray()

    @secure_request(on='/search', level=SecurityLevel.USER)
    def search(
        self,
        docs: 'DocumentArray',
        parameters: Dict = {},
        **kwargs,
    ):
        """Perform a vector similarity search and retrieve the full Document match

        :param docs: the Documents to search with
        :param parameters: Dictionary to define the `filter` that you want to use.
        :param kwargs: additional kwargs for the endpoint

        """
        docs = docs["@c"]

        docs_with_matches = self.create_matches(docs)

        if len(docs[0].text.split()) == 1:
            filter = {
                'must': [{'key': 'title', 'match': {'value': docs[0].text.lower()}}]
            }
            docs_with_matches_filter = self.create_matches(docs, filter)
            self.append_matches_if_not_exists(
                docs_with_matches_filter, docs_with_matches
            )
            return docs_with_matches_filter
        else:
            return docs_with_matches

    @secure_request(on='/delete', level=SecurityLevel.USER)
    def delete(self, parameters: Dict, **kwargs):
        """Delete entries from the index by id

        :param parameters: parameters of the request

        Keys accepted:
            - 'ids': List of Document IDs to be deleted
        """
        deleted_ids = parameters.get('ids', [])
        if len(deleted_ids) == 0:
            return
        del self._index[deleted_ids]

    @secure_request(on='/update', level=SecurityLevel.USER)
    def update(self, docs: DocumentArray, **kwargs):
        """Update existing documents
        :param docs: the Documents to update
        """

        for doc in docs:
            try:
                self._index[doc.id] = doc
            except IndexError:
                self.logger.warning(
                    f'cannot update doc {doc.id} as it does not exist in storage'
                )

    @secure_request(on='/filter', level=SecurityLevel.USER)
    def filter(self, parameters: Dict, **kwargs):
        """
        Query documents from the indexer by the filter `query` object in parameters. The `query` object must follow the
        specifications in the `find` method of `DocumentArray` in the docs https://docarray.jina.ai/fundamentals/documentarray/find/#filter-with-query-operators
        :param parameters: parameters of the request
        """
        return self._index.find(parameters['query'])

    @secure_request(on='/fill_embedding', level=SecurityLevel.USER)
    def fill_embedding(self, docs: DocumentArray, **kwargs):
        """Fill embedding of Documents by id

        :param docs: DocumentArray to be filled with Embeddings from the index
        """
        for doc in docs:
            doc.embedding = self._index[doc.id].embedding

    @secure_request(on='/clear', level=SecurityLevel.USER)
    def clear(self, **kwargs):
        """Clear the index"""
        self._index.clear()

    def close(self) -> None:
        super().close()
        del self._index

    def merge_matches_sum(self, docs, limit):
        # in contrast to merge_matches_min, merge_matches_avg sorts the parent matches by the average distance of all chunk matches
        # we have 3 chunks indexed for each root document but the matches might contain less than 3 chunks
        # in case of less than 3 chunks, we assume that the distance of the missing chunks is the same to the last match
        # m.score.value is a distance metric
        for d in docs:
            parent_id_count_and_sum_and_chunks = defaultdict(lambda: [0, 0, []])
            for m in d.matches:
                count_and_sum_and_chunks = parent_id_count_and_sum_and_chunks[
                    m.parent_id
                ]
                distance = m.scores['cosine'].value
                count_and_sum_and_chunks[0] += 1
                count_and_sum_and_chunks[1] += distance
                count_and_sum_and_chunks[2].append(m)
            all_matches = []
            for group in (3, 2, 1):
                parent_id_to_sum_and_chunks = {
                    parent_id: count_and_sum_and_chunks[1:]
                    for parent_id, count_and_sum_and_chunks in parent_id_count_and_sum_and_chunks.items()
                    if count_and_sum_and_chunks[0] == group
                }
                parent_to_sum_sorted = sorted(
                    parent_id_to_sum_and_chunks.items(), key=lambda x: x[1][0]
                )
                matches = [
                    sum_and_chunks[1][0]
                    for parent_id, sum_and_chunks in parent_to_sum_sorted
                ]
                all_matches.extend(matches)
                print(f'# num parents for group {group}: {len(matches)}')
            d.matches = all_matches[:limit]

    def create_matches(self, docs, filter={}):
        docs_copy = deepcopy(docs)
        docs_copy.match(self._index, filter=filter, limit=180)
        self.merge_matches_sum(docs_copy, 180)
        return docs_copy

    def append_matches_if_not_exists(self, docs_with_matches, docs_with_matches_to_add):
        # get all parent_ids of the matches of the docs_with_matches
        parent_ids = set()
        for doc, doc_to_add in zip(docs_with_matches, docs_with_matches_to_add):
            for match in doc.matches:
                parent_ids.add(match.parent_id)

            # append matches to docs_with_matches if they are not already in the matches
            for match in doc_to_add.matches:
                if match.parent_id not in parent_ids:
                    if len(doc.matches) >= 60:
                        break
                    doc.matches.append(match)
