import os
import subprocess
from time import sleep

import yaml
from jina import DocumentArray

from now.executor.abstract.base_indexer import NOWBaseIndexer as Executor


class NOWQdrantIndexer15(Executor):
    """NOWQdrantIndexer15 indexes Documents into a Qdrant server using DocumentArray  with `storage='qdrant'`"""

    # override
    def construct(self, **kwargs):
        setup_qdrant_server(self.workspace, self.logger)
        self._index = DocumentArray(
            storage='qdrant',
            config={
                'collection_name': 'Persisted',
                'host': 'localhost',
                'port': 6333,
                'n_dim': self.dim,
                'distance': self.metric,
                'ef_construct': None,
                'm': None,
                'scroll_batch_size': 64,
                'full_scan_threshold': None,
                'serialize_config': {},
                'columns': self.columns,
            },
        )
        self.range_operators = ['$gt', '$lt', '$get', '$let']

    # override
    def batch_iterator(self):
        """Iterator which iterates through the documents of self._index and yields batches"""
        batch = []
        for item in self._index:
            batch.append(item)
            if len(batch) == 1000:
                yield batch
                batch = []
        if batch:
            yield batch

    # override
    def convert_filter_syntax(self, search_filter):
        """supports exact matches and range filter"""
        conditions = []
        for attribute, condition in search_filter.items():
            for operator, value in condition.items():
                if operator in self.range_operators:
                    operator_type = 'range'
                    operator_string = operator.replace('$', '')
                elif operator == '$eq':
                    operator_type = 'match'
                    operator_string = 'value'
                else:
                    continue
                conditions.append(
                    {"key": attribute, operator_type: {operator_string: value}}
                )

        search_filter = {"must": conditions} if conditions else {}
        return search_filter

    # override
    def index(self, docs: DocumentArray, parameters: dict, **kwargs):
        """Index new documents"""
        # qdrant needs a list of values when filtering on sentences
        for d in docs:
            if 'title' in d.tags:
                d.tags['title'] = d.tags['title'].lower().split()
        self._index.extend(docs)

    # override
    def delete(self, documents_to_delete, parameters: dict = {}, **kwargs):
        """
        Delete endpoint to delete document/documents from the index.
        Filter conditions can be passed to select documents for deletion.
        """
        for d in documents_to_delete:
            del self._index[d.id]

    # override
    def search(
        self,
        docs: DocumentArray,
        parameters: dict,
        limit: int,
        search_filter: dict,
        **kwargs
    ):
        """Perform a vector similarity search and retrieve Document matches"""
        docs.match(self._index, filter=search_filter, limit=limit)


def setup_qdrant_server(workspace, logger):
    qdrant_config_path = '/qdrant/config/production.yaml'
    if workspace and os.path.exists(qdrant_config_path):
        logger.info('set new storage to network file system location in WOLF')
        qdrant_config = yaml.safe_load(open(qdrant_config_path))
        qdrant_config['storage'] = {
            'storage_path': os.path.join(workspace, 'user_input.json')
        }
        yaml.safe_dump(qdrant_config, open(qdrant_config_path, 'w'))
        logger.info('if qdrant exists, then start it')
    try:
        subprocess.Popen(['./run-qdrant.sh'])
        sleep(3)
        logger.info('Qdrant server started')
    except FileNotFoundError:
        logger.info('Qdrant not found, locally. So it won\'t be started.')
