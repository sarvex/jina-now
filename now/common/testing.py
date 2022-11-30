import os

from now.constants import EXECUTOR_PREFIX


def handle_test_mode(config):
    if os.environ.get('NOW_TESTING', False):
        from now.executor.autocomplete import NOWAutoCompleteExecutor2
        from now.executor.indexer.in_memory import InMemoryIndexer
        from now.executor.preprocessor import NOWPreprocessor

        # this is a hack to make sure the import is not removed
        if NOWPreprocessor and NOWAutoCompleteExecutor2 and InMemoryIndexer:
            pass

        for k, v in config.items():
            # if 'Encoder' in str(v):
            #     config[k] = config[k].replace('+docker', '')
            if 'Indexer' in str(v):
                config[k] = 'InMemoryIndexer'
            if (
                isinstance(v, str)
                and 'jinahub' in v
                and (
                    # TODO: local testing on Qdrant needs to be disabled. At the moment, Qdrant does not start outside of docker
                    # TODO: same for elastic
                    not 'NOWQdrantIndexer' in v
                    and not 'NOWElasticIndexer' in v
                    and not 'CLIPOnnxEncoder' in v
                    and not 'NOWOCRDetector9' in v
                    and not 'TransformerSentenceEncoder' in v
                )
            ):
                config[k] = config[k].replace(EXECUTOR_PREFIX, '').split('/')[0]
