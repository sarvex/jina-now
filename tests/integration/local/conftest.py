import multiprocessing
import os
from time import sleep

import pytest
from jina import Flow

from now.admin.utils import get_default_request_body

# special imports to make executors visible for flow yaml construction
from now.executor.autocomplete import NOWAutoCompleteExecutor2  # noqa: F401
from now.executor.gateway import NOWGateway  # noqa: F401
from now.executor.indexer.elastic import NOWElasticIndexer  # noqa: F401
from now.utils.jcloud.helpers import write_flow_file

BASE_URL = 'http://localhost:8081/api/v1'
SEARCH_URL = f'{BASE_URL}/search-app/search'


def get_request_body(secured):
    request_body = get_default_request_body(secured=secured)
    return request_body


@pytest.fixture
def get_flow(request, random_index_name, tmpdir):
    params = request.param
    docs, user_input = request.getfixturevalue(params)
    event = multiprocessing.Event()
    flow = FlowThread(event, user_input, tmpdir)
    flow.start()
    while not flow.is_flow_ready():
        sleep(1)
    if isinstance(params, tuple):
        yield
    elif isinstance(params, str):
        yield docs, user_input
    event.set()
    sleep(1)
    flow.terminate()


class FlowThread(multiprocessing.Process):
    def __init__(
        self,
        event,
        user_input,
        tmpdir,
    ):
        multiprocessing.Process.__init__(self)

        self.event = event
        user_input.app_instance.setup(user_input=user_input, testing=True)
        for executor in user_input.app_instance.flow_yaml['executors']:
            if not executor.get('external'):
                executor['uses_metas'] = {'workspace': str(tmpdir)}
        flow_file = os.path.join(tmpdir, 'flow.yml')
        write_flow_file(user_input.app_instance.flow_yaml, flow_file)
        self.flow = Flow.load_config(flow_file)

    def is_flow_ready(self):
        return self.flow.is_flow_ready()

    def run(self):
        with self.flow:
            while True:
                if self.event.is_set():
                    break
