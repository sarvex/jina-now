import uuid

from docarray import dataclass
from docarray.typing import Text
from pytest_mock import MockerFixture

from now.admin.benchmark_flow import measure_latency, measure_qps
from now.admin.flow_results import main
from now.admin.update_api_keys import update_api_keys
from now.admin.update_email import update_emails


@dataclass
class MMDoc:
    description: Text


def test_update_emails(mocker: MockerFixture):
    mocker.patch('requests.post', return_value='PASSED')
    update_emails(
        emails=['hoenicke.florian@gmail.com'],
        remote_host='grpc://0.0.0.0',
    )


def test_update_api_keys(mocker: MockerFixture):
    mocker.patch('requests.post', return_value='PASSED')
    api_key = str(uuid.uuid4())
    assert update_api_keys(
        api_keys=[api_key],
        remote_host='grpc://0.0.0.0',
    )


def test_measure_latency(mocker: MockerFixture):
    mocker.patch('now.admin.benchmark_flow.call', return_value='PASSED')
    assert measure_latency()


def test_measure_qps(mocker: MockerFixture):
    mocker.patch('now.admin.benchmark_flow.call', return_value='PASSED')
    measure_qps(0, 1)


def test_flow_result(mocker: MockerFixture):
    mocker.patch('now.admin.flow_results.call', return_value=['test'], autospec=True)
    main()
