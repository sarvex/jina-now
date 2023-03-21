import pytest
from docarray import dataclass
from pytest_mock import MockerFixture

from now.run_all_k8s import compare_flows, get_docarray, get_flow_status, stop_now


@dataclass
class MMStructure:
    is_multimodal: bool


def test_flow_status(mocker: MockerFixture):
    mocker.patch(
        'now.deployment.deployment.list_all_wolf',
        return_value=[{'name': 'test', 'id': 1}],
    )
    mocker.patch(
        'now.deployment.deployment.status_wolf',
        return_value='SUCCEEDED',
    )
    _, flow_id, _ = get_flow_status(action='delete', cluster='test')


def test_compare_flows_with_flow_ids(mocker: MockerFixture):
    mocker.patch(
        'now.compare.compare_flows.compare_flows_for_queries',
        return_value='PASS',
    )
    mocker.patch(
        'now.run_all_k8s.get_docarray',
        return_value=[MMStructure(is_multimodal=True)],
    )
    kwargs = {
        'flow_ids': '1,2',
        'dataset': 'test',
        'limit': 1,
        'disable_to_datauri': True,
        'results_per_table': 20,
    }
    compare_flows(**kwargs)


def test_compare_flows_no_flow_ids(mocker: MockerFixture):
    kwargs = {
        'path_req_params': 'tests/unit/test_correct_response.json',
        'dataset': 'test',
        'limit': 1,
        'disable_to_datauri': True,
        'results_per_table': 20,
    }

    mocker.patch(
        'now.compare.compare_flows.compare_flows_for_queries',
        return_value='PASS',
    )
    mocker.patch(
        'now.run_all_k8s.get_docarray',
        return_value=[MMStructure(is_multimodal=True)],
    )

    compare_flows(**kwargs)
    with pytest.raises(Exception):
        kwargs['path_req_params'] = 'tests/unit/test_wrong_response.json'
        compare_flows(**kwargs)


def test_stop(mocker: MockerFixture):
    mock_response = mocker.Mock()
    mock_response.json.return_value = {'message': 'DELETED'}

    def _mock_flow_status():
        return {'status': {'phase': 'Serving'}}, 'flow_id', 'cluster'

    mocker.patch(
        'now.run_all_k8s.get_flow_status',
        return_value=_mock_flow_status(),
    )
    mocker.patch(
        'now.deployment.deployment.terminate_wolf',
        return_value='SUCCEEDED',
    )
    mocker.patch('requests.delete', return_value=mock_response)
    stop_now()


def test_get_docarray(mocker: MockerFixture):
    mocker.patch(
        'docarray.DocumentArray.pull',
        return_value='SUCCEEDED',
    )
    get_docarray('test')
