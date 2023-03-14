from dataclasses import dataclass

from pytest_mock import MockerFixture

from now.run_all_k8s import compare_flows, get_flow_status


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


def test_compare_flows(mocker: MockerFixture):
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
