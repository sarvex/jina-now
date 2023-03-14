from pytest_mock import MockerFixture

from now.run_all_k8s import get_flow_status


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
