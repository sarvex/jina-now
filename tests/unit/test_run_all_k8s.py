import pytest
from docarray import Document, DocumentArray, dataclass
from docarray.typing import Text
from pytest_mock import MockerFixture

from now.run_all_k8s import compare_flows, get_docarray, get_flow_status, stop_now


@dataclass
class MMStructure:
    description: Text


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


def _mock_post_response(mock_post):
    mock_json_response = [
        {
            "id": "1",
            "scores": {"cosine": {"value": 2}},
            "tags": {
                "title": "Test1",
            },
            "fields": {
                "text_0": {"uri": None, "text": "Test1", "blob": None},
                "video_0": {"uri": "https://example.com", "text": None, "blob": None},
            },
        },
        {
            "id": "2",
            "scores": {"cosine": {"value": 2}},
            "tags": {
                "title": "Test2",
            },
            "fields": {
                "video_0": {"uri": "https://example.com", "text": None, "blob": None},
                "text_0": {"uri": None, "text": "Test2", "blob": None},
            },
        },
    ]
    mock_status = 200
    mock_post.return_value.json.return_value = mock_json_response
    mock_post.return_value.status_code = mock_status


def test_compare_flows_with_flow_ids(mocker: MockerFixture):
    mock_post = mocker.patch('requests.post')
    _mock_post_response(mock_post)
    kwargs = {
        'flow_ids': '1,2',
        'dataset': 'team-now/pop-lyrics',
        'limit': 1,
        'disable_to_datauri': True,
        'results_per_table': 20,
    }
    compare_flows(**kwargs)


def test_compare_flows_no_flow_ids(mocker: MockerFixture):
    mock_post = mocker.patch('requests.post')
    _mock_post_response(mock_post)
    kwargs = {
        'path_score_calculation': './test_correct_response.json',
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
        return_value=DocumentArray([Document(MMStructure(description="test"))]),
    )

    compare_flows(**kwargs)
    with pytest.raises(Exception):
        kwargs['path_score_calculation'] = './test_wrong_response.json'
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
