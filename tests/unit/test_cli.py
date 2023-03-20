from argparse import ArgumentParser, Namespace

from pytest_mock import MockerFixture

from now.cli import cli, get_run_args


class TestClass:
    def __init__(self):
        self.example = 'test'


class Args:
    def __init__(self, task):
        self.now = task


def test_run_args(mocker: MockerFixture):
    test_input = TestClass()
    mocker.patch.object(
        ArgumentParser, 'parse_known_args', return_value=[test_input, None]
    )
    assert get_run_args()


def test_cli_call(mocker: MockerFixture):
    mocker.patch('now.run_all_k8s.start_now', return_value='STARTED')
    mocker.patch('now.run_all_k8s.stop_now', return_value='STOPPED')
    mocker.patch('now.run_all_k8s.compare_flows', return_value='COMPARED')
    kwargs_stop = {
        'now': 'stop',
    }
    kwargs_compare = {
        'now': "compare",
    }
    kwargs_stop = Namespace(**kwargs_stop)
    kwargs_compare = Namespace(**kwargs_compare)
    cli(args=kwargs_stop)
    cli(args=kwargs_compare)
