from argparse import ArgumentParser

from pytest_mock import MockerFixture

from now.cli import get_run_args


class TestClass:
    def __init__(self):
        self.example = 'test'


def test_run_args(mocker: MockerFixture):
    test_input = TestClass()
    mocker.patch.object(
        ArgumentParser, 'parse_known_args', return_value=[test_input, None]
    )
    assert get_run_args()
