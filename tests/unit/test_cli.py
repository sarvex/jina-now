# from argparse import ArgumentParser, Namespace
#
# from pytest_mock import MockerFixture
#
# from now.cli import cli, get_run_args
#
#
# class TestClass:
#     def __init__(self):
#         self.example = 'test'
#
#
# def test_run_args(mocker: MockerFixture):
#     test_input = TestClass()
#     mocker.patch.object(
#         ArgumentParser, 'parse_known_args', return_value=[test_input, None]
#     )
#     assert get_run_args()
#
#
# def test_cli_call(mocker: MockerFixture):
#     mocker.patch('now.run_all_k8s.start_now', return_value='STARTED')
#     mocker.patch('now.run_all_k8s.stop_now', return_value='STOPPED')
#     mocker.patch('now.run_all_k8s.compare_flows', return_value='COMPARED')
#
#     def _set_kwargs(task):
#         kwargs = {'now': task}
#         kwargs = Namespace(**kwargs)
#         return kwargs
#
#     cli(args=_set_kwargs('stop'))
#     cli(args=_set_kwargs('compare'))
