from now.cli.parser import get_main_parser


def test_main_parser():
    main_parser = get_main_parser()
    assert main_parser.epilog
