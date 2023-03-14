from argparse import Namespace

from now.cli import cli
from now.constants import DatasetTypes


def test_flow_logs(
    cleanup,
    random_flow_name,
    index_fields,
    filter_fields,
    model_selection,
    dataset,
):
    kwargs = {
        'now': 'logs',
        'flow_name': random_flow_name,
        'dataset_type': DatasetTypes.DEMO,
        'admin_name': 'team-now',
        'index_fields': index_fields,
        'filter_fields': filter_fields,
        'dataset_name': dataset,
        'secured': True,
        'api_key': None,
        'additional_user': False,
    }
    kwargs.update(model_selection)
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)

    print('RESPONSE: ', response)
    assert len(response) > 0
