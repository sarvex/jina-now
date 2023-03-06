import os

import pytest
from hubble.payment.client import PaymentClient

from now.executor.gateway import hubble_report
from now.executor.gateway.hubble_report import (
    get_free_credits,
    report,
    set_free_credits_if_needed,
)


@pytest.mark.parametrize(
    'use_free_credits, remaining_free_credits, is_reporting',
    [[True, 999, False], [False, 1000, True]],
)
def test_report_usage(
    mocker, tmpdir, use_free_credits, remaining_free_credits, is_reporting
):
    os.environ['M2M_TOKEN'] = 'dummy_m2m_token'
    mocker.patch.object(hubble_report, 'workspace', return_value=str(tmpdir))
    mocker.patch.object(
        PaymentClient, 'get_authorized_jwt', return_value={'data': 'dummy_token'}
    )
    mocked_report_usage = mocker.patch.object(PaymentClient, 'report_usage')
    mocked_get_summary = mocker.patch.object(
        PaymentClient,
        'get_summary',
        return_value={
            'data': {'hasPaymentMethod': True, 'credits': 10},
            'status_code': 200,
        },
    )
    set_free_credits_if_needed(1000, is_initial=True)
    report(user_token='dummy_user_token', quantity=1, use_free_credits=use_free_credits)
    assert get_free_credits() == remaining_free_credits
    if is_reporting:
        mocked_report_usage.assert_called_once()
        mocked_get_summary.assert_called_once()
    else:
        mocked_report_usage.assert_not_called()
        mocked_get_summary.assert_not_called()
