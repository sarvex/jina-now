import pytest
from hubble.payment.client import PaymentClient

from now.executor.gateway.hubble_report import init_payment_client, report


@pytest.mark.parametrize(
    'has_payment_method, user_credits, user_account_status, internal_product_id, cost, num_report_usage_calls',
    [
        (True, 10, 'active', 'free-plan', 2, 1),  # standard case
        (False, 0, 'active', 'free-plan', 2, 0),  # test no payment no credits
        (True, 0, 'active', 'free-plan', 2, 1),  # test no credits but payment method
        (True, 10, 'inactive', 'free-plan', ..., 0),  # test inactive user
        (True, 10, 'active', 'pro-plan', 1, 1),  # test pro user
    ],
)
def test_report_usage(
    mocker,
    has_payment_method,
    user_credits,
    user_account_status,
    internal_product_id,
    cost,
    num_report_usage_calls,
):
    mocker.patch.object(
        PaymentClient, 'get_authorized_jwt', return_value={'data': 'dummy_token'}
    )
    mocker.patch(
        'now.executor.gateway.hubble_report.get_impersonation_token',
        return_value='dummy_token',
    )
    mocker.patch(
        'now.executor.gateway.now_gateway.get_impersonation_token',
        return_value='dummy_token',
    )
    mocked_report_usage = mocker.patch.object(PaymentClient, 'report_usage')
    mocker.patch.object(
        PaymentClient,
        'get_summary',
        return_value={
            'data': {
                'hasPaymentMethod': has_payment_method,
                'credits': user_credits,
                'userAccountStatus': user_account_status,
                'internalProductId': internal_product_id,
            },
            'status_code': 200,
        },
    )
    init_payment_client(token='dummy_token')
    report(quantity_basic=2, quantity_pro=1)
    # assert that the mocked_report_usage was called once with the expected arguments

    if num_report_usage_calls == 0:
        mocked_report_usage.assert_not_called()
    else:
        mocked_report_usage.assert_called_once_with(
            'dummy_token', 'search', 'free-plan', cost
        )
