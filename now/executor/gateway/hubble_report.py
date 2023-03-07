import logging
import os
import threading
from time import sleep

from fastapi import status
from fastapi.responses import JSONResponse
from hubble.payment.client import PaymentClient

from now.constants import (
    NOWGATEWAY_BASE_FEE_QUANTITY,
    NOWGATEWAY_BASE_FEE_SLEEP_INTERVAL,
    NOWGATEWAY_SEARCH_FEE_PRO_QUANTITY,
    NOWGATEWAY_SEARCH_FEE_QUANTITY,
)

logger = logging.getLogger(__file__)
payment_client = None
authorized_jwt = None


def start_base_fee_thread(user_token):
    thread = threading.Thread(target=base_fee_thread, args=(user_token,))
    thread.start()


def base_fee_thread(user_token):
    while True:
        sleep(NOWGATEWAY_BASE_FEE_SLEEP_INTERVAL)
        report(
            user_token=user_token,
            quantity_basic=NOWGATEWAY_BASE_FEE_QUANTITY,
            quantity_pro=NOWGATEWAY_BASE_FEE_QUANTITY,
        )


def report_search_usage(user_token):
    report(
        user_token=user_token,
        quantity_basic=NOWGATEWAY_SEARCH_FEE_QUANTITY,
        quantity_pro=NOWGATEWAY_SEARCH_FEE_PRO_QUANTITY,
    )


def report(user_token, quantity_basic, quantity_pro):
    app_id = 'search'
    product_id = 'mm_query'
    try:
        global payment_client
        global authorized_jwt
        if payment_client is None:
            m2m_token = os.environ['M2M_TOKEN']
            payment_client = PaymentClient(m2m_token=m2m_token)
            authorized_jwt = payment_client.get_authorized_jwt(user_token=user_token)[
                'data'
            ]
        summary = get_summary(authorized_jwt)
        if summary['internal_product_id'] == 'free-plan':
            quantity = quantity_basic
        else:
            quantity = quantity_pro
        if can_charge(summary):
            payment_client.report_usage(authorized_jwt, app_id, product_id, quantity)
        else:
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={
                    'message': 'User has reached quota limit, please upgrade subscription'
                },
            )
    except Exception as e:
        print(e)


def get_summary(authorized_jwt):
    resp = payment_client.get_summary(token=authorized_jwt, app_id='search-api')
    has_payment_method = resp['data'].get('hasPaymentMethod', False)
    credits = resp['data'].get('credits', None)
    internal_product_id = resp['data'].get('internalProductId', None)
    user_account_status = resp['data'].get('userAccountStatus', None)
    return {
        'has_payment_method': has_payment_method,
        'credits': credits,
        'internal_product_id': internal_product_id,
        'user_account_status': user_account_status,
    }


def can_charge(summary):
    return (summary['credits'] > 0 or summary['has_payment_method']) and (
        summary['user_account_status'] == 'active'
    )
