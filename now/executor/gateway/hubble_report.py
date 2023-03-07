import json
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
    NOWGATEWAY_FREE_CREDITS,
)

logger = logging.getLogger(__file__)
payment_client = None
authorized_jwt = None


def start_base_fee_thread(user_token):
    thread = threading.Thread(target=base_fee_thread, args=(user_token,))
    thread.start()


def base_fee_thread(user_token):
    set_free_credits_if_needed(NOWGATEWAY_FREE_CREDITS, is_initial=True)
    while True:
        sleep(NOWGATEWAY_BASE_FEE_SLEEP_INTERVAL)
        report(
            user_token=user_token,
            quantity=NOWGATEWAY_BASE_FEE_QUANTITY,
            use_free_credits=False,
        )


def workspace():
    return (
        f'/data/{os.environ["K8S_NAMESPACE_NAME"]}'
        if 'K8S_NAMESPACE_NAME' in os.environ
        else '~/.cache/jina-now'
    )


def credits_path():
    return os.path.join(workspace(), 'free_credits.json')


def get_free_credits():
    with open(credits_path(), 'r') as f:
        free_credits = json.load(f)['free_credits']
    return free_credits


def set_free_credits_if_needed(value, is_initial=False):
    if is_initial and os.path.exists(credits_path()):
        return
    with open(credits_path(), 'w') as f:
        json.dump({'free_credits': value}, f)


def report(user_token, quantity, use_free_credits=False):
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
        free_credits = get_free_credits()
        if use_free_credits and free_credits > 0:
            set_free_credits_if_needed(free_credits - quantity)
            return
        if can_charge(authorized_jwt):
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


def can_charge(authorized_jwt):
    resp = payment_client.get_summary(token=authorized_jwt, app_id='search-api')
    has_payment_method = resp['data'].get('hasPaymentMethod', False)
    remain_credits = resp['data'].get('credits', None)
    if remain_credits is None:
        remain_credits = 0.00001
        logger.error(f'Failed to get payment summary: {resp}')
    return remain_credits > 0 or has_payment_method
