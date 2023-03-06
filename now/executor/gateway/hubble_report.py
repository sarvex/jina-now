import json
import logging
import os

from fastapi import status
from fastapi.responses import JSONResponse

logger = logging.getLogger(__file__)
client = None
authorized_jwt = None


def credits_path():
    ws = f'/data/{os.environ["K8S_NAMESPACE_NAME"]}'
    return os.path.join(ws, 'free_credits.json')


def get_free_credits():
    with open(credits_path(), 'r') as f:
        free_credits = json.load(f)['free_credits']
    return free_credits


def set_free_credits_if_needed(value, is_initial=False):
    if is_initial and os.path.exists(credits_path()):
        return
    with open(credits_path(), 'w') as f:
        json.dump({'free_credits': value}, f)


def report(user_token, app_id, product_id, quantity, use_free_credits=False):
    try:
        global payment_client
        global authorized_jwt
        if payment_client is None:
            from hubble.payment.client import PaymentClient

            m2m_token = os.environ['M2M_TOKEN']
            payment_client = PaymentClient(m2m_token=m2m_token)
            authorized_jwt = payment_client.get_authorized_jwt(user_token=user_token)[
                'data'
            ]
        if use_free_credits:
            free_credits = get_free_credits()
            if free_credits > 0:
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
    else:
        logger.error(f'Failed to get payment summary: {resp}')
    return remain_credits > 0 or has_payment_method
