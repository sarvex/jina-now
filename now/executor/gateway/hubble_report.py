import logging
import os

from fastapi import status

logger = logging.getLogger(__file__)
from fastapi.responses import JSONResponse

client = None
authorized_jwt = None


def report(user_token, app_id, product_id, quantity):
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
