import datetime
import logging
import os
import sys
import threading
from time import sleep

from hubble.payment.client import PaymentClient

from now.constants import (
    NOWGATEWAY_BASE_FEE_QUANTITY,
    NOWGATEWAY_BASE_FEE_SLEEP_INTERVAL,
    NOWGATEWAY_SEARCH_FEE_PRO_QUANTITY,
    NOWGATEWAY_SEARCH_FEE_QUANTITY,
)

logger = logging.getLogger(__file__)
logger.setLevel(os.environ.get('JINA_LOG_LEVEL', 'INFO'))
logger.addHandler(logging.StreamHandler(sys.stdout))


def current_time():
    return datetime.datetime.utcnow().isoformat() + 'Z'


def start_base_fee_thread(authorized_jwt):
    thread = threading.Thread(target=base_fee_thread, args=(authorized_jwt,))
    thread.start()


def base_fee_thread(authorized_jwt):
    while True:
        sleep(NOWGATEWAY_BASE_FEE_SLEEP_INTERVAL)
        report(
            authorized_jwt=authorized_jwt,
            quantity_basic=NOWGATEWAY_BASE_FEE_QUANTITY,
            quantity_pro=NOWGATEWAY_BASE_FEE_QUANTITY,
        )


def report_search_usage(user_token):
    report(
        user_token=user_token,
        quantity_basic=NOWGATEWAY_SEARCH_FEE_QUANTITY,
        quantity_pro=NOWGATEWAY_SEARCH_FEE_PRO_QUANTITY,
    )


def report(quantity_basic, quantity_pro, authorized_jwt=None, user_token=None):
    if not authorized_jwt and not user_token:
        raise Exception('Either authorized_jwt or user_token must be provided')
    app_id = 'search'
    product_id = 'mm_query'
    try:
        m2m_token = os.environ.get('M2M_TOKEN')
        if not m2m_token:
            logger.info('M2M_TOKEN not set in the environment')
        payment_client = PaymentClient(m2m_token=m2m_token)
        if not authorized_jwt:
            authorized_jwt = payment_client.get_authorized_jwt(user_token)['data']
        summary = get_summary(authorized_jwt, payment_client)
        logger.info(f'Payment summary: \n{summary}')
        if summary['internal_product_id'] == 'free-plan':
            quantity = quantity_basic
        else:
            quantity = quantity_pro
        if can_charge(summary):
            payment_client.report_usage(authorized_jwt, app_id, product_id, quantity)
            logger.info(
                f'`{quantity}` credits charged for {user_token} at {current_time()}'
            )
        else:
            logger.info(f'Could not charge {user_token}. Check payment summary')
    except Exception as e:
        import traceback

        traceback.print_exc()
        logger.critical(e)


def get_summary(authorized_jwt, payment_client):
    resp = payment_client.get_summary(token=authorized_jwt, app_id='search-api')
    has_payment_method = resp['data'].get('hasPaymentMethod', False)
    user_credits = resp['data'].get('credits', None)
    internal_product_id = resp['data'].get('internalProductId', None)
    user_account_status = resp['data'].get('userAccountStatus', None)
    return {
        'has_payment_method': has_payment_method,
        'credits': user_credits,
        'internal_product_id': internal_product_id,
        'user_account_status': user_account_status,
    }


def can_charge(summary):
    return (
        (summary['credits'] is not None and summary['credits'] > 0)
        or summary['has_payment_method']
    ) and summary['user_account_status'] == 'active'
