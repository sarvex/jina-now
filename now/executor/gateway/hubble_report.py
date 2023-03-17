import datetime
import logging
import os
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
payment_client = None
authorized_jwt = None


def current_time():
    return datetime.datetime.utcnow().isoformat() + 'Z'


def start_base_fee_thread(user_token):
    logger.info(f'Starting base fee thread at {current_time()}')
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
    logger.info('** Entered report_search_usage() **')
    report(
        user_token=user_token,
        quantity_basic=NOWGATEWAY_SEARCH_FEE_QUANTITY,
        quantity_pro=NOWGATEWAY_SEARCH_FEE_PRO_QUANTITY,
    )


def report(user_token, quantity_basic, quantity_pro):
    logger.info('*******************************')
    logger.info(f'Charging user with token {user_token} at {current_time()}')
    app_id = 'search'
    product_id = 'free-plan'
    try:
        global payment_client
        global authorized_jwt
        if payment_client is None:
            m2m_token = os.environ.get('M2M_TOKEN')
            if not m2m_token:
                raise ValueError('M2M_TOKEN not set in the environment')
            logger.info(f'M2M_TOKEN: {m2m_token[:10]}...{m2m_token[-10:]}')
            payment_client = PaymentClient(m2m_token=m2m_token)
        if authorized_jwt is None:
            logger.info('No authorized JWT found. Getting one...')
            if not user_token:
                raise ValueError('No user token found. Please provide jwt token')
            jwt = payment_client.get_authorized_jwt(user_token=user_token)['data']
            authorized_jwt = jwt  # store it as a global variable to avoid re-auth
        logger.info(f'Authorized JWT: {authorized_jwt[:10]}...{authorized_jwt[-10:]}')
        summary = get_summary()
        logger.info(f'Credits before: {summary["credits"]}')
        if summary['internal_product_id'] == 'free-plan':
            quantity = quantity_basic
        else:
            quantity = quantity_pro
        if can_charge(summary):
            payment_client.report_usage(authorized_jwt, app_id, product_id, quantity)
            logger.info(
                f'**** `{round(quantity, 3)}` credits charged at time: {current_time()} ****'
            )
            summary = get_summary()
            logger.info(f'Credits after: {summary["credits"]}')
        else:
            logger.info(f'**** Could not charge. Check payment summary ****')
        logger.info(f'Payment summary: {summary}')
        logger.info('*******************************')
    except Exception as e:
        import traceback

        traceback.print_exc()
        # Do not continue with request if payment fails
        raise e


def get_summary():
    resp = payment_client.get_summary(token=authorized_jwt, app_id='search')  # type: ignore
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
