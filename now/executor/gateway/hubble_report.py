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
session_token = None


def current_time():
    return datetime.datetime.utcnow().isoformat() + 'Z'


def base_fee_thread(user_token):
    while True:
        sleep(NOWGATEWAY_BASE_FEE_SLEEP_INTERVAL)
        report(
            quantity_basic=NOWGATEWAY_BASE_FEE_QUANTITY,
            quantity_pro=NOWGATEWAY_BASE_FEE_QUANTITY,
        )


def start_base_fee_thread(user_token, impersonation_token):
    # impersonation token is now our session token since it remains same per user over time
    logger.info('Starting base fee thread')
    global session_token
    if not impersonation_token:
        session_token = impersonation_token
    init_payment_client(user_token)
    thread = threading.Thread(target=base_fee_thread, args=(user_token,))
    thread.start()


def report_search_usage(user_token):
    logger.info('** Entered report_search_usage() **')
    init_payment_client(user_token)
    report(
        quantity_basic=NOWGATEWAY_SEARCH_FEE_QUANTITY,
        quantity_pro=NOWGATEWAY_SEARCH_FEE_PRO_QUANTITY,
    )


def init_payment_client(user_token):
    global payment_client
    global session_token
    try:
        if payment_client is None:
            m2m_token = os.environ.get('M2M')
            if not m2m_token:
                raise ValueError('M2M not set in the environment')
            logger.info(f'M2M_TOKEN: {m2m_token[:10]}...{m2m_token[-10:]}')
            payment_client = PaymentClient(m2m_token=m2m_token)
        if session_token is None:
            logger.info('No session token found. Getting one...')
            # Can also get session token using the below method
            impersonation_token = payment_client.get_authorized_jwt(
                user_token=user_token
            )['data']
            session_token = (
                impersonation_token  # store it as a global variable to avoid re-auth
            )
    except Exception as e:
        import traceback

        traceback.print_exc()
        # Do not continue with request if initialization fails
        raise e


def report(quantity_basic, quantity_pro):
    logger.info(
        f'Session Token: {session_token} \n at the time of reporting: {current_time()}'
    )
    app_id = 'search'
    product_id = 'free-plan'
    try:
        summary = get_summary()
        logger.info(f'Credits before: {summary["credits"]}')
        if summary['internal_product_id'] == 'free-plan':
            quantity = quantity_basic
        else:
            quantity = quantity_pro
        if can_charge(summary):
            payment_client.report_usage(session_token, app_id, product_id, quantity)  # type: ignore
            logger.info(f'**** `{round(quantity, 3)}` credits charged ****')
            summary = get_summary()
            logger.info(f'Credits after: {summary["credits"]}')
        else:
            logger.info(f'**** Could not charge. Check payment summary ****')
        logger.info(f'Payment summary: {summary}')
    except Exception as e:
        import traceback

        traceback.print_exc()
        # Do not continue with request if payment fails
        raise e


def get_summary():
    resp = payment_client.get_summary(token=session_token, app_id='search')  # type: ignore
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
