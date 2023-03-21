import datetime
import logging
import os
import threading
import traceback
from time import sleep

import requests
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
user_id = None  # Needed to get impersonation token (without expiry)


def current_time():
    return datetime.datetime.utcnow().isoformat() + 'Z'


def start_base_fee_thread(user_token, impersonation_token, hubble_user_id):
    # impersonation token can be session token since it remains same per user over time
    # It can be also use to send usage report to hubble
    logger.info('Starting base fee thread')
    global session_token, user_id
    # set either impersonation token (without expiry) or user token (with expiry) as session token
    session_token = impersonation_token or user_token
    user_id = hubble_user_id
    thread = threading.Thread(target=base_fee_thread, args=(session_token,))
    thread.start()


def base_fee_thread(token):
    while True:
        sleep(NOWGATEWAY_BASE_FEE_SLEEP_INTERVAL)
        init_payment_client(token)
        report(
            quantity_basic=NOWGATEWAY_BASE_FEE_QUANTITY,
            quantity_pro=NOWGATEWAY_BASE_FEE_QUANTITY,
        )


def report_search_usage(token):
    logger.info('** Entered report_search_usage() **')
    init_payment_client(token)
    report(
        quantity_basic=NOWGATEWAY_SEARCH_FEE_QUANTITY,
        quantity_pro=NOWGATEWAY_SEARCH_FEE_PRO_QUANTITY,
    )


def is_soon_to_expire(token):
    from hubble import Client

    client = Client(token=token)
    try:
        token_details = client.get_raw_session().json()
        expiry = token_details['data'].get('expireAt')
        # Check if expiry is within 7 days then renew
        if expiry:
            expiry = datetime.datetime.strptime(expiry, '%Y-%m-%dT%H:%M:%S.%fZ')
            expiring_soon = (
                expiry - datetime.timedelta(days=7)
            ) < datetime.datetime.utcnow()
            logger.info(f'Is token: {token} expiring soon? {expiring_soon}')
            return expiring_soon
    except Exception as e:
        logger.error(f'Error while getting user info with token {token} : {e}')
        return True
    return False


def get_impersonation_token(hubble_user_id=None):
    impersonation_token = None
    try:
        resp = requests.post(
            url='https://api.hubble.jina.ai/v2/rpc/user.m2m.impersonateUser',
            json={'userId': hubble_user_id or user_id},
            headers={'Authorization': f'Basic {os.environ.get("M2M", None)}'},
        )
        resp.raise_for_status()
        impersonation_token = resp.json()['data']
    except Exception as e:
        # Do not raise error here
        traceback.print_exc()
        logger.error(f'Error while getting impersonation token: {e}')

    return impersonation_token


def init_payment_client(token):
    global payment_client
    global session_token
    try:
        if payment_client is None:
            m2m_token = os.environ.get('M2M')
            if not m2m_token:
                raise ValueError('M2M not set in the environment')
            logger.info(f'M2M_TOKEN: {m2m_token[:10]}...{m2m_token[-10:]}')
            payment_client = PaymentClient(m2m_token=m2m_token)
        if session_token is None or is_soon_to_expire(session_token):
            logger.info('No/Old session token found. Getting new one...')
            if not token:
                raise ValueError(
                    'jwt not set in the request body. Can not get session token'
                )
            session_token = get_impersonation_token()  # to avoid re-authenticating
            logger.info(f'New session token: {session_token}')
    except Exception as e:
        import traceback

        traceback.print_exc()
        # Do not continue with request if initialization fails
        raise e


def report(quantity_basic, quantity_pro):
    logger.info(
        f'Session Token: {session_token} \n'
        f'at the time of reporting: {current_time()}'
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
