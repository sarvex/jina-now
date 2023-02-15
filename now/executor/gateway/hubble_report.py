import os

client = None
authorized_jwt = None


def report(user_token, app_id, product_id, quantity):
    try:
        global client
        global authorized_jwt
        if client is None:
            from hubble.payment.client import PaymentClient

            m2m_token = os.environ['M2M_TOKEN']
            client = PaymentClient(m2m_token=m2m_token)
            authorized_jwt = client.get_authorized_jwt(user_token=user_token)['data']
        client.report_usage(authorized_jwt, app_id, product_id, quantity)
    except Exception as e:
        print(e)
