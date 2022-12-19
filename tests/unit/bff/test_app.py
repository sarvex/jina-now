def test_check_liveness(client):
    response = client.get(f'/api/v1/app/ping')
    assert response.status_code == 200
    assert response.json() == 'pong!'


def test_read_root(client):
    response = client.get(f'/api/v1/app')
    assert response.status_code == 200


def test_get_docs(client):
    response = client.get(f'/api/v1/app/docs')
    assert response.status_code == 200


def test_get_redoc(client):
    response = client.get(f'/api/v1/app/redoc')
    assert response.status_code == 200
