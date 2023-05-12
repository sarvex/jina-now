def test_check_liveness(client):
    response = client.get('/api/v1/search-app/ping')
    assert response.status_code == 200
    assert response.json() == 'pong!'


def test_read_root(client):
    response = client.get('/api/v1/search-app')
    assert response.status_code == 200


def test_get_docs(client):
    response = client.get('/api/v1/search-app/docs')
    assert response.status_code == 200


def test_get_redoc(client):
    response = client.get('/api/v1/search-app/redoc')
    assert response.status_code == 200
