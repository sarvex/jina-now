import pytest


@pytest.mark.parametrize(
    'app_name',
    [
        ('search'),
        ('text-to-video'),
    ],
)
class TestParametrized:
    def test_get_docs(self, client, app_name):
        response = client.get(f'/api/v1/{app_name}/docs')
        assert response.status_code == 200

    def test_get_redoc(self, client, app_name):
        response = client.get(f'/api/v1/{app_name}/redoc')
        assert response.status_code == 200
