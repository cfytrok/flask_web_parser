import re
import time

import pytest

from app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True

    with app.test_client() as client:
        yield client


def test_get_home(client):
    response = client.get('/')
    assert response.status_code == 200
    assert b'<form' in response.data


def test_post_home(client):
    response = client.post('/', data={'start_url': 'http://example.com'})
    assert response.status_code == 200
    assert re.match(b'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', response.data)
    id = response.data.decode()

    response2 = client.get(f'/{id}')
    assert response2.status_code == 200
    assert b'Pending' in response2.data
    time.sleep(2)

    response3 = client.get(f'/{id}')
    assert response3.status_code == 200
    assert b'Pending' not in response3.data
    assert b'href' in response3.data
