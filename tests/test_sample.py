"""Tests for Learning Wise web application."""

import pytest
from learning_wise import create_app


@pytest.fixture
def client():
    """Create a test client for the Flask app."""
    app = create_app()
    app.config['TESTING'] = True
    
    with app.test_client() as client:
        yield client


def test_index_route(client):
    """Test the index route returns expected response."""
    response = client.get('/')
    assert response.status_code == 200
    data = response.get_json()
    assert data['message'] == 'Learning Wise API'
    assert data['status'] == 'running'


def test_sum_route_basic(client):
    """Test the sum endpoint with n=5."""
    response = client.get('/api/sum/5')
    assert response.status_code == 200
    data = response.get_json()
    assert data['n'] == 5
    assert data['sum'] == 15  # 1+2+3+4+5 = 15


def test_sum_route_zero(client):
    """Test the sum endpoint with n=0."""
    response = client.get('/api/sum/0')
    assert response.status_code == 200
    data = response.get_json()
    assert data['sum'] == 0


def test_sum_route_large(client):
    """Test the sum endpoint with a larger number."""
    response = client.get('/api/sum/100')
    assert response.status_code == 200
    data = response.get_json()
    assert data['sum'] == 5050  # Sum of 1 to 100
