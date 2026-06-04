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


def test_sum_route_one(client):
    """Test the sum endpoint with n=1."""
    response = client.get('/api/sum/1')
    assert response.status_code == 200
    data = response.get_json()
    assert data['n'] == 1
    assert data['sum'] == 1


def test_sum_route_ten(client):
    """Test the sum endpoint with n=10."""
    response = client.get('/api/sum/10')
    assert response.status_code == 200
    data = response.get_json()
    assert data['n'] == 10
    assert data['sum'] == 55  # 1+2+...+10 = 55


def test_sum_route_twenty(client):
    """Test the sum endpoint with n=20."""
    response = client.get('/api/sum/20')
    assert response.status_code == 200
    data = response.get_json()
    assert data['sum'] == 210  # 1+2+...+20 = 210


def test_sum_route_negative(client):
    """Test the sum endpoint with invalid negative number.
    
    Note: Flask's <int:n> converter doesn't support negative numbers in URLs,
    so this should return 404 Not Found.
    """
    response = client.get('/api/sum/-5')
    # Negative values won't match the route, so we expect 404
    assert response.status_code == 404


def test_app_creation():
    """Test that the app can be created without errors."""
    app = create_app()
    assert app is not None
    assert app.name == 'learning_wise'


def test_app_config_testing():
    """Test that app can be configured for testing."""
    app = create_app()
    app.config['TESTING'] = True
    assert app.config['TESTING'] is True


def test_json_response_format(client):
    """Test that API responses are in JSON format."""
    response = client.get('/api/sum/3')
    assert response.content_type == 'application/json'
    data = response.get_json()
    assert isinstance(data, dict)
    assert 'n' in data
    assert 'sum' in data


def test_multiple_requests(client):
    """Test that multiple requests work correctly."""
    # Request 1
    response1 = client.get('/api/sum/2')
    data1 = response1.get_json()
    assert data1['sum'] == 3
    
    # Request 2
    response2 = client.get('/api/sum/3')
    data2 = response2.get_json()
    assert data2['sum'] == 6
    
    # Verify they're independent
    assert data1['sum'] != data2['sum']
