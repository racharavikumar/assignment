"# Learning Wise - Web Application

A simple Flask-based web application for learning and demonstrating best practices in Python development, testing, and CI/CD.

## Overview

**Learning Wise** is a minimal yet complete web application that serves as a reference implementation for:
- Building Python web applications with Flask
- Writing comprehensive unit tests with pytest
- Generating test coverage reports
- Implementing CI/CD pipelines with Jenkins
- Python project packaging with `pyproject.toml`

## Features

### API Endpoints

1. **Index Route** (`GET /`)
   - Returns basic API information
   - Response: `{ "message": "Learning Wise API", "status": "running" }`

2. **Sum Calculator** (`GET /api/sum/<n>`)
   - Calculates the sum of integers from 1 to n
   - Parameter: `n` (integer)
   - Response: `{ "n": <n>, "sum": <result> }`
   - Example: `/api/sum/5` returns `{ "n": 5, "sum": 15 }`

## Installation

### Prerequisites
- Python 3.8+
- pip

### Setup

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On Unix/Linux/MacOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

## Testing

### Run All Tests

```bash
pytest -q
```

### Run Tests with Coverage Report

```bash
pytest --cov=learning_wise --cov-report=html --cov-report=term
```

This generates:
- Terminal report (text summary)
- HTML coverage report in `htmlcov/` directory

### Test Coverage

The project includes **12+ comprehensive tests** covering:
- Index route functionality
- Sum API endpoint with various inputs (0, 1, 5, 10, 20, 100, negative)
- App creation and configuration
- JSON response format validation
- Multiple sequential requests
- Edge cases and error handling

## Running the Application

```bash
# Activate virtual environment first
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Run the Flask app
python -m learning_wise

# The app will start on http://localhost:5000
```

## Project Structure

```
learning_wise/
├── learning_wise/           # Main package
│   └── __init__.py         # Flask app factory
├── tests/                   # Test suite
│   ├── __init__.py
│   └── test_sample.py      # Unit tests
├── sample_script.py        # Example script that tests the API
├── requirements.txt        # Project dependencies
├── pyproject.toml          # Project configuration
├── Jenkinsfile             # CI/CD pipeline
└── README.md              # This file
```

## Dependencies

- **Flask 2.0.0+**: Web framework
- **pytest 7.0+**: Testing framework
- **pytest-cov 3.0+**: Code coverage measurement

## CI/CD Pipeline

The project includes a **Jenkinsfile** that automates:

1. **Checkout** - Clone the repository
2. **Setup Environment** - Create Python virtual environment
3. **Install Dependencies** - Install all requirements
4. **Install Project** - Install the package in development mode
5. **Run sample_script.py** - Execute the test script
6. **Run Tests** - Execute pytest suite with JUnit XML report and coverage reports
7. **Build Package** - Create distributable wheel package

### Test Reports

- **JUnit XML reports**: `tests/junit-results.xml` - Test results visible in Jenkins
- **Coverage HTML report**: `htmlcov/index.html` - Interactive coverage dashboard

## Development

### Adding New Tests

Create test functions in `tests/test_sample.py`:

```python
def test_new_feature(client):
    """Test description."""
    response = client.get('/api/endpoint')
    assert response.status_code == 200
```

### Adding New API Routes

Edit `learning_wise/__init__.py`:

```python
@app.route('/api/new-route')
def new_route():
    return {'result': 'data'}
```

## Troubleshooting

### Virtual Environment Not Activating
- Ensure you're in the project root directory
- Use the correct activation command for your OS

### Tests Not Found
- Run from project root: `pytest tests/`
- Ensure `tests/__init__.py` exists

### Coverage Report Not Generated
- Install pytest-cov: `pip install pytest-cov`
- Run with: `pytest --cov=learning_wise --cov-report=html`

## License

This is a learning project. Free to use and modify.
" 
