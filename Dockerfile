FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY requirements.txt .
COPY pyproject.toml .
COPY learning_wise/ ./learning_wise/
COPY tests/ ./tests/
COPY sample_script.py .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Install the package in development mode
RUN pip install -e .

# Default command runs tests
CMD ["python", "-m", "pytest", "-q", "--junit-xml=tests/junit-results.xml", "--cov=learning_wise", "--cov-report=html", "--cov-report=xml"]
