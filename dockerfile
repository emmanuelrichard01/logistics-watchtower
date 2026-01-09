# Use a slim Python 3.11 image (Matches your local setup)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (needed for some Kafka libraries)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (Caching layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ src/

# Default command (can be overridden in docker-compose)
CMD ["python", "src/api.py"]