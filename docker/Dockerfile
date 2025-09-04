FROM python:3.11-slim

# (Optional) system deps helpful for native wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first for better build caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the whole repo into /app inside the image
COPY . /app

# Make /app/src importable and logs unbuffered
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

# Default command is provided by docker-compose (python -m monitor.main)
