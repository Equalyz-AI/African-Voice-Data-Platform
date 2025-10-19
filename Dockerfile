FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev libffi-dev libpq-dev make && \
    rm -rf /var/lib/apt/lists/*

# Copy only requirements.txt first
COPY requirements.txt .

# Upgrade pip to latest compatible version
RUN pip install --upgrade "pip<24.1"

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop", "--http", "httptools"]


