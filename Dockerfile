FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    cron \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/pipeline

# Copy and install Python dependencies
COPY requirements.txt /opt/pipeline/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /opt/pipeline/data/bronze \
             /opt/pipeline/data/silver \
             /opt/pipeline/data/gold \
             /opt/pipeline/logs

# Copy pipeline files
COPY scripts/ /opt/pipeline/scripts/
COPY sql/ /opt/pipeline/sql/
COPY orchestrator.py /opt/pipeline/orchestrator.py

# Make orchestrator executable
RUN chmod +x /opt/pipeline/orchestrator.py

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command (will be overridden by docker-compose)
CMD ["python3", "/opt/pipeline/orchestrator.py"]
