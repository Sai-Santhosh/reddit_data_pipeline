# Production-ready Dockerfile for Apache Airflow
FROM apache/airflow:2.7.2-python3.11

# Switch to root user for system package installation
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy requirements file
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy source code
COPY src/ /opt/airflow/src/
COPY dags/ /opt/airflow/dags/
COPY config/ /opt/airflow/config/
COPY notebooks/ /opt/airflow/notebooks/

# Set Python path
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD airflow version || exit 1
