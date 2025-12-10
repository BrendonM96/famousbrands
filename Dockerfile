# FB Nova Data Sync - Docker Image
# Syncs data from Production Synapse to Dev Synapse using CSV → Blob → COPY INTO

FROM python:3.11-slim

LABEL maintainer="Famous Brands Data Team"
LABEL description="FB Nova Synapse Data Sync Tool"
LABEL version="1.0.0"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies and ODBC Driver 17 for SQL Server
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        unixodbc-dev \
        apt-transport-https \
        ca-certificates \
    # Add Microsoft repository
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    # Install ODBC Driver 17
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    # Cleanup
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY sync_data_copy.py .
COPY "config file.xlsx" .

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# Health check - verify ODBC driver is installed
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import pyodbc; print(pyodbc.drivers())" || exit 1

# Run the sync script
CMD ["python", "sync_data_copy.py"]

