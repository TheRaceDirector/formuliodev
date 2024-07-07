# Build stage
FROM python:3.10.12-slim-buster AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libboost-system-dev \
    libboost-python-dev \
    libssl-dev \
    libtorrent-rasterbar-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.10.12-slim-buster

# Copy installed packages from builder stage
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libboost-system1.67.0 \
    libboost-python1.67.0 \
    libssl1.1 \
    libtorrent-rasterbar9 \
    && rm -rf /var/lib/apt/lists/*

# Add a non-root user
RUN groupadd -r appuser && useradd -r -g appuser -s /bin/bash -d /home/appuser appuser

# Set the working directory
WORKDIR /app

# Copy the application code
COPY . .

# Set the PYTHONPATH
ENV PYTHONPATH=/app

# Make the /app directory writable by all users
RUN chmod -R 777 /app

# Ensure the library is accessible
RUN ldconfig

# Make all Python scripts executable
RUN find . -name "*.py" -exec chmod +x {} +

# Create log directory and set permissions
RUN mkdir -p /var/log/gunicorn && chown appuser:appuser /var/log/gunicorn

# Switch to the non-root user
USER appuser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/ || exit 1

# Command to run the application
CMD ["gunicorn", "-c", "gunicorn_config.py", "formulio_addon:app"]

# Expose port 8000
EXPOSE 8000