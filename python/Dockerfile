# syntax=docker/dockerfile:1

# ==============================================================================
# Stage 1: Builder — install dependencies into a virtualenv
# ==============================================================================
FROM python:3.11-slim AS builder

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create virtualenv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy package metadata and source so setuptools can build the wheel
COPY pyproject.toml .
COPY src/ src/

# Install runtime dependencies only (no dev extras)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# ==============================================================================
# Stage 2: Runtime — copy only the virtualenv and app code (no build tools)
# ==============================================================================
FROM python:3.11-slim AS runtime

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create non-root user for security
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home appuser

# Copy virtualenv from builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source code
COPY . /app

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Healthcheck: verify clickhouse_connect is importable
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import clickhouse_connect; print('ok')" || exit 1

# Default command
CMD ["python", "main.py", "demo"]
