# Use the same base Python version as BlockFetcher
FROM python:3.12-slim

# Install system dependencies needed for building some Python packages
RUN apt-get update && \
    apt-get install -y gcc build-essential && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Poetry
# Consider installing a specific version if needed: pip install poetry==<version>
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Configure Poetry to not create virtual environment in container
RUN poetry config virtualenvs.create false

# Install dependencies (use --only main for production builds)
RUN poetry install --no-root

# Create logs directory relative to WORKDIR
RUN mkdir logs

# Copy application code (ensure all needed dirs like utils, config, scripts are copied)
COPY . .

RUN git clone --branch bds_eth_uniswapv3_core --single-branch --depth 1 https://github.com/powerloom/snapshotter-computes /computes

# Make entrypoint script executable
RUN chmod +x scripts/entrypoint.py

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Use entrypoint script to configure and start the service
CMD ["python", "scripts/entrypoint.py"]
