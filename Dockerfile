# Build stage
FROM python:3.12 AS builder

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv

# Copy pyproject.toml and README.md together
COPY ./pyproject.toml ./uv.lock ./README.md /app/

WORKDIR /app/

# Create virtual environment in project directory and sync dependencies
RUN uv venv --python 3.12 && \
    uv sync --no-dev  # Install only main dependencies, no dev group

COPY ./src /app/src

# No need to re-sync unless src includes additional dependencies

# Production stage
FROM python:3.12-slim

WORKDIR /app/

# Copy the entire app directory including the .venv folder
COPY --from=builder /app /app

# Ensure Python uses the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
