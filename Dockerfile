# Build stage
FROM python:3.12 AS builder

COPY ./pyproject.toml ./poetry.lock* /app/

WORKDIR /app/

RUN pip install poetry
RUN poetry config virtualenvs.in-project true  # Ensure venv is created in project dir
RUN poetry install --only main --no-root --no-directory

COPY ./src /app

RUN poetry install --only main

# Production stage
FROM python:3.12-slim

WORKDIR /app/

# Copy the entire app directory including the .venv folder
COPY --from=builder /app /app

# Ensure Python uses the virtual environment
ENV PATH="/app/.venv/bin:$PATH"
