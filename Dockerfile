FROM python:3.12

COPY ./pyproject.toml ./poetry.lock* /app/

WORKDIR /app/

RUN pip install poetry && poetry install --only main --no-root --no-directory

COPY ./src /app

RUN poetry install --only main

# COPY .certs /app/.certs
# COPY logging_configs/ /app/logging_configs

# # CMD [ "bash" ]

# CMD ["poetry", "run", "python", "main.py"]

# src/v4vapp_backend_v2/lnd_grpc/main.py
#python /Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/src/v4vapp_backend_v2/lnd_grpc/main.py