# GitHub Copilot / AI Agent Instructions for v4vapp-backend-v2

## Quick summary
- Purpose: Backend bridge between Hive (on-chain) and Lightning (LND) for v4v.app.
- Major components:
  - Monitors (processes): `hive_monitor_v2.py`, `lnd_monitor_v2.py`, `db_monitor.py` — read chain events and emit internal events
  - Conversion layer: `src/v4vapp_backend_v2/conversion/*` (e.g. `keepsats_to_hive.py`, `hive_to_keepsats.py`) — business rules for moving value
  - LND gRPC client: `src/v4vapp_backend_v2/lnd_grpc/*` — generated proto stubs + `lnd_client.py` wrapper
  - API / Admin: `src/api_v2.py`, admin app at `src/v4vapp_backend_v2/admin/*` (FastAPI/Typer)
  - Database & accounting: `src/v4vapp_backend_v2/database/*` and `src/v4vapp_backend_v2/accounting/*` (MongoDB ledger-centric model)
  - Config & logging: `src/v4vapp_backend_v2/config/setup.py` and `config/` YAML files (Pydantic models)

## Big-picture notes (helpful for agent decisions)
- System flow: Hive operations -> conversion/process modules -> ledger updates and LND calls -> accounting/services update. Look for events in `process/` and `events/` to trace flows.
- Persistent state is in MongoDB (collections configured via `config/*.yaml`). Tests expect a replica set for some tests; many unit tests use `mongomock-motor`.
- LND integration is via generated protobufs in `src/v4vapp_backend_v2/lnd_grpc/`. Avoid editing generated `_pb2.py/_grpc.py` directly; regenerate from `.proto` files when needed.
- Configuration is loaded via `InternalConfig` / `config/setup.py`. Tests monkeypatch `BASE_CONFIG_PATH` to `tests/data/config` (see `tests/conftest.py`). Use `config/devhive.config.yaml` for full-stack runs.

## How to run / test (explicit commands and environment)
- CI uses `uv` (UV package manager) and runs `uv run pytest`. Locally you can either:
  - Use UV (recommended for parity with CI):
    - `uv venv --python 3.12`
    - `uv sync --group dev`
    - `uv run pytest`
  - Or use an activated virtualenv: `python -m pip install -e '.[dev]'` then `python -m pytest`
- Full-stack tests and some functional tests spawn real monitors; they need services or `docker-compose up`:
  - `docker-compose up --build` (uses services defined in `docker-compose.yaml`: MongoDB, Redis, various monitors)
  - The MongoDB docker service is configured as a replica set on port `37017` and expects `LOCAL_TAILSCALE_IP`/`.env` to be set on a home machine environment.
- Tests often require secrets/env vars (examples in CI): `HIVE_ACC_TEST`, `HIVE_MEMO_TEST_KEY`. See `.github/workflows/pytest.yml` for how CI supplies them.

## Proto / gRPC workflow
- When modifying `.proto` files under `src/v4vapp_backend_v2/lnd_grpc/`:
  - Use specific versions to avoid conflicts (from README):
    - `grpcio`, `grpcio-tools` and `googleapis-common-protos` pinned as in README (e.g. `1.62.0` historically worked)
  - Regenerate Python stubs (example):
    - `python -m grpc_tools.protoc -I=. --python_out=src/v4vapp_backend_v2/lnd_grpc --grpc_python_out=src/v4vapp_backend_v2/lnd_grpc path/to/*.proto`
  - Re-run any type stub generation if you use `mypy-protobuf` to generate pyi files.
  - Note: `pyproject.toml` excludes generated files from linters (`*_pb2.py`, `*_grpc.py`) via Ruff config.

## Conventions & patterns to follow
- Pydantic is used for config and data models (see `src/v4vapp_backend_v2/config/setup.py` and `src/v4vapp_backend_v2/models/*`). Prefer model validators and typed models for validation.
- Singletons: `InternalConfig` uses a module-level singleton; tests reset `_instance` in `conftest.py` — follow that pattern for tests.
- Tests: many tests load fixture test config from `tests/data/config` via monkeypatching; mimic that when adding tests that require config.
- Logging: uses a custom logger `v4vapp_backend_v2.config.mylogger` and emoji prefixes in messages — keep line length and log format compatibility.
- Async patterns: use `pytest-asyncio` and `asyncio` fixtures; there is a session-scoped event loop in `tests/conftest.py`.

## Short debugging tips
- Local full-stack run: use `tests.conftest:full_stack_setup` to see how monitors are started (it runs `python src/*.py` processes and looks for readiness lines). Use that to replicate startup behavior.
- When tests fail on DB/Redis, inspect containers from `docker-compose` (`docker compose ps` / `docker logs ...`) or run `uv run pytest -k <testname> -s` for verbose output.

## Files to inspect when you need context
- Entry points: `src/hive_monitor_v2.py`, `src/lnd_monitor_v2.py`, `src/db_monitor.py`, `src/api_v2.py`, `src/v4vapp_backend_v2/admin/run_admin.py`
- Business logic: `src/v4vapp_backend_v2/process/`, `src/v4vapp_backend_v2/conversion/`, `src/v4vapp_backend_v2/accounting/`
- LND gRPC: `src/v4vapp_backend_v2/lnd_grpc/` (proto + generated stubs)
- Configs & logging: `config/*.yaml`, `src/v4vapp_backend_v2/config/setup.py`, `src/v4vapp_backend_v2/config/mylogger.py`
- Tests & test data: `tests/` and `tests/data/` (many fixtures/example inputs live here)

## Safety and project-specific constraints
- Avoid changing generated proto files in place — regenerate instead.
- Keep compatibility with existing ledger/accounting assumptions — tests encode many invariants (balance sheet, ledgers). Changes that affect ledger semantics must include accounting tests (`tests/accounting/`).

---
If you'd like, I can iterate and add small code snippets (example test templates, typical mock patterns) or merge this into an existing file if you want different wording. Any sections you want me to expand or clarify? ✅
