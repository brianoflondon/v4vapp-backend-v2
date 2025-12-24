# Nectar / Hive changes (defensive list→tuple normalization) 🔧

## Summary

A small defensive change was made to avoid an environment-specific error that appears when passing Python lists through to `nectar.Hive()` in some runtime setups (notably Docker images). The error seen in logs was similar to:

```
Error binding parameter 2: type 'list' is not supported
```

This file documents the change, rationale, tests added, and how to verify in your environment.

---

## Why this was necessary

- The `nectar.Hive` constructor sometimes forwards parameters into an internal SQLite binding or code path that does not accept Python `list` objects for certain parameters (for example `keys` or `node`).
- Locally (development/debug) the environment tolerated lists; inside Docker the same runtime raised an SQLite binding error and prevented the service from starting.

## The change

- In `src/v4vapp_backend_v2/hive/hive_extras.py`, before calling `Hive(...)` we now normalize the parameter types:

```py
# Before calling Hive(...)
if "keys" in kwargs and isinstance(kwargs["keys"], list):
    kwargs["keys"] = tuple(kwargs["keys"])
if "node" in kwargs and isinstance(kwargs["node"], list):
    kwargs["node"] = tuple(kwargs["node"])
```

- This keeps the public API (you can still pass lists) but ensures we pass types that are safe for downstream consumers.

## Tests added

- `tests/test_hive_extras_keys_type.py` verifies that:
  - Passing `keys` as a `list` results in `keys` forwarded to `Hive` as a `tuple`.
  - Passing `node` as a `list` results in `node` forwarded to `Hive` as a `tuple`.

Run the new tests with:

- `uv run pytest tests/test_hive_extras_keys_type.py` (recommended, to match CI)
- or `python -m pytest tests/test_hive_extras_keys_type.py`

## Files changed

- Modified: `src/v4vapp_backend_v2/hive/hive_extras.py` (normalize list -> tuple for `keys` and `node` before invoking `Hive`)
- Added: `tests/test_hive_extras_keys_type.py` (unit tests)
- Added: `docs/nectar_changes.md` (this file)

## How to verify in Docker

1. Build the image you use on your other machine (the same Dockerfile you use for production). Example (from repo root):

```bash
docker build -t v4vapp-api:local .
# or use your normal build flow (compose, CI, etc.)
```

2. Start the container and watch logs for the previous error message.

3. Run the test suite inside the container (or locally) to ensure the new unit tests pass.

## Notes / Follow-ups

- If we find other parameters that get forwarded to low-level bindings and cause the same type issues, we should add similar normalization.
- If you'd like, I can also add a short debug log when normalization happens to make future diagnosis easier.

---

If you want this write-up expanded into a short PR description, I can draft that as well.
