from __future__ import annotations

import asyncio
import json
import random
from time import time
from typing import List

import httpx

ICON = "🐝API"

HIVE_API_ENDPOINTS = [
    "https://hiveapi.actifit.io/",
    "https://api.dev.openhive.network/",
    "https://api.syncad.com/",
    "https://techcoderx.com/",
]

HIVE_API_ENDPOINT_DOWN_TTL_SECONDS = 300
HIVE_API_ENDPOINT_REFRESH_INTERVAL_SECONDS = 300

REDIS_HIVE_API_ENDPOINT_DOWN_PREFIX = "hive_api:endpoint_down:"
REDIS_HIVE_API_ENDPOINT_REFRESH_KEY = "hive_api:endpoint_health:last_refresh"


def _endpoint_redis_key(endpoint: str) -> str:
    return f"{REDIS_HIVE_API_ENDPOINT_DOWN_PREFIX}{endpoint.rstrip('/')}"


def _get_internal_config_refs():
    # Imported lazily to avoid import cycles with config.setup.
    from v4vapp_backend_v2.config.setup import InternalConfig, logger

    return InternalConfig, logger


def _redis_client():
    InternalConfig, _ = _get_internal_config_refs()
    return getattr(InternalConfig, "redis_decoded", None)


def mark_hive_api_endpoint_failed(
    endpoint: str,
    error: Exception | str | None = None,
    ttl_seconds: int = HIVE_API_ENDPOINT_DOWN_TTL_SECONDS,
) -> None:
    """Mark an endpoint as temporarily down in Redis.

    A TTL-based key is used so endpoints automatically return to service
    when the cooldown expires.
    """
    redis_client = _redis_client()
    _, logger = _get_internal_config_refs()
    if not redis_client:
        return

    try:
        payload = {
            "endpoint": endpoint,
            "error": str(error) if error else "",
            "failed_at": int(time()),
        }
        redis_client.setex(_endpoint_redis_key(endpoint), ttl_seconds, json.dumps(payload))
        logger.warning(
            f"{ICON} Marked endpoint as failed: {endpoint} (error: {error})",
            extra={"notification": False},
        )
    except Exception as redis_error:
        logger.warning(
            f"{ICON} Failed to cache endpoint failure for {endpoint}: {redis_error}",
            extra={"notification": False, "error": redis_error},
        )


def mark_hive_api_endpoint_healthy(endpoint: str) -> None:
    """Clear temporary down state for an endpoint."""
    redis_client = _redis_client()
    _, logger = _get_internal_config_refs()
    if not redis_client:
        return

    try:
        redis_client.delete(_endpoint_redis_key(endpoint))
        logger.info(
            f"{ICON} Marked endpoint healthy: {endpoint}",
            extra={"notification": False, "endpoint": endpoint},
        )
    except Exception as redis_error:
        logger.warning(
            f"{ICON} Failed to clear endpoint failure key for {endpoint}: {redis_error}",
            extra={"notification": False, "error": redis_error},
        )


def is_hive_api_endpoint_down(endpoint: str) -> bool:
    """Return True when the endpoint is currently on cooldown in Redis."""
    redis_client = _redis_client()
    if not redis_client:
        return False
    try:
        return bool(redis_client.get(_endpoint_redis_key(endpoint)))
    except Exception:
        return False


def get_hive_api_endpoints(
    *,
    shuffle_endpoints: bool = True,
    include_down: bool = False,
) -> List[str]:
    """Return currently usable Hive API endpoints.

    If all endpoints are in cooldown or Redis is unavailable, this returns
    the full static list so callers always have a fallback path.
    """
    endpoints = HIVE_API_ENDPOINTS[:]
    if not include_down:
        active = [endpoint for endpoint in endpoints if not is_hive_api_endpoint_down(endpoint)]
        if active:
            endpoints = active

    if shuffle_endpoints:
        random.shuffle(endpoints)

    return endpoints


async def _check_hive_api_endpoint(endpoint: str) -> tuple[str, bool, str]:
    """Probe an endpoint and return (endpoint, ok, error_message)."""
    test_url = endpoint + "balance-api/accounts/v4vapp-test/balances"
    try:
        timeout = httpx.Timeout(2.0, connect=2.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(test_url)
            response.raise_for_status()
        return endpoint, True, ""
    except Exception as error:
        return endpoint, False, str(error)


async def working_api_endpoints(
    force_recheck: bool = False,
    refresh_interval_seconds: int = HIVE_API_ENDPOINT_REFRESH_INTERVAL_SECONDS,
) -> List[str]:
    """Probe endpoints and update Redis-backed health cache.

    The health probe is rate-limited by a Redis key so repeated calls do not
    probe on every request. Set ``force_recheck=True`` to bypass rate limiting.
    """
    redis_client = _redis_client()
    _, logger = _get_internal_config_refs()

    if redis_client and not force_recheck:
        try:
            recently_checked = redis_client.get(REDIS_HIVE_API_ENDPOINT_REFRESH_KEY)
            if recently_checked:
                return get_hive_api_endpoints(shuffle_endpoints=False)
            redis_client.setex(
                REDIS_HIVE_API_ENDPOINT_REFRESH_KEY,
                refresh_interval_seconds,
                str(int(time())),
            )
        except Exception as redis_error:
            logger.warning(
                f"{ICON} Failed endpoint refresh throttling via Redis: {redis_error}",
                extra={"notification": False, "error": redis_error},
            )

    tasks: dict[str, asyncio.Task] = {}
    async with asyncio.TaskGroup() as tg:
        for endpoint in HIVE_API_ENDPOINTS:
            tasks[endpoint] = tg.create_task(_check_hive_api_endpoint(endpoint))

    good_endpoints: List[str] = []
    for endpoint, task in tasks.items():
        endpoint_checked = endpoint
        ok = False
        error_text = "unknown"
        try:
            endpoint_checked, ok, error_text = task.result()
        except Exception as task_error:
            error_text = str(task_error)

        if ok:
            mark_hive_api_endpoint_healthy(endpoint_checked)
            good_endpoints.append(endpoint_checked)
        else:
            mark_hive_api_endpoint_failed(endpoint_checked, error=error_text)

    if good_endpoints:
        return good_endpoints

    logger.warning(
        f"{ICON} No working Hive API endpoints from health probe; using full fallback list",
        extra={"notification": False, "hive_api_endpoints": HIVE_API_ENDPOINTS},
    )
    return HIVE_API_ENDPOINTS[:]
