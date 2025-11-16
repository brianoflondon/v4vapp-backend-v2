import json
from random import shuffle

import httpx

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.witness_details import WitnessDetails

API_ENDPOINTS = [
    "https://hiveapi.actifit.io/hafbe-api/",
    "https://api.dev.openhive.network/hafbe-api/",
    "https://api.syncad.com/hafbe-api/",
    "https://techcoderx.com/hafbe-api/",
]

ICON = "🔍"


async def fetch_witness_details(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """
    Helper function to fetch witness details with retry logic.
    """
    timeout = httpx.Timeout(20.0, connect=10.0)
    logger.info(f"{ICON} fetching witness details from {url}")
    return await client.get(url, timeout=timeout)


def fix_witness_at_root(answer: dict) -> dict:
    """
    Fixes the witness details if they are at the root of the response.
    """
    if "witness_name" in answer:
        return {"witness": answer}
    return answer


async def get_hive_witness_details(
    hive_accname: str = "", ignore_cache: bool = False
) -> WitnessDetails | None:
    """
    Fetches details about a Hive witness.

    This function sends a GET request to "https://api.syncad.com/hafbe-api/witnesses"
    and retrieves the details of a Hive witness with the specified account name.
    It includes retry logic for transient network failures and falls back to Redis cache
    if the API is unavailable.

    Args:
        hive_accname (str): The account name of the Hive witness. If empty, fetches all witnesses.

    Returns:
        WitnessDetails | None: A WitnessDetails object containing the witness details, or None if the request fails.
    """
    cache_key = f"witness_{hive_accname}"
    if not ignore_cache:
        logger.info(f"{ICON} Checking Redis cache for witness details with key: {cache_key}")
        try:
            ttl = InternalConfig.redis_decoded.ttl(cache_key)
            if ttl and ttl > 0 and (1800 - ttl) < 300:
                cached_data = InternalConfig.redis_decoded.get(cache_key)
                if cached_data:
                    answer = json.loads(cached_data)
                    answer = fix_witness_at_root(answer)
                    logger.info(f"{ICON} Cache hit for {hive_accname}")
                    return WitnessDetails.model_validate(answer)
        except Exception as e:
            logger.warning(
                f"{ICON} Failed to check TTL or retrieve cached witness details from Redis: {e}",
                extra={"notification": False, "error": e},
            )
    # Attempt to fetch from API
    failure = False
    url: str = "not set"
    try:
        shuffled_endpoints = API_ENDPOINTS[:]
        shuffle(shuffled_endpoints)
        for api_url in shuffled_endpoints:
            url = f"{api_url}witnesses/{hive_accname}" if hive_accname else f"{api_url}witnesses/"
            try:
                timeout = httpx.Timeout(20.0, connect=10.0)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await fetch_witness_details(client, url)
                    response.raise_for_status()  # Raises an exception for 4xx/5xx status codes
                    answer = response.json()
                    answer = fix_witness_at_root(answer)
                    # Cache the result in Redis
                    try:
                        InternalConfig.redis_decoded.setex(
                            name=cache_key, value=json.dumps(answer), time=1800
                        )
                    except Exception as redis_error:
                        logger.warning(f"Failed to cache witness details in Redis: {redis_error}")

                    if failure:
                        logger.info(
                            f"Successfully fetched witness details for {hive_accname} after retrying with {url}",
                            extra={"notification": False},
                        )

                    return WitnessDetails.model_validate(answer)
            except httpx.HTTPStatusError as e:
                logger.warning(
                    f"{ICON} API returned status {e.response.status_code} for {url}",
                    extra={"notification": False, "error": e},
                )
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                logger.error(
                    f"{ICON} Connection failed to {url}: {e}",
                    extra={"notification": False, "error": e},
                )

            except ValueError as e:
                logger.warning(
                    f"{ICON} Failed to parse JSON response from {url}, trying again...",
                    extra={"notification": False, "error": e},
                )
                failure = True

    except Exception as e:
        logger.exception(
            f"{ICON} Unexpected error fetching witness details from {url}: {e}",
            extra={"notification": False, "error": e},
        )

    # Fallback to Redis cache
    try:
        if not InternalConfig.redis_decoded.ping():
            logger.error(
                f"{ICON} Redis is unavailable, cannot fetch cached data",
                extra={"notification": False},
            )
            return None

        cached_data = InternalConfig.redis_decoded.get(cache_key)
        if cached_data:
            answer = json.loads(cached_data)
            logger.info(
                f"{ICON} Successfully retrieved witness details from cache for {hive_accname}"
            )
            return WitnessDetails.model_validate(answer)
        else:
            logger.warning(f"{ICON} No cached data found for {cache_key}")
    except ValueError as e:
        logger.warning(
            f"{ICON} Failed to parse JSON response from {url}",
            extra={"notification": False, "error": e},
        )
    except Exception as redis_error:
        logger.error(
            f"{ICON} Failed to retrieve witness details from Redis cache: {redis_error}",
            extra={"notification": False, "error": redis_error},
        )

    logger.warning(
        f"{ICON} Failed to get witness details for {hive_accname} from both API and cache",
        extra={"notification": True},
    )
    return None
