import json
from random import shuffle

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.witness_details import WitnessDetails

API_ENDPOINTS = [
    "https://api.syncad.com/hafbe-api/witnesses",
    "https://techcoderx.com/hafbe-api/witnesses",
]


@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, min=1, max=10),  # Exponential backoff: 1s, 2s, 4s
    retry=retry_if_exception_type(
        (httpx.ConnectError, httpx.ConnectTimeout)
    ),  # Retry on connection issues
    reraise=True,  # Reraise the last exception if all retries fail
)
async def fetch_witness_details(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """
    Helper function to fetch witness details with retry logic.
    """
    return await client.get(url, timeout=20)


async def get_hive_witness_details(hive_accname: str = "") -> WitnessDetails | None:
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
    try:
        ttl = InternalConfig.redis_decoded.ttl(cache_key)
        if ttl and ttl > 0 and (1800 - ttl) < 300:
            cached_data = await InternalConfig.redis_decoded.get(cache_key)
            answer = json.loads(cached_data)
            return WitnessDetails.model_validate(answer)
    except Exception:
        pass
    # Attempt to fetch from API
    failure = False
    try:
        shuffled_endpoints = API_ENDPOINTS[:]
        shuffle(shuffled_endpoints)
        url: str = ""
        for api_url in shuffled_endpoints:
            url = f"{api_url}/{hive_accname}" if hive_accname else api_url
            try:
                async with httpx.AsyncClient() as client:
                    response = await fetch_witness_details(client, url)
                    response.raise_for_status()  # Raises an exception for 4xx/5xx status codes

                    answer = response.json()
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
                    f"API returned status {e.response.status_code} for {url}",
                    extra={"notification": False, "error": e},
                )
            except httpx.ConnectError as e:
                logger.error(
                    f"Connection failed to {url}: {e}", extra={"notification": False, "error": e}
                )
            except httpx.ConnectTimeout as e:
                logger.warning(
                    f"Connection timeout to {url}: {e}", extra={"notification": False, "error": e}
                )
            except ValueError as e:
                logger.warning(
                    f"Failed to parse JSON response from {url}, trying again...",
                    extra={"notification": False, "error": e},
                )
                failure = True

    except Exception as e:
        logger.exception(
            f"Unexpected error fetching witness details from {url}: {e}",
            extra={"notification": False, "error": e},
        )

    # Fallback to Redis cache
    try:
        if not InternalConfig.redis_decoded.ping():
            logger.error(
                "Redis is unavailable, cannot fetch cached data", extra={"notification": False}
            )
            return None

        cached_data = InternalConfig.redis_decoded.get(cache_key)
        if cached_data:
            answer = json.loads(cached_data)
            logger.info(f"Successfully retrieved witness details from cache for {hive_accname}")
            return WitnessDetails.model_validate(answer)
        else:
            logger.warning(f"No cached data found for {cache_key}")
    except ValueError as e:
        logger.warning(
            f"Failed to parse JSON response from {url}",
            extra={"notification": False, "error": e},
        )
    except Exception as redis_error:
        logger.error(
            f"Failed to retrieve witness details from Redis cache: {redis_error}",
            extra={"notification": False, "error": redis_error},
        )

    logger.warning(
        f"Failed to get witness details for {hive_accname} from both API and cache",
        extra={"notification": True},
    )
    return None
