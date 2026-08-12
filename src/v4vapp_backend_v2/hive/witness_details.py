import json

import httpx

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.hive_api_endpoints import (
    HIVE_API_ENDPOINTS,
    get_hive_api_endpoints,
    mark_hive_api_endpoint_failed,
    mark_hive_api_endpoint_healthy,
)
from nectar.hive import Hive
from v4vapp_backend_v2.hive.hive_extras import default_hive_nodes
from v4vapp_backend_v2.hive_models.witness_details import WitnessDetails

ICON = "🔍"


async def fetch_witness_details(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """
    Helper function to fetch witness details with retry logic.
    """
    timeout = httpx.Timeout(20.0, connect=10.0)
    logger.debug(f"{ICON} fetching witness details from {url}")
    return await client.get(url, timeout=timeout)


def fix_witness_at_root(answer: dict) -> dict:
    """
    Fixes the witness details if they are at the root of the response.
    """
    if "witness_name" in answer:
        return {"witness": answer}
    return answer


def _parse_vest_value(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _convert_vests_to_vote_power(hive, raw_vests: str | int | float | None) -> float:
    """Convert raw vesting share values into Hive Power using the Hive client."""
    parsed_vests = _parse_vest_value(raw_vests)
    if parsed_vests is None:
        return 0.0
    return hive.vests_to_token_power(parsed_vests)


def _enhance_witness_voter_with_total_value(voter: dict, hive) -> dict:
    if not isinstance(voter, dict):
        return voter

    # The Hive voter API returns vesting amounts in raw vest shares.
    # Convert those raw vest values into Hive Power (HP) for display.
    vest_value = voter.get("vests") or voter.get("account_vests")
    proxy_vest_value = voter.get("proxied_vests") or voter.get("proxy_vests")
    vote_value = 0.0
    proxy_value = 0.0

    if hive is not None:
        try:
            vote_value = _convert_vests_to_vote_power(hive, vest_value)
            proxy_value = _convert_vests_to_vote_power(hive, proxy_vest_value)
        except Exception as e:
            logger.warning(
                f"{ICON} Failed to calculate total value for voter {voter.get('voter_name', voter.get('account'))}: {e}",
                extra={"notification": False, "error": e},
            )

    voter["vote_value"] = vote_value
    voter["proxy_value"] = proxy_value
    voter["total_value"] = vote_value + proxy_value
    return voter


def _get_witness_voter_key(voter: dict) -> str | None:
    if not isinstance(voter, dict):
        return None
    return voter.get("voter_name") or voter.get("account")


def _normalize_witness_voter(voter: dict) -> dict:
    if not isinstance(voter, dict):
        return voter
    if "vote_value" not in voter:
        voter["vote_value"] = 0.0
    if "proxy_value" not in voter:
        voter["proxy_value"] = 0.0
    if "total_value" not in voter:
        voter["total_value"] = voter["vote_value"] + voter["proxy_value"]
    return voter


def _build_witness_voter_map(voters: list[dict] | dict) -> dict[str, dict]:
    if isinstance(voters, dict):
        return {
            key: _normalize_witness_voter(value)
            for key, value in voters.items()
            if isinstance(value, dict)
        }

    voter_map: dict[str, dict] = {}
    for voter in voters:
        normalized = _normalize_witness_voter(voter)
        key = _get_witness_voter_key(normalized)
        if key:
            voter_map[key] = normalized
    return voter_map


async def get_hive_witness_details(
    hive_accname: str = "", ignore_cache: bool = False, time_delay: int = 0
) -> WitnessDetails | None:
    """
    Fetches details about a Hive witness.

    This function sends a GET request to "https://api.syncad.com/hafbe-api/witnesses"
    and retrieves the details of a Hive witness with the specified account name.
    It includes retry logic for transient network failures and falls back to Redis cache
    if the API is unavailable.

    Args:
        hive_accname (str): The account name of the Hive witness. If empty, fetches all witnesses.
        ignore_cache (bool): If True, ignores the Redis cache and fetches fresh data from the API.
        time_delay (int): Optional delay in seconds before fetching to allow for cache updates.

    Returns:
        WitnessDetails | None: A WitnessDetails object containing the witness details, or None if the request fails.
    """
    cache_key = f"witness:{hive_accname}"
    if not ignore_cache:
        logger.debug(f"{ICON} Checking Redis cache for witness details with key: {cache_key}")
        try:
            ttl = InternalConfig.redis_decoded.ttl(cache_key)
            if ttl and ttl > 0 and (1800 - ttl) < 300:
                cached_data = InternalConfig.redis_decoded.get(cache_key)
                if cached_data:
                    answer = json.loads(cached_data)
                    answer = fix_witness_at_root(answer)
                    logger.debug(f"{ICON} Cache hit for {hive_accname}")
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
        shuffled_endpoints = (
            get_hive_api_endpoints(shuffle_endpoints=True) or HIVE_API_ENDPOINTS[:]
        )
        for api_url in shuffled_endpoints:
            url = (
                f"{api_url}hafbe-api/witnesses/{hive_accname}"
                if hive_accname
                else f"{api_url}hafbe-api/witnesses/"
            )
            try:
                timeout = httpx.Timeout(5.0, connect=5.0)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await fetch_witness_details(client, url)
                    response.raise_for_status()  # Raises an exception for 4xx/5xx status codes
                    answer = response.json()
                    answer = fix_witness_at_root(answer)
                    mark_hive_api_endpoint_healthy(api_url)
                    # Cache the result in Redis
                    try:
                        InternalConfig.redis_decoded.set(
                            name=cache_key, value=json.dumps(answer), ex=1800
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
                mark_hive_api_endpoint_failed(api_url, error=e)
                logger.warning(
                    f"{ICON} API returned status {e.response.status_code} for {url}",
                    extra={"notification": False, "error": e},
                )
                failure = True
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                mark_hive_api_endpoint_failed(api_url, error=e)
                logger.error(
                    f"{ICON} Connection failed to {url}: {e}",
                    extra={"notification": False, "error": e},
                )
                failure = True
            except ValueError as e:
                mark_hive_api_endpoint_failed(api_url, error=e)
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
        extra={"notification": False},
    )
    return None


async def get_witness_voters(
    witness_name: str,
    page_size: int = 1000,
    ignore_cache: bool = False,
) -> dict[str, dict] | None:
    """
    Fetches and caches the full map of voters for a given witness.

    Args:
        witness_name (str): The witness whose voters should be downloaded.
        page_size (int): Number of voters to request per page.
        ignore_cache (bool): If True, bypasses Redis and fetches fresh data.

    Returns:
        dict[str, dict] | None: A map of voter account name to voter record, or None if the request fails.
    """
    cache_key = f"witness:{witness_name}:voters"
    if not ignore_cache:
        try:
            cached_data = InternalConfig.redis_decoded.get(cache_key)
            if cached_data:
                cached_voters = json.loads(cached_data)
                return _build_witness_voter_map(cached_voters)
        except Exception as e:
            logger.warning(
                f"{ICON} Failed to read cached voters for {witness_name}: {e}",
                extra={"notification": False, "error": e},
            )

    shuffled_endpoints = get_hive_api_endpoints(shuffle_endpoints=True) or HIVE_API_ENDPOINTS[:]
    hive = None
    try:
        hive = Hive(node=default_hive_nodes())
    except Exception as e:
        logger.warning(
            f"{ICON} Unable to create Hive client for voter power calculation: {e}",
            extra={"notification": False, "error": e},
        )

    for api_url in shuffled_endpoints:
        voters: dict[str, dict] = {}
        page = 1
        url = "not set"
        try:
            while True:
                url = (
                    f"{api_url}hafbe-api/witnesses/{witness_name}/voters"
                    f"?page={page}&page-size={page_size}&sort=vests&direction=desc"
                )
                timeout = httpx.Timeout(5.0, connect=5.0)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await fetch_witness_details(client, url)
                    response.raise_for_status()
                    answer = response.json()
                mark_hive_api_endpoint_healthy(api_url)

                if isinstance(answer, dict):
                    page_voters = answer.get("voters", [])
                else:
                    page_voters = answer

                if not isinstance(page_voters, list):
                    logger.warning(
                        f"{ICON} Unexpected voter payload for {witness_name} from {url}",
                        extra={"notification": False},
                    )
                    return None

                enhanced_voters = [
                    _enhance_witness_voter_with_total_value(voter, hive) for voter in page_voters
                ]
                for voter in enhanced_voters:
                    key = _get_witness_voter_key(voter)
                    if key:
                        voters[key] = voter
                if len(page_voters) < page_size:
                    break
                page += 1

            try:
                InternalConfig.redis_decoded.set(
                    name=cache_key,
                    value=json.dumps(voters),
                    ex=1800,  # Cache for 30 minutes
                )
            except Exception as redis_error:
                logger.warning(
                    f"{ICON} Failed to cache voters for {witness_name}: {redis_error}",
                    extra={"notification": False, "error": redis_error},
                )

            return voters
        except httpx.HTTPStatusError as e:
            mark_hive_api_endpoint_failed(api_url, error=e)
            logger.warning(
                f"{ICON} API returned status {e.response.status_code} for {url}",
                extra={"notification": False, "error": e},
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            mark_hive_api_endpoint_failed(api_url, error=e)
            logger.error(
                f"{ICON} Connection failed to {url}: {e}",
                extra={"notification": False, "error": e},
            )
        except ValueError as e:
            mark_hive_api_endpoint_failed(api_url, error=e)
            logger.warning(
                f"{ICON} Failed to parse JSON response from {url}, trying again...",
                extra={"notification": False, "error": e},
            )
        except Exception as e:
            logger.exception(
                f"{ICON} Unexpected error fetching voters for {witness_name} from {url}: {e}",
                extra={"notification": False, "error": e},
            )

    try:
        cached_data = InternalConfig.redis_decoded.get(cache_key)
        if cached_data:
            cached_voters = json.loads(cached_data)
            return _build_witness_voter_map(cached_voters)
    except Exception as e:
        logger.warning(
            f"{ICON} Failed to read cached voters for {witness_name}: {e}",
            extra={"notification": False, "error": e},
        )

    logger.warning(
        f"{ICON} Failed to fetch voters for {witness_name} from all API endpoints",
        extra={"notification": False},
    )
    return None


async def check_witness_vote(hive_accname: str, witness_name: str) -> bool:
    """
    Checks if a Hive account has voted for a specific witness.

    Args:
        hive_accname (str): The account name of the Hive user.
        witness_name (str): The name of the witness to check.

    Returns:
        bool: True if the account has voted for the witness, False otherwise.
    """
    cache_key = f"witness:{witness_name}:witness_vote_{hive_accname}_votes_for_{witness_name}"
    cache_result = InternalConfig.redis_decoded.get(cache_key)
    if cache_result is not None and cache_result in ["True", "False"]:
        return cache_result == "True"

    shuffled_endpoints = get_hive_api_endpoints(shuffle_endpoints=True) or HIVE_API_ENDPOINTS[:]
    for api_url in shuffled_endpoints:
        url = f"{api_url}hafbe-api/accounts/{hive_accname}"
        try:
            timeout = httpx.Timeout(5.0, connect=5.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await fetch_witness_details(client, url)
                response.raise_for_status()  # Raises an exception for 4xx/5xx status codes
                answer = response.json()
                mark_hive_api_endpoint_healthy(api_url)
                witness_votes = answer.get("witness_votes", [])
                if witness_name in witness_votes:
                    InternalConfig.redis_decoded.set(name=cache_key, value="True", ex=600)
                    return True
                InternalConfig.redis_decoded.set(name=cache_key, value="False", ex=600)
                return False

        except httpx.HTTPStatusError as e:
            mark_hive_api_endpoint_failed(api_url, error=e)
            logger.warning(
                f"{ICON} API returned status {e.response.status_code} for {url}",
                extra={"notification": False, "error": e},
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            mark_hive_api_endpoint_failed(api_url, error=e)
            logger.error(
                f"{ICON} Connection failed to {url}: {e}",
                extra={"notification": False, "error": e},
            )
        except ValueError as e:
            mark_hive_api_endpoint_failed(api_url, error=e)
            logger.warning(
                f"{ICON} Failed to parse JSON response from {url}, trying again...",
                extra={"notification": False, "error": e},
            )
    logger.warning(
        f"{ICON} Failed to check witness vote for {hive_accname} from all API endpoints",
        extra={"notification": False},
    )
    return False
