import json
import logging
from os.path import exists
from typing import List, Set

import httpx
from aiocache import cached

from v4vapp_backend_v2.config.setup import InternalConfig, async_time_stats_decorator


@async_time_stats_decorator()
async def check_bad_hive_accounts(check_for: List[str]) -> bool:
    """
    Checks if any of the provided Hive account names are present in the bad accounts list.

    Args:
        check_for (List[str]): A list of Hive account names to check.

    Returns:
        bool: True if any account in the list is found in the bad accounts set, False otherwise.

    Raises:
        Any exceptions raised by `get_bad_hive_accounts()` will propagate.
    """
    bad_accounts_set = await get_bad_hive_accounts()
    return any(account in bad_accounts_set for account in check_for)


@async_time_stats_decorator()
async def check_not_development_accounts(accounts: List[str]) -> bool:
    """
    Checks if any accounts are not allowed in development mode.

    Args:
        accounts (List[str]): A list of Hive account names to check.

    Returns:
        bool: True if development mode is ON and any account is not in the allowed list, False otherwise.
    """
    if not InternalConfig().config.development.enabled:
        return False
    allowed_set = set(InternalConfig().config.development.allowed_hive_accounts)
    if any(account not in allowed_set for account in accounts):
        return True  # Raise error: dev ON and at least one account not allowed
    return False  # No error: dev OFF or all accounts allowed


@cached(ttl=300)
async def get_bad_hive_accounts() -> Set[str]:
    """
    Asynchronously retrieves a combined set of bad Hive account usernames.

    This function fetches a list of bad actors from an external source and merges it
    with a locally stored list of bad Hive accounts from 'data/bad_hive_accounts.json'.
    If the local file cannot be loaded, a warning is logged and only the fetched list is used.

    Returns:
        Set[str]: A set containing usernames of bad Hive accounts.
    """
    bad_actors = await fetch_bad_actor_list()
    bad_accounts: Set[str] = set()
    try:
        if exists("data/bad_hive_accounts.json"):
            with open("data/bad_hive_accounts.json", "r") as f:
                bad_accounts = set(json.load(f))
    except Exception as e:
        logging.warning(f"Error loading bad accounts: {e}")
        bad_accounts = set()
    combined = bad_actors | bad_accounts
    return combined


async def fetch_bad_actor_list() -> Set[str]:
    url = "https://gitlab.syncad.com/hive/wallet/-/raw/master/src/app/utils/BadActorList.js?ref_type=heads"
    try:
        # Fetch the content from the URL
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes

        # Extract the content between the backticks
        content = response.text
        start = content.find("`") + 1
        end = content.rfind("`")
        if start == 0 or end == -1:
            raise ValueError("Could not find list boundaries in the file")

        # Get the list portion and process it
        list_content = content[start:end].strip()
        # Split into lines and filter out empty lines
        bad_actor_list = {line.strip() for line in list_content.split("\n") if line.strip()}

        return bad_actor_list

    except httpx.HTTPError as e:
        print(f"Error fetching the list: {e}")
        return set()
    except ValueError as e:
        print(f"Error parsing the list: {e}")
        return set()
