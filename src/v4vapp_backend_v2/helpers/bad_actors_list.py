import json
import logging
import tempfile
from os.path import exists
from pathlib import Path
from typing import List, Set

import httpx
from aiocache import cached

from v4vapp_backend_v2.config.setup import InternalConfig, logger


# @async_time_stats_decorator()
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


# @async_time_stats_decorator()
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

    TMP_DIR = Path(tempfile.gettempdir())
    TMP_FILE = TMP_DIR / "bad_actors_backup_list.txt"
    REDIS_KEY = "bad_actors:backup"
    REDIS_TTL = 3600

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

        # Persist a local backup (atomic write using pathlib) and cache in Redis
        try:
            tmp_tmp = TMP_FILE.parent / (TMP_FILE.name + ".tmp")
            tmp_tmp.write_text("\n".join(sorted(bad_actor_list)), encoding="utf-8")
            tmp_tmp.replace(TMP_FILE)
        except Exception as e:
            logger.warning(f"Failed to write backup file {TMP_FILE}: {e}")

        try:
            # Store as JSON array in Redis with TTL
            InternalConfig.redis_decoded.setex(
                name=REDIS_KEY, time=REDIS_TTL, value=json.dumps(list(bad_actor_list))
            )
        except Exception as e:
            logger.warning(f"Failed to cache bad actors in Redis: {e}")

        return bad_actor_list

    except (httpx.HTTPError, ValueError) as fetch_exc:
        # On fetch/parsing errors, attempt fallbacks in order: Redis -> /tmp -> bundled file
        logger.warning(f"Error fetching/parsing the list: {fetch_exc}", exc_info=True)

        # 1) Try Redis
        try:
            cached = InternalConfig.redis_decoded.get(REDIS_KEY)
            if cached:
                try:
                    payload = json.loads(cached)
                    if isinstance(payload, (list, tuple)):
                        return set(payload)
                except Exception as e:
                    logger.warning(f"Failed to parse cached bad actors from Redis: {e}", exc_info=True)
        except Exception as e:
            logger.warning(f"Redis unavailable when loading bad actors backup: {e}", exc_info=True)

        # 2) Try /tmp file
        try:
            if TMP_FILE.exists():
                text = TMP_FILE.read_text(encoding="utf-8")
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                if lines:
                    return set(lines)
        except Exception as e:
            logger.warning(f"Failed to read tmp backup file {TMP_FILE}: {e}", exc_info=True)

        # 3) Final fallback: bundled file shipped with the repo (next to this module)
        try:
            bundled = Path(__file__).parent / "bad_actors_backup_list.txt"
            if bundled.exists():
                content = bundled.read_text(encoding="utf-8")
                if "`" in content:
                    start = content.find("`") + 1
                    end = content.rfind("`")
                    if start != 0 and end != -1:
                        list_content = content[start:end]
                        lines = {ln.strip() for ln in list_content.splitlines() if ln.strip()}
                        if lines:
                            return lines
                # fallback: plain lines
                lines = {ln.strip() for ln in content.splitlines() if ln.strip()}
                if lines:
                    return lines
        except Exception as e:
            logger.warning(f"Failed to read bundled fallback bad actors file: {e}", exc_info=True)

        return set()
