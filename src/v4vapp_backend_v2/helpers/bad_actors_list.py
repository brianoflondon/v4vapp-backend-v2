import json
import tempfile
from pathlib import Path

import httpx
from aiocache import cached

from v4vapp_backend_v2.config.setup import InternalConfig, logger


def _load_local_bad_actors() -> set[str]:
    """Load the local bad actors list once at module import time."""
    try:
        local_bad = Path(__file__).parent / "bad_actors_local_list.txt"
        if local_bad.exists():
            with open(local_bad, "r") as f:
                return {
                    line.split("|", 1)[0].strip()
                    for line in f
                    if line.strip() and line.split("|", 1)[0].strip()
                }
    except Exception as e:
        logger.warning(f"Error loading local bad actors: {e}")
    return set()


# Cache the local list at module import time (only read once)
_LOCAL_BAD_ACTORS = _load_local_bad_actors()


# @async_time_stats_decorator()
async def check_bad_hive_accounts(check_for: list[str]) -> bool:
    """
    Checks if any of the provided Hive account names are present in the bad accounts list.

    Args:
        check_for (list[str]): A list of Hive account names to check.

    Returns:
        bool: True if any account in the list is found in the bad accounts set, False otherwise.

    Raises:
        Any exceptions raised by `get_bad_hive_accounts()` will propagate.
    """
    bad_accounts_set = await get_bad_hive_accounts()
    bad_accounts_found = {account for account in check_for if account in bad_accounts_set}
    if bad_accounts_found:
        logger.warning(
            f"⚠️ Bad accounts found: {bad_accounts_found}",
            extra={"bad_accounts": list(bad_accounts_found)},
        )
    return any(bad_accounts_found)


# @async_time_stats_decorator()
async def check_not_development_accounts(accounts: list[str]) -> bool:
    """
    Checks if any accounts are not allowed in development mode.

    Args:
        accounts (list[str]): A list of Hive account names to check.

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
async def get_bad_hive_accounts() -> set[str]:
    """
    Asynchronously retrieves a combined set of bad Hive account usernames.

    This function fetches a list of bad actors from an external source and merges it
    with a locally stored list of bad Hive accounts from 'bad_actors_local_list.txt'.
    The local list is loaded once at module import time.

    Returns:
        set[str]: A set containing usernames of bad Hive accounts.
    """
    bad_actors = await fetch_bad_actor_list()
    combined = bad_actors | _LOCAL_BAD_ACTORS
    return combined


async def fetch_bad_actor_list() -> set[str]:
    url = "https://gitlab.syncad.com/hive/wallet/-/raw/master/src/app/utils/BadActorList.js?ref_type=heads"

    TMP_DIR = Path(tempfile.gettempdir())
    TMP_FILE = TMP_DIR / "bad_actors_backup_list.txt"
    REDIS_KEY = "bad_actors:backup"
    REDIS_TTL = 3600

    try:
        # Fetch the content from the URL with a short timeout for debug runs
        async with httpx.AsyncClient(timeout=5.0) as client:
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
            InternalConfig.redis_decoded.set(
                name=REDIS_KEY, value=json.dumps(list(bad_actor_list)), ex=REDIS_TTL
            )
        except Exception as e:
            logger.warning(f"Failed to cache bad actors in Redis: {e}")

        return bad_actor_list

    except (httpx.HTTPError, ValueError) as fetch_exc:
        # On fetch/parsing errors, attempt fallbacks in order: Redis -> /tmp -> bundled file
        logger.debug(f"Error fetching/parsing the list: {fetch_exc.__class__.__name__}")

        # 1) Try Redis
        try:
            cached = InternalConfig.redis_decoded.get(REDIS_KEY)
            if cached:
                try:
                    payload = json.loads(cached)
                    if isinstance(payload, (list, tuple)):
                        logger.debug("Using bad actors list from Redis cache")
                        return set(payload)
                except Exception as e:
                    logger.debug(f"Failed to parse cached bad actors from Redis: {e}")
        except Exception as e:
            logger.debug(f"Redis unavailable when loading bad actors backup: {e}")

        # 2) Try /tmp file
        try:
            if TMP_FILE.exists():
                text = TMP_FILE.read_text(encoding="utf-8")
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                if lines:
                    logger.debug("Using bad actors list from /tmp backup")
                    return set(lines)
        except Exception as e:
            logger.debug(f"Failed to read tmp backup file {TMP_FILE}: {e}")

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
                        lines = [ln.strip() for ln in list_content.splitlines() if ln.strip()]
                        if lines:
                            logger.debug("Using bundled bad actors backup list")
                            return set(lines)
                # fallback: plain lines
                lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
                if lines:
                    logger.debug("Using bundled bad actors list (plain format)")
                    return set(lines)
        except Exception as e:
            logger.debug(f"Failed to read bundled fallback bad actors file: {e}")

        return set()
