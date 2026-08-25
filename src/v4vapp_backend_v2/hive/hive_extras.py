import asyncio
import json
import struct
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import backoff
import httpx
from nectar.account import Account
from nectar.amount import Amount
from nectar.blockchain import Blockchain
from nectar.exceptions import MissingKeyError
from nectar.hive import Hive
from nectar.market import Market
from nectar.memo import Memo
from nectar.price import Price
from nectar.transactionbuilder import TransactionBuilder
from nectarapi.exceptions import RPCError, UnhandledRPCError
from nectarbase.operations import Custom_json as NectarCustomJson
from nectarbase.operations import Transfer as NectarTransfer
from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import HiveRoles, InternalConfig, logger
from v4vapp_backend_v2.helpers.bad_actors_list import (
    check_not_development_accounts,
    get_bad_hive_accounts,
)
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_to_float_or_int
from v4vapp_backend_v2.hive.hive_api_endpoints import (
    get_hive_api_endpoints,
    mark_hive_api_endpoint_failed,
    mark_hive_api_endpoint_healthy,
)
from v4vapp_backend_v2.hive_models.account_name_type import AccName, AccNameType
from v4vapp_backend_v2.hive_models.pending_transaction_class import (
    PendingCustomJson,
    PendingTransaction,
)
from v4vapp_backend_v2.process.lock_str_class import CustIDType
from v4vapp_backend_v2.process.process_errors import HiveToLightningError

DEFAULT_GOOD_NODES = [
    "https://api.hive.blog",
    "https://api.deathwing.me",
    "https://hive-api.arcange.eu",
    "https://api.openhive.network",
    "https://techcoderx.com",
    "https://api.c0ff33a.uk",
    "https://hiveapi.actifit.io",
    "https://api.syncad.com",
]

BLOCK_STREAM_ONLY = ["https://rpc.podping.org/"]

EXCLUDE_NODES = [
    "https://rpc.mahdiyari.info",
    # "https://api.hive.blog",
    "https://api.deathwing.me",
    # "https://hive-api.arcange.eu",
    "https://api.openhive.network",
    # "https://techcoderx.com",
    # "https://api.c0ff33a.uk",
    # "https://hiveapi.actifit.io",
    # "https://api.syncad.com",
    # "https://hive-api.dlux.io",
    "https://hive-api.3speak.tv",
]

MAX_HIVE_BATCH_SIZE = 25

HIVE_BLOCK_TIME = 3  # seconds

REDIS_KEY_GOOD_NODES = "good_nodes:"
HIVE_INTERNAL_MARKET_FAILURE_COOLDOWN_SECONDS = 120
HIVE_INTERNAL_MARKET_SUCCESS_CACHE_SECONDS = 45

_hive_internal_market_cooldown_until: datetime | None = None
_hive_internal_market_last_success: "HiveInternalQuote | None" = None
_hive_internal_market_last_success_at: datetime | None = None


class CustomJsonSendError(Exception):
    """
    Custom exception for errors related to sending custom JSON data.

    Args:
        message (str): Error message.
        extra (dict): Additional information about the error.
    """

    def __init__(self, message: str, extra: dict | None = None):
        super().__init__(message)
        self.extra = extra if extra else {}


class HiveTransferError(Exception):
    """
    Custom exception for errors related to Hive transfers.

    Args:
        message (str): Error message.
        extra (dict): Additional information about the error.
    """

    def __init__(self, message: str, extra: dict | None = None):
        super().__init__(message)
        self.extra = extra if extra else {}


class HiveNotHiveAccount(HiveTransferError):
    """
    Exception raised when a provided account name is not a valid Hive account.
    so a transfer to it can't be made for notifications
    """


class HiveNotEnoughHiveInAccount(HiveTransferError):
    """
    Exception raised when there are not enough Hive funds in the account.
    """

    def __init__(self, message: str, sending_amount: Amount):
        super().__init__(message)
        self.sending_amount = sending_amount


class HiveNotEnoughHiveRCMana(HiveTransferError):
    """
    Exception raised when there is not enough Hive RC mana to perform the transfer.
    """

    def __init__(self, message: str, sending_amount: Amount):
        super().__init__(message)
        self.sending_amount = sending_amount


class HiveTryingToSendZeroOrNegativeAmount(HiveTransferError):
    """
    Exception raised when trying to send a zero or negative amount.
    """


class HiveMissingKeyError(HiveTransferError):
    """
    Exception raised when a required key is missing.
    """


class HiveSomeOtherRPCException(HiveTransferError):
    """
    Exception raised for other unhandled RPC errors.
    This is a catch-all for any other exceptions that do not fit the specific cases.
    """


class HiveToKeepsatsConversionError(HiveTransferError):
    """Custom exception for Hive to Keepsats conversion errors."""


class HiveConversionLimits(HiveTransferError):
    """Custom exception for conversion limit errors."""


class HiveAccountNameOnExchangesList(HiveTransferError):
    """Custom exception for when a Hive account name is found on exchanges list."""


class HiveDevelopmentAccountError(HiveTransferError):
    """Custom exception for development-related errors."""


def _filter_excluded_nodes(nodes: list[str]) -> list[str]:
    """Drop nodes in EXCLUDE_NODES (exact match). Always apply before Hive().

    Callers that pass ``node=`` can reintroduce excluded endpoints; filter always.
    """
    if not nodes:
        return []
    excluded = set(EXCLUDE_NODES)
    return [n for n in nodes if n not in excluded]


def default_hive_nodes(stream_only: bool = False) -> list[str]:
    """
    Static default RPC node list for Nectar ``Hive(node=...)``.

    Health probes, retries, and failover after construction are Nectar's job.
    Beacon/Redis good-node lists are not used for client construction.
    """
    my_good_nodes = get_good_nodes()
    nodes = _filter_excluded_nodes(my_good_nodes or list(DEFAULT_GOOD_NODES))
    if stream_only:
        nodes = nodes + [n for n in BLOCK_STREAM_ONLY if n not in nodes]
    if not nodes:
        # Last resort if EXCLUDE_NODES wiped the list
        nodes = list(DEFAULT_GOOD_NODES)
    return nodes


# Nectar defaults are timeout=60s and num_retries=100 — a single dead-pool call can
# block the caller for tens of minutes. Streaming/monitor paths must fail fast so
# our outer recovery loop can rotate nodes and keep the event loop healthy.
STREAM_HIVE_TIMEOUT = 12
STREAM_HIVE_NUM_RETRIES = 5
STREAM_HIVE_NUM_RETRIES_CALL = 2
# hive-nectar 1.0.7+ defaults monitor_interval=0; pass explicitly for older builds
# and so stream clients never opt into background NodePoolMonitor probes.
STREAM_HIVE_MONITOR_INTERVAL = 0.0


def stream_hive_kwargs(stream_only: bool = False) -> dict[str, Any]:
    """Keyword args for a fail-fast Nectar ``Hive`` used by long-running streams."""
    return {
        "node": default_hive_nodes(stream_only=stream_only),
        "timeout": STREAM_HIVE_TIMEOUT,
        "num_retries": STREAM_HIVE_NUM_RETRIES,
        "num_retries_call": STREAM_HIVE_NUM_RETRIES_CALL,
        "monitor_interval": STREAM_HIVE_MONITOR_INTERVAL,
    }


def make_stream_hive(keys: Any = None, stream_only: bool = False) -> Hive:
    """
    Build a Hive client for block streaming / hive_monitor.

    Uses short per-request timeouts and few retries so RPC failures surface to
    stream_ops quickly instead of freezing the process on Nectar's defaults.

    Background ``NodePoolManager`` health monitors are disabled
    (``monitor_interval=0``). Streaming owns failover via rebuild; abandoned
    monitor threads + probe sockets contributed to EMFILE under recovery pressure.
    """
    kwargs = stream_hive_kwargs(stream_only=stream_only)
    if keys:
        kwargs["keys"] = keys

    hive = Hive(**kwargs)
    # Belt-and-suspenders for nectar builds that still default monitor_interval=30.
    _disarm_node_pool_monitor(hive)
    return hive


def _best_effort(action: Callable[[], Any], *args: Any, **kwargs: Any) -> None:
    """Run teardown/cleanup that must never raise (BLE001/S110 by design)."""
    try:
        action(*args, **kwargs)
    except Exception:
        return


def _stop_pool_manager(pool_mgr: Any) -> None:
    """Stop NodePoolMonitor and join if possible."""
    if pool_mgr is None:
        return
    monitor = getattr(pool_mgr, "_monitor_thread", None)
    if hasattr(pool_mgr, "stop_monitoring"):
        _best_effort(pool_mgr.stop_monitoring)
    else:
        _best_effort(pool_mgr.close)
    if monitor is not None and getattr(monitor, "is_alive", lambda: False)():
        _best_effort(monitor.join, 0.5)
    try:
        pool_mgr.monitor_interval = 0.0
    except Exception:
        return


def _disarm_node_pool_monitor(hive: Any) -> None:
    """
    Best-effort stop of NodePoolManager's probe loop if one is already running.

    Transport-level mark_failed / get_active_node still work; only background
    re-probes (and their per-cycle thread fan-out) are disabled.
    """
    try:
        rpc = getattr(hive, "rpc", None)
        if rpc is None:
            return
        nodes = getattr(rpc, "nodes", None)
        pool_mgr = getattr(nodes, "pool_manager", None) if nodes is not None else None
        if pool_mgr is None:
            return
        # Already disabled — nothing to do.
        if float(getattr(pool_mgr, "monitor_interval", 0) or 0) <= 0 and not getattr(
            pool_mgr, "_monitor_thread", None
        ):
            return
        _stop_pool_manager(pool_mgr)
    except Exception:
        return


def close_hive_client(hive: Any = None) -> None:
    """
    Release Nectar ``Hive`` resources so rebuild/rotate paths do not leak FDs.

    Prefers ``Hive.close()`` / ``BlockChainInstance.close()`` (nectar 1.0.6+), which
    closes the per-instance httpx clients **and** stops ``NodePoolManager``.
    Falls back to ``GrapheneRPC.close()`` (hard-close + pool stop on 1.0.7+) and
    manual session teardown for older builds.

    Safe to call with None or non-Hive objects. Does not close Nectar's process-wide
    shared httpx client.
    """
    if hive is None:
        return

    # Mark closed so callers/zombies can detect intentional teardown.
    try:
        hive._v4v_hive_closed = True
    except Exception:  # noqa: S110
        pass

    # Capture rpc before Hive.close() nulls it.
    rpc = getattr(hive, "rpc", None)

    # 1) Full instance close when available (rpc + instance client/async_client + pool).
    hive_close = getattr(hive, "close", None)
    if callable(hive_close) and not isinstance(hive, type):
        try:
            hive_close()
        except Exception:  # noqa: S110
            pass
        else:
            return

    if rpc is None:
        return

    # 2) GrapheneRPC.close — on 1.0.7+ hard-closes and stops the pool manager.
    if hasattr(rpc, "close") and callable(rpc.close):
        try:
            # Pre-stop monitor for older nectar where rpc.close left it running.
            nodes = getattr(rpc, "nodes", None)
            pool_mgr = getattr(nodes, "pool_manager", None) if nodes is not None else None
            if pool_mgr is None and hasattr(rpc, "__dict__"):
                pool_mgr = rpc.__dict__.get("_failover_pool_manager")
            _stop_pool_manager(pool_mgr)
            rpc.close()
        except Exception:  # noqa: S110
            pass
        else:
            return

    # 3) Fallback for older nectar without GrapheneRPC.close().
    try:
        shared = None
        try:
            from nectarapi import graphenerpc as _grpc

            shared = getattr(_grpc, "_shared_httpx_client", None)
        except Exception:
            shared = None

        session = None
        if hasattr(rpc, "__dict__"):
            session = rpc.__dict__.get("_failover_session")
        if session is None:
            session = getattr(rpc, "session", None)

        if session is not None and session is not shared:
            _best_effort(session.close)

        if hasattr(rpc, "__dict__"):
            rpc.__dict__["_failover_session"] = None
            rpc.__dict__.pop("_failover_pool_manager", None)
        try:
            rpc.session = None
        except Exception:
            return
    except Exception:
        return


def get_hive_client(stream_only: bool = False, nobroadcast: bool = False, *args, **kwargs) -> Hive:
    """
    Thin factory around Nectar ``Hive``.

    Prefer ``Hive(keys=..., nobroadcast=..., node=default_hive_nodes())`` at call
    sites. This helper remains for tests/scripts and only:

    - defaults ``nobroadcast``
    - supplies a static ``node`` list when omitted (bare ``Hive()`` can hang on
      empty Nectar config defaults)
    - filters ``EXCLUDE_NODES`` when ``node`` is provided

    Node rotation/failover is entirely Nectar's node pool after construction.
    """
    kwargs.setdefault("nobroadcast", nobroadcast)

    if "node" not in kwargs or not kwargs["node"]:
        kwargs["node"] = default_hive_nodes(stream_only=stream_only)
    else:
        node_arg = kwargs["node"]
        if isinstance(node_arg, str):
            node_arg = [node_arg]
        filtered = _filter_excluded_nodes(list(node_arg))
        kwargs["node"] = filtered or default_hive_nodes(stream_only=stream_only)

    try:
        return Hive(*args, **kwargs)
    except ValueError as e:
        # Nectar raises ValueError for bad keys / wallet issues
        logger.warning(f"Bad keys passed to Hive client: {e}", extra={"notification": True})
        raise HiveMissingKeyError(
            f"Bad keys passed to Hive client: {e}", extra={"notification": True}
        ) from e


def get_blockchain_instance(*args, **kwargs) -> Blockchain:
    """
    Create a Blockchain instance bound to a Nectar Hive client.

    Accepts either ``hive_instance=`` or ``hive=`` (or constructs ``Hive``).
    Must pass ``blockchain_instance=`` to Nectar's ``Blockchain`` — plain
    ``hive_instance`` is ignored by Nectar.
    """
    hive = kwargs.pop("hive_instance", None) or kwargs.pop("hive", None)
    if hive is None:
        hive_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in {"mode", "max_block_wait_repetition", "data_refresh_time_seconds"}
        }
        if "nobroadcast" not in hive_kwargs:
            hive_kwargs["nobroadcast"] = False
        if "node" not in hive_kwargs or not hive_kwargs["node"]:
            hive_kwargs["node"] = default_hive_nodes()
        hive = Hive(*args, **hive_kwargs)
    mode = kwargs.pop("mode", "head")
    max_block_wait_repetition = kwargs.pop("max_block_wait_repetition", None)
    return Blockchain(
        blockchain_instance=hive,
        mode=mode,
        max_block_wait_repetition=max_block_wait_repetition,
    )


def get_good_nodes() -> list[str]:
    """
    Fetches a list of default nodes from the specified API endpoint.

    This function sends a GET request to "https://beacon.peakd.com/api/nodes"
    but uses my proxy endpoint "https://devapi.v4v.app/v2/beacon/nodes/"
    and retrieves a list of nodes. It then filters the nodes to include only
    those with a score of 100 and returns their endpoints.

    Returns:
        List[str]: A list of endpoints for nodes with a score of 100.
    """
    cached_nodes = InternalConfig.redis_decoded.get(REDIS_KEY_GOOD_NODES)
    if cached_nodes:
        return _filter_excluded_nodes(json.loads(cached_nodes))

    beacon_urls = [
        "https://devapi.v4v.app/v2/beacon/nodes/",
        "https://api.v4v.app/v2/beacon/nodes/",
        "https://beacon.peakd.com/api/nodes",
    ]

    good_nodes: list[str] = []

    while not good_nodes and beacon_urls:
        url = "unset"
        try:
            url = beacon_urls.pop(0)
            params = {"source": "v4vapp_backend"}
            response = httpx.get(url, params=params, timeout=5, follow_redirects=True)
            response.raise_for_status()
            nodes = response.json()
            logger.info(
                "Fetched good nodes Last good nodes",
                extra={"beacon_response": nodes, "error_code_clear": "beacon_nodes_fail"},
            )
            good_nodes = [node["endpoint"] for node in nodes if node["score"] >= 80]
            good_nodes = _filter_excluded_nodes(good_nodes)
            logger.info(f"Good nodes {good_nodes}", extra={"good_nodes": good_nodes})
            try:
                InternalConfig.redis_decoded.set(
                    name=REDIS_KEY_GOOD_NODES,
                    value=json.dumps(good_nodes),
                    ex=3600,
                )
            except Exception as e:
                logger.warning(
                    f"Failed to set good nodes in Redis: {e}", extra={"notification": False}
                )
            return good_nodes
        except Exception as e:
            logger.warning(
                f"Failed to fetch good nodes from {url}: {e}",
                extra={"notification": False},
            )

    good_nodes_json = InternalConfig.redis_decoded.get(REDIS_KEY_GOOD_NODES)
    if good_nodes_json and isinstance(good_nodes_json, str):
        good_nodes = _filter_excluded_nodes(json.loads(good_nodes_json))
    if good_nodes:
        logger.warning(
            "Failed to fetch good nodes: using last good nodes.",
            extra={
                "notification": False,
                "error_code": "beacon_nodes_fail",
            },
        )
    else:
        logger.warning(
            "Failed to fetch good nodes: using default nodes.",
            extra={
                "notification": False,
                "error_code": "beacon_nodes_fail",
            },
        )
        good_nodes = _filter_excluded_nodes(list(DEFAULT_GOOD_NODES))
        InternalConfig.redis_decoded.set(
            name=REDIS_KEY_GOOD_NODES,
            value=json.dumps(good_nodes),
            ex=3600,
        )

    if len(good_nodes) < 2:
        logger.warning(
            f"Too few good nodes found ({len(good_nodes)}), using default nodes.",
            extra={"good_nodes": good_nodes},
        )
        good_nodes = _filter_excluded_nodes(list(DEFAULT_GOOD_NODES))
        InternalConfig.redis_decoded.set(
            name=REDIS_KEY_GOOD_NODES,
            value=json.dumps(good_nodes),
            ex=1800,
        )
    return good_nodes


async def get_verified_hive_client(
    hive_role: HiveRoles = HiveRoles.server,
    nobroadcast: bool = False,
) -> tuple[Hive, str]:
    """
    Asynchronously obtains a verified Hive client instance using server account credentials from the internal configuration.

    Args:
        nobroadcast (bool, optional): If True, disables broadcasting of transactions. Defaults to False.
        hive_role (HiveRoles, optional): The role to use for the Hive client. Defaults to HiveRoles.server.

    Returns:
        Tuple[Hive, str]: A tuple containing the initialized Hive client and the server account name.

    Raises:
        HiveToLightningError: If the server account configuration or required keys are missing.
    """
    return get_verified_hive_client_non_async(hive_role=hive_role, nobroadcast=nobroadcast)


def get_verified_hive_client_non_async(
    hive_role: HiveRoles = HiveRoles.server,
    nobroadcast: bool = False,
) -> tuple[Hive, str]:
    """
    Synchronously obtains a verified Hive client instance using server account credentials from the internal configuration.

    Args:
        nobroadcast (bool, optional): If True, disables broadcasting of transactions. Defaults to False.
        hive_role (HiveRoles, optional): The role to use for the Hive client. Defaults to HiveRoles.server.

    Returns:
        Tuple[Hive, str]: A tuple containing the initialized Hive client and the server account name.

    Raises:
        HiveToLightningError: If the server account configuration or required keys are missing.
    """
    hive_config = InternalConfig().config.hive_config
    hive_account = hive_config.get_hive_role_account(hive_role)
    if not hive_account:
        raise HiveToLightningError("Missing Hive server account configuration for repayment")

    memo_key = hive_account.memo_key or ""
    active_key = hive_account.active_key or ""
    posting_key = hive_account.posting_key or ""

    keys = [key for key in [memo_key, active_key, posting_key] if key]

    if not keys:
        raise HiveToLightningError("Missing Hive server account keys for repayment")

    hive_client = Hive(
        keys=keys,
        nobroadcast=nobroadcast,
        node=default_hive_nodes(),
    )
    return hive_client, hive_account.name


async def get_verified_hive_client_for_accounts(
    accounts: list[str],
    nobroadcast: bool = False,
) -> Hive:
    """
    Asynchronously obtains a verified Hive client instance for a list of accounts using server account credentials from the internal configuration.
    This function checks the provided accounts against the internal Hive configuration and initializes a Hive client with the necessary keys.
    If no keys are found for the provided accounts, it defaults to using the server account's memo and active keys.

    Args:
        accounts (List[str]): A list of Hive account names to verify.
        nobroadcast (bool, optional): If True, disables broadcasting of transactions. Defaults to False.

    Returns:
        Hive: An initialized Hive client instance.

    Raises:
        HiveToLightningError: If the server account configuration or required keys are missing.
    """
    hive_config = InternalConfig().config.hive_config
    hive_accounts = []
    keys = []
    accounts = list(
        set(accounts + [InternalConfig().server_id])
    )  # Ensure server account is included and remove duplicates
    for account in accounts:
        if hive_config.hive_accs.get(account):
            hive_account = hive_config.hive_accs[account]
            hive_accounts.append(hive_account)
            all_keys = hive_account.keys
            if all_keys:
                keys.extend(all_keys)
    if not keys and hive_config.server_account:
        keys = [
            hive_config.server_account.memo_key,
            hive_config.server_account.active_key,
            hive_config.server_account.posting_key,
        ]
    if keys == ["", "", ""]:
        hive_client = Hive(nobroadcast=nobroadcast, node=default_hive_nodes())
    else:
        hive_client = Hive(
            keys=keys,
            nobroadcast=nobroadcast,
            node=default_hive_nodes(),
        )
    return hive_client


def get_transfer_cust_id(
    from_acc: AccNameType,
    to_acc: AccNameType,
    hive_config=None,
    expense_accounts: list[str] | None = None,
    exchange_accounts: list[str] | None = None,
) -> CustIDType:
    """
    Compute the customer id (cust_id) for a transfer a module-level helper.

    Parameters:
        from_acc: sender account name
        to_acc: recipient account name
        hive_config: optional object with attribute `all_account_names` (server, treasury, funding, exchange)
        expense_accounts: optional list of expense account names (defaults to ["privex"]).
        exchange_accounts: optional list of exchange account names (defaults to ["exchange"]).

    Returns:
        CustIDType: computed customer id following the same rules as the original method.
    """
    if hive_config is None:
        hive_config = InternalConfig().config.hive_config
    account_names = hive_config.all_account_names

    # Defensive check - ensure we have exactly 4 account names
    if not account_names or len(account_names) != 4:
        return f"{to_acc}->{from_acc}"

    server_account, treasury_account, funding_account, _exchange_account = account_names

    expense_accounts = (
        expense_accounts or InternalConfig().config.expense_config.hive_expense_accounts or []
    )
    exchange_accounts = (
        exchange_accounts or InternalConfig().config.hive_config.exchange_account_names or []
    )

    # Server to Treasury: cust_id = to_account (treasury)
    if from_acc == server_account and to_acc == treasury_account:
        return to_acc

    # Treasury to Server: cust_id = from_account (treasury)
    elif (
        from_acc == treasury_account
        and to_acc == server_account
        or from_acc == funding_account
        and to_acc == treasury_account
    ):
        return from_acc

    # Treasury to Funding: cust_id = to_account (funding)
    elif (
        from_acc == treasury_account
        and to_acc == funding_account
        or (from_acc == treasury_account or from_acc == server_account)
        and to_acc in exchange_accounts
    ):
        return to_acc

    # Exchange to Treasury: cust_id = from_account (exchange)
    elif from_acc in exchange_accounts and to_acc == treasury_account:
        return from_acc

    # Server to expense: cust_id = to_account (expense)
    elif from_acc == server_account and to_acc in expense_accounts or from_acc == server_account:
        return to_acc

    # Customer to server (deposit): cust_id = from_account (customer)
    elif to_acc == server_account:
        return from_acc

    else:
        return f"{to_acc}:{from_acc}"


class HiveInternalQuote(BaseModel):
    hive_hbd: float | None = None
    raw_response: dict[str, Any] = {}
    error: str = ""


async def call_hive_internal_market() -> HiveInternalQuote:
    """
    Asynchronously calls the Hive internal market API to retrieve the highest bid and
    lowest ask prices.

        Dict[str, float]: A dictionary containing the calculated Hive to HBD price and
        the market ticker data.
        Dict[str, float]: A dictionary containing the calculated Hive to HBD price and
        the market ticker data.
        If an error occurs, returns a dictionary with an error message.

    Raises:
        Exception: If there is an issue calling the Hive Market API.
        The function logs the last node used by the Hive blockchain instance and any
        errors encountered.
    Note:
        The function logs the last node used by the Hive blockchain instance and any
        errors encountered.
    """
    global _hive_internal_market_cooldown_until
    global _hive_internal_market_last_success
    global _hive_internal_market_last_success_at

    now = datetime.now(tz=UTC)
    if (
        _hive_internal_market_last_success
        and _hive_internal_market_last_success_at
        and now - _hive_internal_market_last_success_at
        < timedelta(seconds=HIVE_INTERNAL_MARKET_SUCCESS_CACHE_SECONDS)
    ):
        return _hive_internal_market_last_success

    if _hive_internal_market_cooldown_until and now < _hive_internal_market_cooldown_until:
        return HiveInternalQuote(error="HiveInternalMarket cooldown active")

    hive = Hive(node=default_hive_nodes())
    market = Market("HBD:HIVE", hive=hive)
    try:
        ticker = market.ticker()
        # raise KeyError("'highest_bid'")
        highest_bid: Price = ticker["highest_bid"]
        highest_bid_value = float(highest_bid["price"])
        lowest_ask: Price = ticker["lowest_ask"]
        lowest_ask_value = float(lowest_ask["price"])
        hive_hbd = float(((lowest_ask_value - highest_bid_value) / 2) + highest_bid_value)
        answer = HiveInternalQuote(hive_hbd=hive_hbd, raw_response=ticker)
        _hive_internal_market_last_success = answer
        _hive_internal_market_last_success_at = now
        _hive_internal_market_cooldown_until = None
        return answer
    except Exception as ex:
        # logging.exception(ex)
        logger.info(
            f"Calling Market API on Hive: {market['blockchain_instance'].data['last_node']}"
        )
        _hive_internal_market_cooldown_until = datetime.now(tz=UTC) + timedelta(
            seconds=HIVE_INTERNAL_MARKET_FAILURE_COOLDOWN_SECONDS
        )
        message = f"Problem calling Hive Market API {ex}"
        logger.error(message)
        return HiveInternalQuote(error=message)


# @async_time_decorator
async def account_hive_balances_async(hive_accname: str = "") -> dict[str, Amount | str]:
    """
    Asynchronously retrieves the current HIVE and HBD balances for the given account.
    """
    if not hive_accname:
        hive_accname = InternalConfig().server_id

    candidate_endpoints = get_hive_api_endpoints(shuffle_endpoints=True)
    for endpoint in candidate_endpoints:
        url = endpoint + f"balance-api/accounts/{hive_accname}/balances"
        try:
            timeout = httpx.Timeout(1.0, connect=1.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict):
                hive_balance = data.get("hive_balance")
                hbd_balance = data.get("hbd_balance")
                if hive_balance is not None and hbd_balance is not None:
                    mark_hive_api_endpoint_healthy(endpoint)
                    balances = [
                        Amount(f"{Decimal(hive_balance) / Decimal(1000):.3f} HIVE"),
                        Amount(f"{Decimal(hbd_balance) / Decimal(1000):.3f} HBD"),
                    ]
                    return {
                        "HIVE": balances[0],
                        "HBD": balances[1],
                        "HIVE_fmt": f"{balances[0].amount:,.3f}",
                        "HBD_fmt": f"{balances[1].amount:,.3f}",
                    }
            mark_hive_api_endpoint_failed(endpoint, error="invalid balance payload")
        except Exception as e:
            mark_hive_api_endpoint_failed(endpoint, error=e)
            logger.warning(
                f"Balance API unavailable for {endpoint}, trying next endpoint: {e}",
                extra={
                    "hive_accname": hive_accname,
                    "notification": False,
                    "hive_api_endpoints": candidate_endpoints,
                },
            )

    balance = await asyncio.to_thread(account_hive_balances, hive_accname)
    return balance


# @time_decorator
def account_hive_balances(hive_accname: str = "") -> dict[str, Amount | str]:
    """
    Retrieves the current HIVE and HBD balances for the given account.
    Returns

    Returns:
        Dict[str, float]: A dictionary containing the HIVE and HBD balances.
    """
    hive: Hive | None = None
    balances: list[Amount] | None = None
    if not hive_accname:
        hive_accname = InternalConfig().server_id
    try:
        hive = Hive(node=default_hive_nodes())
        hive_account = Account(hive_accname, blockchain_instance=hive)
        available = hive_account.balances.get("available", None)
        balances = list(available) if available is not None else None
    except Exception as e:
        rpc = getattr(hive, "rpc", None) if hive is not None else None
        url = str(getattr(rpc, "url", "unknown")) if rpc is not None else "unknown"
        logger.error(f"Error In Hive {url}: {e}", extra={"hive_accname": hive_accname})
    try:
        if not balances or len(balances) < 2:
            return {
                "HIVE": Amount("0.000 HIVE"),
                "HBD": Amount("0.000 HBD"),
                "HIVE_fmt": "0.000",
                "HBD_fmt": "0.000",
            }
        return {
            "HIVE": balances[0],
            "HBD": balances[1],
            "HIVE_fmt": f"{balances[0].amount:,.3f}",
            "HBD_fmt": f"{balances[1].amount:,.3f}",
        }
    except Exception as e:
        logger.error(f"Error fetching server hive balances: {e}")
        raise HiveSomeOtherRPCException(f"Error fetching server hive balances: {e}") from e


def get_event_id(hive_event: Any) -> str:
    """
    Get the event id from the Hive event.

    Args:
        hive_event (dict): The Hive event.

    Returns:
        str: The event id.
    """
    if not hive_event:
        return ""
    if not isinstance(hive_event, dict):
        return ""
    if not hive_event.get("trx_id"):
        return ""
    trx_id = hive_event.get("trx_id", "")
    op_in_trx = hive_event.get("op_in_trx", 0)
    return f"{trx_id}_{op_in_trx}" if int(op_in_trx) != 0 else str(trx_id)


def decode_memo(
    memo: str = "",
    hive_inst: Hive | None = None,
    memo_keys: list[str] | None = None,
    trx_id: str = "",
    op_in_trx: int = 0,
) -> str:
    """
    Decode an encrypted memo.

    Args:
        memo (str): The encrypted memo to decode.
        memo_keys (List[str]): A list of memo keys.
        hive_inst (Hive): A Hive instance.

    Returns:
        str: The decrypted memo.
    """
    if memo_keys is None:
        memo_keys = []

    if not memo and not trx_id:
        return ""

    if not memo_keys and not hive_inst:
        raise ValueError("No memo keys or Hive instance provided.")

    if memo_keys and not hive_inst:
        hive_inst = Hive(keys=memo_keys, node=default_hive_nodes())
        blockchain = get_blockchain_instance(hive_instance=hive_inst)

    if not hive_inst:
        raise ValueError("No Hive instance provided.")

    if trx_id and not memo:
        blockchain = get_blockchain_instance(hive_instance=hive_inst)
        trx = blockchain.get_transaction(trx_id)
        operations = trx.get("operations") if isinstance(trx, dict) else None
        if not operations:
            return ""
        op_entry = operations[op_in_trx]
        if isinstance(op_entry, dict):
            memo = str(op_entry.get("value", {}).get("memo") or "")
        else:
            memo = ""

    if not memo or memo[0] != "#":
        return memo

    try:
        m = Memo(from_account=None, to_account=None, blockchain_instance=hive_inst)
        d_memo = m.decrypt(memo)
        if d_memo == memo:
            return memo
        if d_memo:
            return d_memo[1:]
        return ""
    except struct.error:
        # arises when an unencrypted memo is decrypted..
        return memo
    except ValueError as e:
        # Memo is not encrypted
        logger.info(f"Memo is not encrypted: {e}")
        return memo
    except MissingKeyError as e:
        logger.debug(f"MissingKeyError: {e}")
        return memo

    except Exception as e:
        logger.error(f"Problem in decode_memo: {e}", extra={"trx_id": trx_id, "memo": memo})
        logger.error(memo)
        logger.exception(e)
        return memo


async def send_custom_json(
    json_data: dict[str, Any],
    send_account: str,
    hive_client: Hive | None = None,
    keys: list[str] | None = None,
    id: str = "v4vapp_transfer",
    nobroadcast: bool = False,
    active: bool = True,
    resend_attempt: int = 0,
) -> dict[str, str]:
    """
    Asynchronously sends a custom JSON operation to the Hive blockchain.

    This function allows sending a custom JSON operation with specified parameters
    to the Hive blockchain. It supports both active and posting authority, and can
    be configured to either broadcast the transaction or not.

    Args:
        json_data (dict): The JSON data to be sent. Must be a non-empty dictionary.
        send_account (str): The Hive account that will send the custom JSON operation.
        hive_client (Hive | None, optional): An instance of the Hive client. If not provided,
            a new client will be created using the provided keys. Defaults to None.
        keys (List[str], optional): A list of private keys to initialize the Hive client
            if `hive_client` is not provided. Defaults to an empty list.
        id (str, optional): The custom JSON operation ID. Defaults to "v4vapp_transfer".
        nobroadcast (bool, optional): If True, the transaction will not be broadcasted.
            Defaults to False.
        active (bool, optional): If True, the operation will require active authority.
            If False, it will require posting authority. Defaults to True.

    Returns:
        Dict[str, str]: The transaction response from the Hive blockchain.

    Raises:
        TypeError: If `json_data` is not a dictionary.
        ValueError: If `json_data` is empty, or if neither `hive_client` nor `keys`
            are provided.
        CustomJsonSendError: If an error occurs while sending the custom JSON operation.
    """
    # Need Required_auths not posting auths for a transfer
    # test json data is a dict which will become a nice json object:
    if keys is None:
        keys = []

    # Validate shape before persisting a PendingCustomJson row.
    if not isinstance(json_data, dict):
        raise TypeError("json_data must be a dictionary")
    json_data_converted: dict[str, Any] = convert_decimals_to_float_or_int(json_data)
    if not isinstance(json_data_converted, dict):
        raise TypeError("json_data must be a dictionary")
    if not json_data_converted:
        raise ValueError("json_data must not be empty")
    if not hive_client and not keys:
        raise ValueError("No hive_client or keys provided")

    pending = None
    if not resend_attempt:
        pending = PendingCustomJson(
            cj_id=id,
            send_account=send_account,
            json_data=json_data_converted,
            active=active,
            unique_key=f"{send_account}_{id}_{uuid4()}",
            nobroadcast=nobroadcast,
        )
        await pending.save()
    if not hive_client:
        hive_client = Hive(keys=keys, node=default_hive_nodes())
    if hive_client.nobroadcast and hive_client.nobroadcast != nobroadcast:
        raise ValueError("nobroadcast is not set to the same value as hive_client")
    try:
        if active:
            kwargs = {"required_auths": [send_account]}
        else:
            kwargs = {"required_posting_auths": [send_account]}

        trx = hive_client.custom_json(
            id=id, json_data=json_data_converted, **kwargs, nobroadcast=nobroadcast
        )
        if not resend_attempt and pending is not None:
            await pending.delete()
        return trx
    except UnhandledRPCError as ex:
        logger.warning(
            f"Error sending custom_json: {ex}",
            extra={"notification": False, "send_account": send_account},
        )
        raise CustomJsonSendError(
            f"Error sending custom_json: {ex}",
            extra={
                "json_data": json_data_converted,
                "send_account": send_account,
                "nobroadcast": nobroadcast,
            },
        )
    except MissingKeyError as ex:
        logger.warning(
            f"Error sending custom_json: MissingKeyError: {ex}",
            extra={"notification": False, "send_account": send_account},
        )
        raise CustomJsonSendError("Wrong key used", extra={"send_account": send_account}) from ex
    except Exception as ex:
        logger.exception(ex, extra={"notification": False})
        logger.error(f"{send_account} {ex} {ex.__class__}", extra={"notification": False})
        raise CustomJsonSendError(f"Error sending custom_json: {ex}") from ex


async def perform_transfer_checks(
    from_account: AccName | str,
    to_account: AccName | str,
    amount: Amount | None = None,
    nobroadcast: bool = False,
) -> bool:
    """
    Perform full validations, raise errors if a failure

    Args:
        from_account (str): The account name to perform transfer checks on.
        to_account (str): The account name to perform transfer checks on.
        amount (Amount, optional): The amount of the transfer. Defaults to Amount(0).
        nobroadcast (bool, optional): Flag indicating whether to broadcast the transfer. Defaults to False.

    Returns:
        bool: True if all validations pass, False otherwise.

    Raises:
        HiveAccountNameOnExchangesList: If the account name is on the bad accounts list.
        HiveNotEnoughHiveInAccount: If there is not enough balance in the account to
        perform the transfer.

    """
    from_account = AccName(from_account)
    to_account = AccName(to_account)
    if not amount:
        amount = Amount(amount="0.000 HIVE")
    if await check_not_development_accounts([from_account.no_prefix, to_account.no_prefix]):
        raise HiveDevelopmentAccountError(
            f"{from_account} or {to_account} is not in allowed hive accounts for development mode"
        )
    bad_accounts_set = await get_bad_hive_accounts()
    message = ""
    # Make sure from_account and to_account are AccName instances

    for account in [from_account, to_account]:
        if account.no_prefix in bad_accounts_set:
            message += f"{account} is on the bad accounts list "
    if message:
        raise HiveAccountNameOnExchangesList(message)
    return True


async def send_transfer_bulk(
    transfer_list: list[PendingTransaction] | None = None,
    custom_json_list: list[PendingCustomJson] | None = None,
    hive_client: Hive | None = None,
    keys: list[str] | None = None,
    nobroadcast: bool = False,
    is_private: bool = False,
) -> dict[str, Any]:
    """
    Send multiple Hive token transfers in bulk.

    Args:
        transfer_list (List[SendHiveTransfer]): List of transfer details, each containing sender, receiver, amount, and memo.
        hive_client (Hive | None, optional): An instance of Hive client. If not provided, one will be created using the provided keys.
        keys (List[str], optional): List of private keys to use for signing transactions if hive_client is not provided.
        nobroadcast (bool, optional): If True, transactions will not be broadcasted to the network. Defaults to False.
        is_private (bool, optional): If True, indicates the operation should be private. Defaults to False.

    Returns:
        Dict[str, str]: The result of the broadcasted transaction, or an empty dictionary if not broadcasted.

    Raises:
        ValueError: If neither hive_client nor keys are provided, or if nobroadcast is True while hive_client is provided.
        HiveNotEnoughHiveInAccount: If the sender does not have sufficient funds.
        HiveTryingToSendZeroOrNegativeAmount: If attempting to send zero or negative amount, or duplicate transaction detected.
        HiveSomeOtherRPCException: For any other RPC or unexpected exceptions.
    """
    if transfer_list is None:
        transfer_list = []
    if custom_json_list is None:
        custom_json_list = []
    if keys is None:
        keys = []
    if not hive_client and not keys:
        raise ValueError("No hive_client or keys provided")
    if not hive_client:
        hive_client = Hive(keys=keys, nobroadcast=nobroadcast, node=default_hive_nodes())
    if hive_client and nobroadcast:
        raise ValueError(
            "nobroadcast is not supported if hive_client is passed, nobroadcast must be set in the hive_client"
        )
    # transfer = transfer_list[0]
    try:
        tx = TransactionBuilder(blockchain_instance=hive_client)
        for transfer in transfer_list:
            transfer_nectar = {
                "from": transfer.from_account,
                "to": transfer.to_account,
                "amount": transfer.amount,
                "memo": transfer.memo,
            }
            tx.appendOps(NectarTransfer(transfer_nectar))
            tx.appendSigner(transfer.from_account, "active")
        for custom_json in custom_json_list:
            custom_json_nectar = {
                "id": custom_json.cj_id,
                "json": custom_json.json_data,
                "required_auths": [custom_json.send_account],
                "required_posting_auths": [],
            }
            tx.appendOps(NectarCustomJson(custom_json_nectar))
            tx.appendSigner(custom_json.send_account, "active")

        # signed_tx = tx.sign()
        broadcast_tx = tx.broadcast()
        return broadcast_tx or {}
    except UnhandledRPCError as ex:
        # Handle insufficient funds
        logger.error(
            f"UnhandledRPCError during send_transfer: {ex}",
            extra={
                "notification": False,
                "transfer_list": transfer_list,
            },
        )
        raise HiveSomeOtherRPCException(f"{ex}")
        # for arg in ex.args:
        #     logger.error(arg)

        # if "does not have sufficient funds" in arg:
        #     raise HiveNotEnoughHiveInAccount(
        #         f"{transfer.from_account} Failure during send | "
        #         f"Not enough to pay {transfer.amount}  | "
        #         f"to: {transfer.to_account} | Hive error: {ex}",
        #         sending_amount=Amount(str(transfer.amount)),
        #     )
        # if "Cannot transfer a negative amount" in arg:
        #     raise HiveTryingToSendZeroOrNegativeAmount(
        #         f"{transfer.from_account} Failure during send | "
        #         f"Can't send negative or zero {transfer.amount}  | "
        #         f"to: {transfer.to_account} | Hive error: {ex}"
        #     )
        # if "Duplicate transaction check failed" in arg:
        #     raise HiveTryingToSendZeroOrNegativeAmount(
        #         f"{transfer.from_account} Failure during send | "
        #         f"Looks like we tried to send transaction twice | "
        #         f"{transfer.memo} | "
        #         f"{transfer.amount}  | "
        #         f"to: {transfer.to_account} | Hive error: {ex}"
        #     )
        # else:
        #     # trx = {"UnhandledRPCError": f"{ex}"}
    except Exception as ex:
        logger.error(
            f"UnhandledRPCError during send_transfer: {ex}",
            extra={
                "notification": False,
                "transfer_list": transfer_list,
            },
        )
        raise HiveSomeOtherRPCException(f"{ex}") from ex


async def send_pending(
    pending: PendingTransaction,
    hive_client: Hive | None = None,
) -> dict[str, str]:
    """
    Send a pending transaction.

    Args:
        pending (PendingTransaction): The pending transaction to send.
        hive_client (Hive | None, optional): An instance of Hive client. If not provided, one will be created using the provided keys.

    Returns:
        Dict[str, str]: The result of the broadcasted transaction, or an empty dictionary if not broadcasted.

    Raises:
        ValueError: If neither hive_client nor keys are provided, or if nobroadcast is True while hive_client is provided.
        HiveNotEnoughHiveInAccount: If the sender does not have sufficient funds.
        HiveTryingToSendZeroOrNegativeAmount: If attempting to send zero or negative amount, or duplicate transaction detected.
        HiveSomeOtherRPCException: For any other RPC or unexpected exceptions.
    """
    return await send_transfer(
        to_account=str(pending.to_account),
        amount=pending.amount,
        from_account=str(pending.from_account),
        memo=pending.memo,
        nobroadcast=pending.nobroadcast,
        hive_client=hive_client,
        store_pending=pending,
    )


async def send_transfer(
    to_account: str,
    amount: Amount,
    from_account: str,
    memo: str = "",
    hive_client: Hive | None = None,
    keys: list[str] | None = None,
    nobroadcast: bool = False,
    is_private: bool = False,
    store_pending: PendingTransaction | None = None,
) -> dict[str, str]:
    """
    Sends a transfer of Hive tokens from one account to another, with support for retries,
    private memos, and error handling.

    Args:
        to_account (str): The recipient Hive account name.
        amount (Amount): The amount to transfer, including asset type.
        from_account (str): The sender Hive account name.
        memo (str, optional): Memo to include with the transfer. Defaults to "".
        hive_client (Hive, optional): An existing Hive client instance. If not provided,
            one will be created using keys. Defaults to None.
        keys (List[str], optional): List of private keys for signing the transaction.
            Defaults to [].
        nobroadcast (bool, optional): If True, the transaction will not be broadcast to
            the network. Defaults to False.
        is_private (bool, optional): If True, the memo will be encrypted (prefixed with '#').
            Defaults to False.
        store_pending (PendingTransaction, optional): An existing PendingTransaction instance to update.
            If not provided, a new PendingTransaction will be created and saved before sending.
            Defaults to None.

    Returns:
        Dict[str, str]: The transaction result dictionary, including transaction ID and
        other details.

    Raises:
        ValueError: If neither hive_client nor keys are provided, or if the account is invalid.
        HiveNotEnoughHiveInAccount: If the sender does not have sufficient funds.
        HiveTryingToSendZeroOrNegativeAmount: If the transfer amount is zero or negative,
            or if a duplicate transaction is detected.
        HiveSomeOtherRPCException: For other RPC errors or if transaction expiration occurs
            after retries.

    """
    if keys is None:
        keys = []
    if not hive_client and not keys:
        raise ValueError("No hive_client or keys provided")
    if not hive_client:
        hive_client = Hive(keys=keys, nobroadcast=nobroadcast, node=default_hive_nodes())
    if hive_client and hive_client.nobroadcast and hive_client.nobroadcast != nobroadcast:
        raise ValueError("nobroadcast is not set to the same value as hive_client")

    account: Account = Account(from_account, blockchain_instance=hive_client)
    if not account:
        raise ValueError("Invalid account")
    try:
        await perform_transfer_checks(
            from_account=from_account,
            to_account=to_account,
            # amount=amount,
            # nobroadcast=nobroadcast,
        )
    except HiveDevelopmentAccountError as e:
        logger.error(f"HiveDevelopmentAccountError: {e}")
        raise
    except HiveAccountNameOnExchangesList:
        # This will be switched to a new account
        to_account = "v4vapp.sus"
    if is_private:
        memo = f"#{memo}"
    retries = 0
    if not store_pending:
        store_pending = await PendingTransaction(
            from_account=from_account,
            to_account=to_account,
            amount=amount,
            memo=memo,
            nobroadcast=nobroadcast,
            is_private=is_private,
            unique_key=f"{from_account}_{to_account}_{amount}_{memo}",
        ).save()
    while retries < 3:
        try:
            trx = account.transfer(
                to=to_account.lower(),
                amount=amount.amount,
                asset=str(amount.symbol),
                account=from_account,
                memo=memo,
            )
            # Delete the pending transaction since Hive transaction was successful
            await store_pending.delete()
            check_nobroadcast = " NO BROADCAST " if hive_client.nobroadcast else ""
            logger.info(
                f"Transfer sent{check_nobroadcast}: {from_account} -> {to_account} | "
                f"Amount: {amount.amount_decimal:.3f} {amount.symbol} | "
                f"Memo: {memo} {trx.get('trx_id', '')}",
                extra={**store_pending.log_extra},
            )
            return trx

        except (UnhandledRPCError, RPCError) as ex:
            # Handle insufficient funds
            for arg in ex.args:
                if "does not have sufficient funds" in arg:
                    raise HiveNotEnoughHiveInAccount(
                        f"{from_account} Failure during send | "
                        f"Not enough to pay {amount.amount_decimal:.3f} {amount.symbol} | "
                        f"to: {to_account} | Hive error: {ex}",
                        sending_amount=amount,
                    )
                elif "not enough RC mana" in arg:
                    raise HiveNotEnoughHiveRCMana(
                        f"{from_account} Failure during send | "
                        f"Not enough RC mana to pay {amount.amount_decimal:.3f} {amount.symbol} | "
                        f"to: {to_account} | Hive error: {ex}",
                        sending_amount=amount,
                    )
                elif "Cannot transfer a negative amount" in arg:
                    await store_pending.delete()
                    raise HiveTryingToSendZeroOrNegativeAmount(
                        f"{from_account} Failure during send | "
                        f"Can't send negative or zero {amount.amount_decimal:.3f} {amount.symbol} | "
                        f"to: {to_account} | Hive error: {ex}"
                    )
                elif "Duplicate transaction check failed" in arg:
                    await store_pending.delete()
                    raise HiveTryingToSendZeroOrNegativeAmount(
                        f"{from_account} Failure during send | "
                        f"Looks like we tried to send transaction twice | "
                        f"{memo} | "
                        f"{amount.amount_decimal:.3f} {amount.symbol} | "
                        f"to: {to_account} | Hive error: {ex}"
                    )
                elif "transaction expiration exception" in arg:
                    logger.warning(
                        f"Transaction expired: {arg}",
                        extra={
                            "notification": False,
                            "to_account": to_account,
                            "from_account": from_account,
                            "amount": amount.amount_decimal,
                            "symbol": amount.symbol,
                            "memo": memo,
                        },
                    )
                    retries += 1
                    logger.warning(
                        f"Retrying send_transfer {retries}/3 for {from_account} -> {to_account}",
                        extra={
                            "notification": True,
                            "to_account": to_account,
                            "from_account": from_account,
                            "amount": amount.amount_decimal,
                            "symbol": amount.symbol,
                            "memo": memo,
                        },
                    )
                    if retries >= 3:
                        logger.error(
                            f"Transaction expired after 3 retries: {ex}",
                            extra={
                                "notification": True,
                                "to_account": to_account,
                                "from_account": from_account,
                                "amount": amount.amount_decimal,
                                "symbol": amount.symbol,
                                "memo": memo,
                            },
                        )
                        raise HiveSomeOtherRPCException(
                            f"Transaction expired after 3 retries: {ex}"
                        )
                    continue
                else:
                    trx = {"UnhandledRPCError": f"{ex}"}
                    logger.error(
                        f"UnhandledRPCError during send_transfer: {ex}",
                        extra={
                            "notification": True,
                            "to_account": to_account,
                            "from_account": from_account,
                            "amount": amount.amount_decimal,
                            "symbol": amount.symbol,
                            "memo": memo,
                        },
                    )
                    raise HiveSomeOtherRPCException(f"{ex}")
        except MissingKeyError as ex:
            await store_pending.delete()
            logger.error(
                f"MissingKeyError during send_transfer: {ex}",
                extra={
                    "notification": True,
                    "to_account": to_account,
                    "from_account": from_account,
                    "amount": amount.amount_decimal,
                    "symbol": amount.symbol,
                    "memo": memo,
                },
            )
            raise HiveMissingKeyError(
                f"{from_account} Failure during send | "
                f"Missing Key | "
                f"{memo} | "
                f"{amount.amount_decimal:.3f} {amount.symbol} | "
                f"to: {to_account} | Hive error: {ex}"
            )

        except Exception as ex:
            logger.error(
                f"UnhandledRPCError during send_transfer: {ex} {ex.__class__.__name__}",
                extra={
                    "notification": True,
                    "to_account": to_account,
                    "from_account": from_account,
                    "amount": amount.amount_decimal,
                    "symbol": amount.symbol,
                    "memo": memo,
                },
            )
            raise HiveSomeOtherRPCException(f"{ex}") from ex
    return {}


def process_user_memo(memo: str) -> str:
    """
    Processes a user memo by removing any leading '#' character.

    Args:
        memo (str): The user memo to process.

    Returns:
        str: The processed memo without the leading '#' character.
    """
    # TODO: This needs to process tags like #clean and #keepsats to return a correct memo to pass on.
    # this is where #clean needs to be evaluated
    if not memo:
        return ""
    if memo.startswith("#"):
        return memo[1:]
    return memo


@backoff.on_exception(
    backoff.expo,
    (Exception,),
    max_tries=3,
    jitter=backoff.full_jitter,
    logger=logger,
)
def witness_signing_key(witness_name: str) -> str | None:
    """
    Retrieves the current signing key for a given Hive witness.

    Args:
        witness_name (str): The name of the witness.

    Returns:
        str | None: The current signing key of the witness, or None if not found.

    """
    ICON = "X"
    try:
        hive = Hive(node=default_hive_nodes())
        if not hive or not hive.rpc:
            logger.warning(
                f"{ICON} Could not get Hive client to retrieve signing key for witness {witness_name}.",
                extra={"notification": False},
            )
            return None
        witness_info: dict[str, Any] | None = hive.rpc.get_witness_by_account(witness_name)
        if not witness_info or "signing_key" not in witness_info:
            logger.warning(
                f"{ICON} Could not retrieve witness info for {witness_name}.",
                extra={"notification": False},
            )
            return None
        return witness_info["signing_key"]
    except Exception as e:
        logger.error(
            f"{ICON} Error retrieving signing key for witness {witness_name}: {e}",
            extra={"notification": False},
        )
        return None


def get_hive_amount_from_trx_reply(trx: dict[str, Any]) -> Amount:
    """
    Extracts the amount of a specific asset from a Hive transaction.

    Args:
        trx (Dict[str, Any]): The Hive transaction data.
        asset_symbol (str): The symbol of the asset to extract (e.g., "HIVE" or "HBD").

    Returns:
        Amount: The extracted amount as an Amount object, or a default amount of 0.000 HIVE if not found.
    """
    try:
        # Did this return type change? Used to need trx["operations"][0][1]["amount"]
        # but that was not working for some reason, so changed to this, but need
        # to verify it works for all cases.
        op_dict = trx["operations"][0]["value"]  # type: dict
        return Amount(op_dict["amount"])
    except (KeyError, IndexError):
        try:
            return Amount(trx["operations"][0][1]["amount"])
        except (KeyError, IndexError) as e:
            logger.error(
                f"Failed to parse return amount from transaction: {e}",
                extra={"notification": False, "trx": trx},
            )
    return Amount("0.000 HIVE")


if __name__ == "__main__":
    nodes = get_good_nodes()
    print(nodes)
    # witness = get_hive_witness_details("brianoflondon")
    # print(witness)
