import json
import random
import struct
from typing import Any, Dict, List, Tuple
from uuid import uuid4

import backoff
import httpx
from ecdsa import MalformedPointError  # type: ignore
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
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
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
    "https://hive-api.3speak.tv",
    "https://hiveapi.actifit.io",
    "https://api.syncad.com",
]

BLOCK_STREAM_ONLY = ["https://rpc.podping.org/"]

EXCLUDE_NODES = [
    # "https://rpc.mahdiyari.info",
    "https://api.hive.blog",
    # "https://api.deathwing.me",
    # "https://hive-api.arcange.eu",
    # "https://api.openhive.network",
    # "https://techcoderx.com",
    # "https://api.c0ff33a.uk",
    # "https://hiveapi.actifit.io",
    # "https://api.syncad.com",
    # "https://hive-api.dlux.io",
    "https://hive-api.3speak.tv",
]

MAX_HIVE_BATCH_SIZE = 25

HIVE_BLOCK_TIME = 3  # seconds


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

    pass


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

    pass


class HiveMissingKeyError(HiveTransferError):
    """
    Exception raised when a required key is missing.
    """

    pass


class HiveSomeOtherRPCException(HiveTransferError):
    """
    Exception raised for other unhandled RPC errors.
    This is a catch-all for any other exceptions that do not fit the specific cases.
    """

    pass


class HiveToKeepsatsConversionError(HiveTransferError):
    """Custom exception for Hive to Keepsats conversion errors."""

    pass


class HiveConversionLimits(HiveTransferError):
    """Custom exception for conversion limit errors."""

    pass


class HiveAccountNameOnExchangesList(HiveTransferError):
    """Custom exception for when a Hive account name is found on exchanges list."""

    pass


class HiveDevelopmentAccountError(HiveTransferError):
    """Custom exception for development-related errors."""

    pass


# TODO: #28 Tidy up the calls to redis sync for good nodes and hive internal market
def get_hive_client(stream_only: bool = False, nobroadcast: bool = False, *args, **kwargs) -> Hive:
    """
    Creates and returns a Hive client instance, selecting a working node from a list of available nodes.
    If no node is provided in kwargs, retrieves a list of good nodes from Redis cache or regenerates it if necessary.
    Optionally includes stream-only nodes if `stream_only` is True.
    Attempts to instantiate a Hive client with each node in the list until successful, or raises an error if all nodes fail.

    Args:
        stream_only (bool, optional): If True, includes stream-only nodes in the selection. Defaults to False.
        nobroadcast (bool, optional): If True, will not broadcast transactions. Defaults to False.
        *args: Additional positional arguments to pass to the Hive client constructor.
        **kwargs: Additional keyword arguments to pass to the Hive client constructor. If 'node' is not provided, it will be set internally.

        Hive: An instance of the Hive client connected to a working node.

    Raises:
        ValueError: If no working node can be found after trying all available nodes.
    """
    if "node" not in kwargs:
        # shuffle good nodes
        good_nodes: List[str] = []
        try:
            good_nodes_json = InternalConfig.redis_decoded.get("good_nodes")
            if good_nodes_json and isinstance(good_nodes_json, str):
                ttl = InternalConfig.redis_decoded.ttl("good_nodes")
                if isinstance(ttl, int) and ttl < 3000:
                    good_nodes = get_good_nodes()
                else:
                    good_nodes = json.loads(good_nodes_json)
        except Exception as e:
            logger.warning(f"Redis not available {e}", extra={"notification": False})
        if not good_nodes:
            good_nodes = get_good_nodes()
        if stream_only:
            good_nodes += BLOCK_STREAM_ONLY
        random.shuffle(good_nodes)
        kwargs["node"] = good_nodes
    if "nobroadcast" not in kwargs:
        kwargs["nobroadcast"] = nobroadcast

    count = len(kwargs["node"])
    errors = 0
    while errors < count:
        try:
            hive = Hive(*args, **kwargs)
            return hive
        except TypeError as e:
            logger.warning(
                f"Node {kwargs['node'][0]} not working {e} error: {errors}",
                extra={"notification": True, "nodes": kwargs["node"]},
            )
            # remove the first node from the list
            kwargs["node"] = kwargs["node"][1:]
            errors += 1
        except Exception as e:
            logger.warning(
                f"Node {kwargs['node'][0]} not working {e} error: {errors}",
                extra={"notification": True, "nodes": kwargs["node"]},
            )
            # remove the first node from the list
            kwargs["node"] = kwargs["node"][1:]
            errors += 1
    raise ValueError(f"No working node found {errors} errors")


def get_blockchain_instance(*args, **kwargs) -> Blockchain:
    """
    Create a Blockchain instance.
    """
    if "hive_instance" not in kwargs:
        kwargs["hive"] = get_hive_client(*args, **kwargs)
        kwargs["mode"] = kwargs.get("mode", "head")
        blockchain = Blockchain(*args, **kwargs)
    else:
        kwargs["mode"] = "head"
        blockchain = Blockchain(*args, **kwargs)

    return blockchain


def get_good_nodes() -> List[str]:
    """
    Fetches a list of default nodes from the specified API endpoint.

    This function sends a GET request to "https://beacon.peakd.com/api/nodes"
    but uses my proxy endpoint "https://devapi.v4v.app/v2/beacon/nodes/"
    and retrieves a list of nodes. It then filters the nodes to include only
    those with a score of 100 and returns their endpoints.

    Returns:
        List[str]: A list of endpoints for nodes with a score of 100.
    """
    good_nodes: List[str] = []
    try:
        params = {
            "source": "v4vapp_backend",
        }
        response = httpx.get(
            "https://beacon.v4v.app/",
            params=params,
            timeout=5,
            follow_redirects=True,
        )
        nodes = response.json()
        logger.debug(
            "Fetched good nodes Last good nodes",
            extra={"beacon_response": nodes, "error_code_clear": "beacon_nodes_fail"},
        )
        good_nodes = [node["endpoint"] for node in nodes if node["score"] == 100]
        good_nodes = [node for node in good_nodes if node not in EXCLUDE_NODES]
        logger.debug(f"Good nodes {good_nodes}", extra={"good_nodes": good_nodes})
        try:
            InternalConfig.redis_decoded.setex("good_nodes", 3600, json.dumps(good_nodes))
        except Exception as e:
            logger.warning(
                f"Failed to set good nodes in Redis: {e}", extra={"notification": False}
            )
    except Exception as e:
        good_nodes_json = InternalConfig.redis_decoded.get("good_nodes")
        if good_nodes_json and isinstance(good_nodes_json, str):
            good_nodes = json.loads(good_nodes_json)
        if good_nodes:
            logger.warning(
                f"Failed to fetch good nodes: {e} using last good nodes.",
                extra={
                    "notification": False,
                    "error_code": "beacon_nodes_fail",
                },
            )
        else:
            logger.warning(
                f"Failed to fetch good nodes: {e} using default nodes.",
                extra={
                    "notification": False,
                    "error_code": "beacon_nodes_fail",
                },
            )
            good_nodes = DEFAULT_GOOD_NODES
            InternalConfig.redis_decoded.setex("good_nodes", 3600, json.dumps(good_nodes))

    return good_nodes


async def get_verified_hive_client(
    hive_role: HiveRoles = HiveRoles.server,
    nobroadcast: bool = False,
) -> Tuple[Hive, str]:
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
    hive_config = InternalConfig().config.hive
    hive_account = hive_config.get_hive_role_account(hive_role)
    if not hive_account:
        raise HiveToLightningError("Missing Hive server account configuration for repayment")

    memo_key = hive_account.memo_key or ""
    active_key = hive_account.active_key or ""
    posting_key = hive_account.posting_key or ""

    keys = [key for key in [memo_key, active_key, posting_key] if key]

    if not keys:
        raise HiveToLightningError("Missing Hive server account keys for repayment")

    hive_client = get_hive_client(
        keys=keys,
        nobroadcast=nobroadcast,
    )
    return hive_client, hive_account.name


async def get_verified_hive_client_for_accounts(
    accounts: List[str],
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
    hive_config = InternalConfig().config.hive
    hive_accounts = []
    keys = []
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
        ]
    if keys == ["", ""]:
        hive_client = get_hive_client(nobroadcast=nobroadcast)
    else:
        hive_client = get_hive_client(
            keys=keys,
            nobroadcast=nobroadcast,
        )
    return hive_client


def get_transfer_cust_id(
    from_acc: AccNameType,
    to_acc: AccNameType,
    hive_config=None,
    expense_accounts: list[str] | None = None,
) -> CustIDType:
    """
    Compute the customer id (cust_id) for a transfer a module-level helper.

    Parameters:
        from_acc: sender account name
        to_acc: recipient account name
        hive_config: optional object with attribute `all_account_names` (server, treasury, funding, exchange)
        expense_accounts: optional list of expense account names (defaults to ["privex"]).

    Returns:
        CustIDType: computed customer id following the same rules as the original method.
    """
    if hive_config is None:
        hive_config = InternalConfig().config.hive
    account_names = hive_config.all_account_names

    # Defensive check - ensure we have exactly 4 account names
    if not account_names or len(account_names) != 4:
        return f"{to_acc}->{from_acc}"

    server_account, treasury_account, funding_account, exchange_account = account_names

    expense_accounts = InternalConfig().config.expense_config.hive_expense_accounts or []

    # Server to Treasury: cust_id = to_account (treasury)
    if from_acc == server_account and to_acc == treasury_account:
        return to_acc

    # Treasury to Server: cust_id = from_account (treasury)
    elif from_acc == treasury_account and to_acc == server_account:
        return from_acc

    # Funding to Treasury: cust_id = from_account (funding)
    elif from_acc == funding_account and to_acc == treasury_account:
        return from_acc

    # Treasury to Funding: cust_id = to_account (funding)
    elif from_acc == treasury_account and to_acc == funding_account:
        return to_acc

    # Treasury to Exchange: cust_id = to_account (exchange)
    elif from_acc == treasury_account and to_acc == exchange_account:
        return to_acc

    # Exchange to Treasury: cust_id = from_account (exchange)
    elif from_acc == exchange_account and to_acc == treasury_account:
        return from_acc

    # Server to expense: cust_id = to_account (expense)
    elif from_acc == server_account and to_acc in expense_accounts:
        return to_acc

    # Server to customer (withdrawal): cust_id = to_account (customer)
    elif from_acc == server_account:
        return to_acc

    # Customer to server (deposit): cust_id = from_account (customer)
    elif to_acc == server_account:
        return from_acc

    else:
        return f"{to_acc}:{from_acc}"


class HiveInternalQuote(BaseModel):
    hive_hbd: float | None = None
    raw_response: Dict[str, Any] = {}
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
    hive = get_hive_client()
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
        return answer
    except Exception as ex:
        # logging.exception(ex)
        logger.info(
            f"Calling Market API on Hive: {market['blockchain_instance'].data['last_node']}"
        )
        message = f"Problem calling Hive Market API {ex}"
        logger.error(message)
        return HiveInternalQuote(error=message)


# @time_decorator
def account_hive_balances(hive_accname: str = "") -> Dict[str, Amount | str]:
    """
    Retrieves the current HIVE and HBD balances for the given account.
    Returns

    Returns:
        Dict[str, float]: A dictionary containing the HIVE and HBD balances.
    """
    hive = get_hive_client()
    if not hive_accname:
        hive_accname = InternalConfig().server_id
    hive_account = Account(hive_accname, blockchain_instance=hive)
    try:
        balances: List[Amount] | None = hive_account.balances.get("available", None)
        if not balances or len(balances) < 2:
            return {"HIVE": Amount("0.000 HIVE"), "HBD": Amount("0.000 HBD")}
        return {
            "HIVE": balances[0],
            "HBD": balances[1],
            "HIVE_fmt": f"{balances[0].amount:,.3f}",
            "HBD_fmt": f"{balances[1].amount:,.3f}",
        }
    except Exception as e:
        logger.error(f"Error fetching server hive balances: {e}")
        raise HiveSomeOtherRPCException(f"Error fetching server hive balances: {e}")


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
    return f"{trx_id}_{op_in_trx}" if not int(op_in_trx) == 0 else str(trx_id)


def decode_memo(
    memo: str = "",
    hive_inst: Hive | None = None,
    memo_keys: List[str] = [],
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
    if not memo and not trx_id:
        return ""

    if not memo_keys and not hive_inst:
        raise ValueError("No memo keys or Hive instance provided.")

    if memo_keys and not hive_inst:
        hive_inst = get_hive_client(keys=memo_keys)
        blockchain = get_blockchain_instance(hive_instance=hive_inst)

    if not hive_inst:
        raise ValueError("No Hive instance provided.")

    if trx_id and not memo:
        blockchain = get_blockchain_instance(hive_instance=hive_inst)
        trx = blockchain.get_transaction(trx_id)
        memo = trx.get("operations")[op_in_trx].get("value").get("memo")

    if not memo[0] == "#":
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
    except (MissingKeyError, MalformedPointError) as e:
        logger.debug(f"MissingKeyError: {e}")
        return memo

    except Exception as e:
        logger.error(f"Problem in decode_memo: {e}", extra={"trx_id": trx_id, "memo": memo})
        logger.error(memo)
        logger.exception(e)
        return memo


async def send_custom_json(
    json_data: Dict[str, Any],
    send_account: str,
    hive_client: Hive | None = None,
    keys: List[str] = [],
    id: str = "v4vapp_transfer",
    nobroadcast: bool = False,
    active: bool = True,
    resend_attempt: int = 0,
) -> Dict[str, str]:
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
        ValueError: If `json_data` is not a dictionary, is empty, or if neither `hive_client`
            nor `keys` are provided.
        CustomJsonSendError: If an error occurs while sending the custom JSON operation.
    """
    # Need Required_auths not posting auths for a transfer
    # test json data is a dict which will become a nice json object:

    json_data_converted: Dict[str, Any] = convert_decimals_to_float_or_int(json_data)
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
    if not isinstance(json_data_converted, dict):
        raise ValueError("json_data must be a dictionary")
    if not json_data_converted:
        raise ValueError("json_data must not be empty")
    if not hive_client and not keys:
        raise ValueError("No hive_client or keys provided")
    if not hive_client:
        hive_client = get_hive_client(keys=keys)
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
        raise CustomJsonSendError("Wrong key used", extra={"send_account": send_account})
    except Exception as ex:
        logger.exception(ex, extra={"notification": False})
        logger.error(f"{send_account} {ex} {ex.__class__}", extra={"notification": False})
        raise CustomJsonSendError(f"Error sending custom_json: {ex}")


async def perform_transfer_checks(
    from_account: str,
    to_account: str,
    amount: Amount = Amount(amount="0.000 HIVE"),
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
    if await check_not_development_accounts([from_account, to_account]):
        raise HiveDevelopmentAccountError(
            f"{from_account} or {to_account} is not in allowed hive accounts for development mode"
        )
    bad_accounts_set = await get_bad_hive_accounts()
    message = ""
    for account in [from_account, to_account]:
        if account in bad_accounts_set:
            message += f"{account} is on the bad accounts list "
    if message:
        raise HiveAccountNameOnExchangesList(message)
    return True


async def send_transfer_bulk(
    transfer_list: List[PendingTransaction] = [],
    custom_json_list: List[PendingCustomJson] = [],
    hive_client: Hive | None = None,
    keys: List[str] = [],
    nobroadcast: bool = False,
    is_private: bool = False,
) -> Dict[str, Any]:
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
    if not hive_client and not keys:
        raise ValueError("No hive_client or keys provided")
    if not hive_client:
        hive_client = get_hive_client(keys=keys, nobroadcast=nobroadcast)
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
        raise HiveSomeOtherRPCException(f"{ex}")


async def send_pending(
    pending: PendingTransaction,
    hive_client: Hive | None = None,
) -> Dict[str, str]:
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
    keys: List[str] = [],
    nobroadcast: bool = False,
    is_private: bool = False,
    store_pending: PendingTransaction | None = None,
) -> Dict[str, str]:
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
    if not hive_client and not keys:
        raise ValueError("No hive_client or keys provided")
    if not hive_client:
        hive_client = get_hive_client(keys=keys, nobroadcast=nobroadcast)
    if hive_client.nobroadcast and hive_client.nobroadcast != nobroadcast:
        raise ValueError("nobroadcast is not set to the same value as hive_client")
    account: Account = Account(from_account, blockchain_instance=hive_client)
    if not account:
        raise ValueError("Invalid account")
    try:
        await perform_transfer_checks(
            from_account=from_account,
            to_account=to_account,
            amount=amount,
            nobroadcast=nobroadcast,
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
                to=to_account,
                amount=amount.amount,
                asset=amount.asset,
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
            raise HiveSomeOtherRPCException(f"{ex}")
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
        hive = get_hive_client()
        if not hive or not hive.rpc:
            logger.warning(
                f"{ICON} Could not get Hive client to retrieve signing key for witness {witness_name}.",
                extra={"notification": False},
            )
            return None
        witness_info: Dict[str, Any] | None = hive.rpc.get_witness_by_account(witness_name)
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


if __name__ == "__main__":
    nodes = get_good_nodes()
    print(nodes)
    # witness = get_hive_witness_details("brianoflondon")
    # print(witness)
