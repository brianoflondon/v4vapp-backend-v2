import json
import random
import struct
from typing import Any, Dict, List

import httpx
from ecdsa import MalformedPointError  # type: ignore
from nectar import Hive
from nectar.blockchain import Blockchain
from nectar.exceptions import MissingKeyError
from nectar.market import Market
from nectar.memo import Memo
from nectar.price import Price
from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis

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

EXCLUDE_NODES = [
    "https://rpc.mahdiyari.info",
]

MAX_HIVE_BATCH_SIZE = 25


# TODO: #28 Tidy up the calls to redis sync for good nodes and hive internal market
def get_hive_client(*args, **kwargs) -> Hive:
    """
    Create a Hive client instance.

    Returns:
        HiveClient: A Hive client instance.
    """
    if "node" not in kwargs:
        # shuffle good nodes
        with V4VAsyncRedis().sync_redis as redis_sync_client:
            good_nodes_json = redis_sync_client.get("good_nodes")
        if good_nodes_json and isinstance(good_nodes_json, str):
            ttl = redis_sync_client.ttl("good_nodes")
            if isinstance(ttl, int) and ttl < 3000:
                good_nodes = get_good_nodes()
            else:
                good_nodes = json.loads(good_nodes_json)
        else:
            good_nodes = get_good_nodes()
        redis_sync_client.close()
        random.shuffle(good_nodes)
        kwargs["node"] = good_nodes

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
    raise ValueError(f"No working node found {errors} errors")


def get_blockchain_instance(*args, **kwargs) -> Blockchain:
    """
    Create a Blockchain instance.
    """
    if "hive_instance" not in kwargs:
        hive = get_hive_client(*args, **kwargs)
        blockchain = Blockchain(hive_instance=hive, *args, **kwargs)
    else:
        blockchain = Blockchain(*args, **kwargs)

    return blockchain


def get_good_nodes() -> List[str]:
    """
    Fetches a list of default nodes from the specified API endpoint.

    This function sends a GET request to "https://beacon.peakd.com/api/nodes"
    and retrieves a list of nodes. It then filters the nodes to include only
    those with a score of 100 and returns their endpoints.

    Returns:
        List[str]: A list of endpoints for nodes with a score of 100.
    """
    try:
        response = httpx.get(
            "https://beacon.peakd.com/api/nodes",
        )
        nodes = response.json()
        logger.debug("Fetched good nodes Last good nodes", extra={"beacon_response": nodes})
        good_nodes = [node["endpoint"] for node in nodes if node["score"] == 100]
        good_nodes = [node for node in good_nodes if node not in EXCLUDE_NODES]
        logger.debug(f"Good nodes {good_nodes}", extra={"good_nodes": good_nodes})
        with V4VAsyncRedis().sync_redis as redis_sync_client:
            redis_sync_client.setex("good_nodes", 3600, json.dumps(good_nodes))
    except Exception as e:
        with V4VAsyncRedis().sync_redis as redis_sync_client:
            good_nodes_json = redis_sync_client.get("good_nodes")
        if good_nodes_json and isinstance(good_nodes_json, str):
            good_nodes = json.loads(good_nodes_json)
        if good_nodes:
            logger.warning(f"Failed to fetch good nodes: {e} using last good nodes.", {"extra": e})
        else:
            logger.warning(f"Failed to fetch good nodes: {e} using default nodes.", {"extra": e})
            good_nodes = DEFAULT_GOOD_NODES

    return good_nodes


# async def get_hive_witness_details(hive_accname: str = "") -> WitnessDetails | None:
#     """
#     Fetches details about a Hive witness.

#     This function sends a GET request to "https://api.syncad.com/hafbe-api/witnesses"
#     and retrieves the details of a Hive witness with the specified account name.

#     Args:
#         hive_accname (str): The account name of the Hive witness.

#     Returns:
#         dict: A dictionary containing the details of the Hive witness.
#     """
#     try:
#         if not hive_accname:
#             url = "https://api.syncad.com/hafbe-api/witnesses"
#         else:
#             url = f"https://api.syncad.com/hafbe-api/witnesses/{hive_accname}"
#         async with httpx.AsyncClient() as client:
#             response = await client.get(url, timeout=20)
#             if response.status_code == 200:
#                 answer = response.json()
#                 async with V4VAsyncRedis() as redis_client:
#                     await redis_client.set(
#                         name=f"witness_{hive_accname}", value=json.dumps(answer)
#                     )
#             else:
#                 async with V4VAsyncRedis() as redis_client:
#                     answer = json.loads(await redis_client.get(f"witness_{hive_accname}"))
#                     if not answer:
#                         logger.warning(
#                             f"Failed to get_hive_witness_details "
#                             f"from cache after error: {response.status_code}"
#                         )
#                         return None

#             wd = WitnessDetails.model_validate(answer)
#             return wd

#     except httpx.ConnectTimeout as e:
#         logger.warning(
#             f"Failed to get_hive_witness_details: {e}",
#             extra={"notification": False},
#         )
#         try:
#             async with V4VAsyncRedis() as redis_client:
#                 answer = json.loads(await redis_client.get(f"witness_{hive_accname}"))
#                 if answer:
#                     wd = WitnessDetails.model_validate(answer)
#                     return wd
#             logger.warning(
#                 f"Failed to get_hive_witness_details "
#                 f"from cache after error: {response.status_code}"
#             )
#         except Exception as e:
#             logger.error(
#                 f"Failed to get_hive_witness_details from cache: {e}",
#                 extra={"notification": False},
#             )

#     except Exception as e:
#         logger.exception(f"Failed to get_hive_witness_details: {e}", extra={"notification": False})
#         try:
#             async with V4VAsyncRedis() as redis_client:
#                 answer = json.loads(await redis_client.get(f"witness_{hive_accname}"))
#                 if answer:
#                     wd = WitnessDetails.model_validate(answer)
#                     return wd

#             logger.warning(
#                 f"Failed to get_hive_witness_details "
#                 f"from cache after error: {response.status_code}"
#             )
#         except Exception as e:
#             logger.exception(f"Failed to get_hive_witness_details from cache: {e}")
#     return None


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
    try:
        hive = get_hive_client()
        market = Market("HBD:HIVE", hive=hive)
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
        return d_memo[1:]
    except struct.error:
        # arises when an unencrypted memo is decrypted..
        return memo
    except ValueError as e:
        # Memo is not encrypted
        logger.info(f"Memo is not encrypted: {e}")
        return memo
    except (MissingKeyError, MalformedPointError) as e:
        logger.info(f"MissingKeyError: {e}")
        return memo

    except Exception as e:
        logger.error(f"Problem in decode_memo: {e}", extra={"trx_id": trx_id, "memo": memo})
        logger.error(memo)
        logger.exception(e)
        return memo


if __name__ == "__main__":
    nodes = get_good_nodes()
    print(nodes)
    # witness = get_hive_witness_details("brianoflondon")
    # print(witness)
