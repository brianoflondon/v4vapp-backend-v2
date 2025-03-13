import random
import struct
from enum import StrEnum
from typing import Any, Dict, List

import httpx
from beem import Hive  # type: ignore
from beem.blockchain import Blockchain  # type: ignore
from beem.market import Market  # type: ignore
from beem.memo import Memo  # type: ignore
from beem.price import Price  # type: ignore
from pydantic import BaseModel  # type: ignore

from v4vapp_backend_v2.config.setup import logger

DEFAULT_GOOD_NODES = [
    "https://api.hive.blog",
    "https://api.deathwing.me",
    "https://hive-api.arcange.eu",
    "https://api.openhive.network",
    "https://techcoderx.com",
    "https://api.c0ff33a.uk",
    "https://hive-api.3speak.tv",
    "https://hiveapi.actifit.io",
    "https://rpc.mahdiyari.info",
    "https://api.syncad.com",
]
MAX_HIVE_BATCH_SIZE = 25


def get_hive_client(*args, **kwargs) -> Hive:
    """
    Create a Hive client instance.

    Returns:
        HiveClient: A Hive client instance.
    """
    if "node" not in kwargs:
        # shuffle good nodes
        good_nodes = get_good_nodes()
        random.shuffle(good_nodes)
        kwargs["node"] = good_nodes
    hive = Hive(*args, **kwargs)
    return hive


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
        good_nodes = [node["endpoint"] for node in nodes if node["score"] == 100]
    except Exception as e:
        logger.warning(f"Failed to fetch good nodes: {e}")
        good_nodes = DEFAULT_GOOD_NODES

    return good_nodes


async def get_hive_witness_details(hive_accname: str) -> dict:
    """
    Fetches details about a Hive witness.

    This function sends a GET request to "https://api.syncad.com/hafbe-api/witnesses"
    and retrieves the details of a Hive witness with the specified account name.

    Args:
        hive_accname (str): The account name of the Hive witness.

    Returns:
        dict: A dictionary containing the details of the Hive witness.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.syncad.com/hafbe-api/witnesses/{hive_accname}",
            )
            answer = response.json()
    except Exception as e:
        logger.warning(f"Failed to get_hive_witness_details: {e}")
        return {}

    witness = answer.get("witness")
    if witness and witness.get("witness_name") == hive_accname:
        return witness

    return {}


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
        market = Market(hive=hive)
        ticker = market.ticker()

        # raise KeyError("'highest_bid'")
        highest_bid: Price = ticker["highest_bid"]
        highest_bid_value = float(highest_bid["price"])
        lowest_ask: Price = ticker["lowest_ask"]
        lowest_ask_value = float(lowest_ask["price"])
        hive_hbd = float(
            ((lowest_ask_value - highest_bid_value) / 2) + highest_bid_value
        )
        answer = HiveInternalQuote(hive_hbd=hive_hbd, raw_response=ticker)
        return answer
    except Exception as ex:
        # logging.exception(ex)
        logger.info(
            f"Calling Market API on Hive: "
            f"{market['blockchain_instance'].data['last_node']}"
        )
        message = f"Problem calling Hive Market API {ex}"
        logger.error(message)
        return HiveInternalQuote(error=message)


class HiveExp(StrEnum):
    HiveHub = "https://hivehub.dev/tx/{trx_id}"
    HiveBlockExplorer = "https://hiveblockexplorer.com/tx/{trx_id}"
    HiveExplorer = "https://hivexplorer.com/tx/{trx_id}"
    HiveScanInfo = "https://hivescan.info/transaction/{trx_id}"


def get_hive_block_explorer_link(
    trx_id: str, block_explorer: HiveExp = HiveExp.HiveHub, markdown: bool = False
) -> str:
    """
    Generate a Hive blockchain explorer URL for a given transaction ID.

    Args:
        trx_id (str): The transaction ID to include in the URL
        block_explorer (HiveExp): The blockchain explorer to use (defaults to HiveHub)

    Returns:
        str: The complete URL with the transaction ID inserted
    """
    link_html = block_explorer.value.format(trx_id=trx_id)
    if not markdown:
        return link_html
    markdown_link = f"[{block_explorer.name}]({link_html})"
    return markdown_link


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
    if not memo_keys and not hive_inst:
        raise ValueError("No memo keys or Hive instance provided.")

    if memo_keys and not hive_inst:
        hive_inst = get_hive_client(keys=memo_keys)
        blockchain = get_blockchain_instance(hive_instance=hive_inst)
    else:
        blockchain = get_blockchain_instance(hive_instance=hive_inst)

    if not hive_inst:
        raise ValueError("No Hive instance provided.")

    if trx_id and not memo:
        trx = blockchain.get_transaction(trx_id)
        memo = trx.get("operations")[op_in_trx].get("value").get("memo")

    try:
        m = Memo(from_account=None, to_account=None, blockchain_instance=hive_inst)
        d_memo = m.decrypt(memo)[1:]
        return d_memo
    except struct.error:
        # arrises when an unencrypted memo is decrypted..
        return memo
    except Exception as e:
        logger.error(
            f"Problem in decode_memo: {e}", extra={"trx_id": trx_id, "memo": memo}
        )
        logger.error(memo)
        logger.exception(e)
        return memo


if __name__ == "__main__":
    nodes = get_good_nodes()
    print(nodes)
    # witness = get_hive_witness_details("brianoflondon")
    # print(witness)
