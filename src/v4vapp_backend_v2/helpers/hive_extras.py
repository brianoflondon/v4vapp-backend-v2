import random
from enum import StrEnum
from typing import List

import httpx
from beem import Hive  # type: ignore
from beem.blockchain import Blockchain  # type: ignore

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
        # shuffle goog nodes
        good_nodes = get_good_nodes()
        random.shuffle(good_nodes)
        kwargs["node"] = good_nodes
    hive = Hive(*args, **kwargs)
    return hive


def get_blockchain_instance(*args, **kwargs) -> Blockchain:
    """
    Create a Blockchain instance.
    """

    hive = get_hive_client(*args, **kwargs)
    blockchain = Blockchain(hive_instance=hive, *args, **kwargs)

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


def get_hive_witness_details(hive_accname: str) -> dict:
    """
    Fetches details about a Hive witness.

    This function sends a GET request to "https://api.hive.blog/witnesses"
    and retrieves the details of a Hive witness with the specified account name.

    Args:
        hive_accname (str): The account name of the Hive witness.

    Returns:
        dict: A dictionary containing the details of the Hive witness.
    """
    try:
        response = httpx.get(
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


class HiveExp(StrEnum):
    HiveHub = "https://hivehub.dev/tx/{trx_id}"
    HiveBlockExplorer = "https://hiveblockexplorer.com/tx/{trx_id}"
    HiveExplorer = "https://hivexplorer.com/tx/{trx_id}"


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


if __name__ == "__main__":
    nodes = get_good_nodes()
    print(nodes)
    witness = get_hive_witness_details("brianoflondon")
    print(witness)
