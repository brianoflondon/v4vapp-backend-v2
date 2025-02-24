import json
import os
from typing import List

import httpx
from beem import Hive
from beem.blockchain import Blockchain

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.sync_async import AsyncConverter

BASE_MAIN_NODES: List[str] = [
    "https://rpc.podping.org",
    "https://anyx.io",
    "https://api.hive.blog/",
    "https://api.deathwing.me/",
    "https://hive-api.arcange.eu",
    "https://api.openhive.network",
    "https://hived.emere.sh",
    "https://techcoderx.com",
]

# os.environ["http_proxy"] = "http://home-imac.tail400e5.ts.net:8888"


class HiveClient:
    def __init__(self):
        self.config = InternalConfig().config
        self.check_beacon()
        logger.info("Initializing HiveClient")
        self.hive = Hive(node=self.good_nodes)
        self.blockchain = Blockchain(
            hive=self.hive,
            mode="head",
        )

    def check_beacon(self) -> None:
        if hasattr(self, "good_nodes"):
            return
        try:
            response = httpx.get(
                "https://beacon.peakd.com/api/nodes",
                headers={
                    "Accept": "application/json",
                    "User-Agent": "httpx/0.21.1",
                    "Connection": "keep-alive",
                },
            )
            nodes = response.json()
            self.good_nodes = [
                node.get("endpoint") for node in nodes if node.get("score", 0) == 100
            ]
            # add rpc.podping.org to the start of the list
            self.good_nodes.insert(0, "https://rpc.podping.org")
            logger.info(f"Beacon nodes: {self.good_nodes}")
            if not self.good_nodes:
                self.good_nodes = BASE_MAIN_NODES
        except Exception as e:
            logger.error(f"Failed to get beacon nodes: {e}")
            self.good_nodes = BASE_MAIN_NODES
