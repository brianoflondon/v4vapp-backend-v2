from beem import Hive
from beem.blockchain import Blockchain
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.sync_async import AsyncConverter


class HiveClient:
    def __init__(self):
        logger.info("Initializing HiveClient")
        self.hive = Hive()
        self.blockchain = Blockchain(hive=self.hive, mode="head")
