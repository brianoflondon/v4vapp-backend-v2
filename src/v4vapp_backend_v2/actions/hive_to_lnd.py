from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.op_all import OpAllTransfers


class HiveOperation:
    def __init__(self, op: OpAllTransfers):
        self.op = op

        # Business logic to determine the type of operation.


class InternalHiveTransfer(HiveOperation):
    pass


class HiveToLnd:
    """
    Class to handle the conversion of Hive data to LND format.
    """

    def __init__(self, op: OpAllTransfers, server_account: str = None):
        """
        Initialize the HiveToLnd class with hive data.

        :param hive_data: The data from Hive to be converted.
        :param server_account: The server account associated with the conversion.
        """
        self.op = op
        self.hive_config = InternalConfig().config.hive
        self.server_account = server_account or self.hive_config.server_account
        self.hive_inst = op.hive_inst
        self.lnd_data = None

    def is_hive_to_lnd(self) -> bool:
        """
        Check if the operation is a transfer from Hive to LND.

        :return: True if the operation is a transfer from Hive to LND, False otherwise.
        """
        return True

    async def process(self):
        """
        Convert the Hive data to LND format.

        :return: Converted LND data.
        """
        # Conversion logic goes here
        if not self.op:
            logger.error("No data to process")
            return None
        if not self.is_hive_to_lnd():
            logger.error("Not a Hive to LND transfer")
            return None
