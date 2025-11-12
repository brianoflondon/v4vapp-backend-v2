from pydantic import ConfigDict, Field

from v4vapp_backend_v2.hive.witness_details import get_hive_witness_details
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.witness_details import Witness


class VestingShares(AmountPyd):
    pass


class ProducerMissedRaw(OpBase):
    producer: AccNameType = Field(description="Producer which missed the block")

    model_config = ConfigDict(populate_by_name=True)

    @property
    def log_str(self):
        log_str = f"{self.producer:<17} Missed block {self.block_num:,} {self.age_str}"
        return log_str

    @property
    def notification_str(self):
        notification_str = f"**{self.producer} Missed block** {self.block_num:,} {self.age_str}"
        return notification_str


class ProducerMissed(ProducerMissedRaw):
    witness: Witness | None = Field(None, description="Witness details")
    missing_key: str | None = Field(None, description="Signing key that missed the block")

    @property
    def log_str(self):
        log_str = f"{self.producer:<17} Missed block {self.block_num:,} {self.age_str} | Key: {self.missing_key}"
        return log_str

    @property
    def notification_str(self):
        notification_str = f"*{self.producer} Missed block* {self.block_num:,} {self.age_str} | Key: {self.missing_key}"
        return notification_str

    async def get_witness_details(self):
        """
        Asynchronously retrieves and sets the witness details for the producer.

        This method checks if the producer attribute is set. If it is, it fetches
        the witness details for the producer using the `get_hive_witness_details`
        function. If witness details are found, it sets the `witness` attribute
        with the retrieved witness information.

        Returns:
            None
        """

        if self.producer:
            witness_details = await get_hive_witness_details(self.producer)
            if witness_details:
                self.witness = witness_details.witness
                if self.witness:
                    self.missing_key = self.witness.signing_key
