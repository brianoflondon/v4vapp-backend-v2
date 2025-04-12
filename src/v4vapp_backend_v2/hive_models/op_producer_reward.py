from datetime import datetime

from pydantic import ConfigDict, Field

from v4vapp_backend_v2.hive.witness_details import get_hive_witness_details
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.witness_details import Witness


class VestingShares(AmountPyd):
    pass


class ProducerRewardRaw(OpBase):
    producer: str = Field(description="Producer of the reward")
    vesting_shares: VestingShares = Field(description="Vesting shares awarded")
    timestamp: datetime = Field(description="Timestamp of the reward")

    model_config = ConfigDict(populate_by_name=True)


class ProducerReward(ProducerRewardRaw):
    witness: Witness | None = Field(None, description="Witness details")

    def __init__(self, **data):
        super().__init__(**data)

    @property
    def log_str(self):
        if self.witness:
            log_str = (
                f"{self.block_num:,} | {self.age:.2f} | "
                f"Missed: {self.witness.missed_blocks} | "
                f"Rank: {self.witness.rank} | {self.producer}"
            )
            return log_str
        return f"{self.block_num:,} | {self.age:.2f} | {self.producer} | {self.link}"

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
