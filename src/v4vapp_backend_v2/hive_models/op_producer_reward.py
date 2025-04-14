from datetime import datetime, timedelta

from pydantic import ConfigDict, Field

from v4vapp_backend_v2.hive.witness_details import get_hive_witness_details
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.witness_details import Witness


class VestingShares(AmountPyd):
    pass


class ProducerRewardRaw(OpBase):
    producer: AccNameType = Field(description="Producer of the reward")
    vesting_shares: VestingShares = Field(description="Vesting shares awarded")
    timestamp: datetime = Field(description="Timestamp of the reward")

    model_config = ConfigDict(populate_by_name=True)


class ProducerReward(ProducerRewardRaw):
    witness: Witness | None = Field(None, description="Witness details")
    delta: timedelta | None = Field(None, description="Time delta to the last produced block")
    mean: timedelta | None = Field(None, description="Mean time between the last (n) blocks")

    def __init__(self, **data):
        super().__init__(**data)

    def log_common(self):
        return f"🧱 Delta {self.delta} | Mean {self.mean} | "

    @property
    def log_str(self):
        if self.witness:
            log_str = (
                f"{self.log_common()}"
                f"{self.block_num:,} | {self.age:.2f} | "
                f"Missed: {self.witness.missed_blocks} | "
                f"Rank: {self.witness.rank} | {self.producer.link}"
            )
            return log_str
        return f"{self.block_num:,} | {self.age:.2f} | {self.producer.link} | {self.link}"

    @property
    def notification_str(self):
        if self.witness:
            log_str = (
                f"{self.log_common()}"
                f"{self.block_num:,} | {self.age:.2f} | "
                f"Missed: {self.witness.missed_blocks} | "
                f"Rank: {self.witness.rank} | {self.producer.markdown_link}"
            )
            return log_str
        return f"{self.block_num:,} | {self.age:.2f} | {self.producer.link} | {self.link}"

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
