from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


class VestingShares(AmountPyd):
    pass
    # amount: str = Field(description="The amount as a string representation")
    # nai: str = Field(description="Network Asset Identifier")
    # precision: int = Field(description="Decimal precision for the amount")

    # @property
    # def decimal_amount(self) -> float:
    #     """Convert string amount to decimal with proper precision"""
    #     return float(self.amount) / (10**self.precision)


class ProducerReward(BaseModel):
    type: str = Field(description="Type of the event")
    producer: str = Field(description="Producer of the reward")
    vesting_shares: VestingShares = Field(description="Vesting shares awarded")
    timestamp: datetime = Field(description="Timestamp of the reward")
    block_num: int = Field(description="Block number containing this transaction")
    trx_num: int = Field(description="Transaction number within the block")
    trx_id: str = Field(description="Transaction ID")
    op_in_trx: int = Field(description="Operation index in the transaction")

    model_config = ConfigDict(populate_by_name=True)
