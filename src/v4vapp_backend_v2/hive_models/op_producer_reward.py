from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class VestingShares(BaseModel):
    amount: str = Field(description="The amount as a string representation")
    nai: str = Field(description="Network Asset Identifier")
    precision: int = Field(description="Decimal precision for the amount")

    @property
    def decimal_amount(self) -> float:
        """Convert string amount to decimal with proper precision"""
        return float(self.amount) / (10**self.precision)


class ProducerReward(BaseModel):
    id: str = Field(description="Unique identifier for the reward", alias="_id")
    type: str = Field(description="Type of the event")
    producer: str = Field(description="Producer of the reward")
    vesting_shares: VestingShares = Field(description="Vesting shares awarded")
    timestamp: datetime = Field(description="Timestamp of the reward")
    block_num: int = Field(description="Block number containing this transaction")
    trx_num: int = Field(description="Transaction number within the block")
    trx_id: str = Field(description="Transaction ID")
    op_in_trx: int = Field(description="Operation index in the transaction")

    model_config = ConfigDict(populate_by_name=True)


# Example usage
if __name__ == "__main__":
    data = {
        "type": "producer_reward",
        "producer": "stoodkev",
        "vesting_shares": {"amount": "494441761", "nai": "@@000000037", "precision": 6},
        "_id": "06e50bcafe3adbdeb3895bde10e1d1f99fb2b418",
        "timestamp": "2025-03-22 12:18:48+00:00",
        "block_num": 94358096,
        "trx_num": 0,
        "trx_id": "0000000000000000000000000000000000000000",
        "op_in_trx": 39,
    }

    producer_reward = ProducerReward(**data)
    print(producer_reward)
    print(f"Decimal vesting shares: {producer_reward.vesting_shares.decimal_amount}")
    print(f"Formatted timestamp: {producer_reward.formatted_timestamp}")
