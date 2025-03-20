from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from v4vapp_backend_v2.models.hive_currency_model import HiveCurrency


class HiveFillOrder(BaseModel):
    type: Literal["fill_order"] = Field(description="Event type")
    open_pays: HiveCurrency = Field(description="Payment from the open order")
    open_owner: str = Field(description="Account that created the open order")
    current_pays: HiveCurrency = Field(description="Payment from the current order")
    open_orderid: int = Field(description="ID of the open order")
    current_owner: str = Field(description="Account that created the current order")
    current_orderid: int = Field(description="ID of the current order")
    _id: str = Field(description="Unique identifier for this event")
    timestamp: datetime = Field(description="Time when the order was filled")
    block_num: int = Field(description="Block number containing this transaction")
    trx_num: int = Field(description="Transaction number within the block")
    trx_id: str = Field(description="Transaction ID")

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, value):
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace(" ", "T"))
        return value

    @property
    def hive_amount(self) -> float:
        """Get HIVE amount, regardless of which side of the trade it was on"""
        if self.open_pays.nai == "@@000000021":
            return self.open_pays.decimal_amount
        return self.current_pays.decimal_amount

    @property
    def hbd_amount(self) -> float:
        """Get HBD amount, regardless of which side of the trade it was on"""
        if self.open_pays.nai == "@@000000013":
            return self.open_pays.decimal_amount
        return self.current_pays.decimal_amount

    @property
    def rate(self) -> float:
        """Calculate the exchange rate (HIVE/HBD)"""
        if self.open_pays.nai == "@@000000021":
            return self.open_pays.decimal_amount / self.current_pays.decimal_amount
        return self.current_pays.decimal_amount / self.open_pays.decimal_amount


class HiveEvent(BaseModel):
    hive_event: HiveFillOrder
