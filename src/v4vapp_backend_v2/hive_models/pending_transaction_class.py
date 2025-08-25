import time
from typing import Dict, Optional

from bson import ObjectId
from nectar.amount import Amount
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_retry import mongo_call
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType


class PendingTransaction(BaseModel):
    id: Optional[ObjectId] = Field(default=None, alias="_id")
    timestamp: float = Field(default_factory=time.time)
    from_account: AccNameType
    to_account: AccNameType
    amount: Amount
    memo: str
    nobroadcast: bool
    is_private: bool

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    @field_serializer("id")
    def serialize_objectid(self, value):
        return str(value) if value is not None else None

    @field_serializer("amount")
    def serialize_amount(self, value: Amount):
        return str(value)

    @field_validator("amount", mode="before")
    def parse_amount(cls, v):
        if isinstance(v, Amount):
            return v
        if isinstance(v, str):
            try:
                return Amount(v)
            except ValueError:
                raise TypeError(
                    "amount must be Amount or str which converts to Amount eg 12.020 HIVE"
                )
        raise TypeError("amount must be Amount or str")

    @classmethod
    def collection_name(cls) -> str:
        return "pending"

    @classmethod
    def collection(cls) -> AsyncCollection:
        return InternalConfig.db["pending"]

    @classmethod
    async def list_all(cls) -> list["PendingTransaction"]:
        all_pending = await InternalConfig.db["pending"].find({}).to_list(length=None)
        return [cls(**doc) for doc in all_pending]

    @classmethod
    async def total_pending(cls) -> Dict[str, Amount]:
        all_pending = await cls.list_all()
        totals: Dict[str, Amount] = {}
        for pending in all_pending:
            assert isinstance(pending, PendingTransaction)
            if pending.amount.symbol == "HIVE":
                totals["HIVE"] = totals.get("HIVE", Amount("0.000 HIVE")) + pending.amount
            elif pending.amount.symbol == "HBD":
                totals["HBD"] = totals.get("HBD", Amount("0.000 HBD")) + pending.amount
        return totals

    def __str__(self) -> str:
        """
        Returns a string representation of the PendingTransaction instance, including
        its id, source and destination accounts, amount, and memo.
        """
        return f"PendingTransaction({self.id}, {self.from_account} -> {self.to_account}, {self.amount}, {self.memo})"

    async def save(self) -> "PendingTransaction":
        result = await mongo_call(
            lambda: InternalConfig.db["pending"].insert_one(self.model_dump(exclude={"id"})),
            error_code="db_save_error_pending",
            context=f"pending:{self.timestamp} {self.from_account} -> {self.to_account} (amount: {self.amount})",
        )
        self.id = result.inserted_id
        return self

    async def delete(self):
        if not self.id:
            raise ValueError("Cannot delete PendingTransaction without an ID")
        return await mongo_call(
            lambda: InternalConfig.db["pending"].delete_one({"_id": self.id}),
            error_code="db_delete_error_pending",
            context=f"pending:{self.timestamp} {self.from_account} -> {self.to_account} (amount: {self.amount})",
        )


# Last line
