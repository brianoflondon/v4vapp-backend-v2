import time
from typing import Optional

from bson import ObjectId
from nectar.amount import Amount
from pydantic import BaseModel, ConfigDict, Field, field_serializer
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
    no_broadcast: bool
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

    def __str__(self) -> str:
        return f"PendingTransaction({self.id}, {self.from_account} -> {self.to_account}, {self.amount}, {self.memo})"

    def key(self) -> str:
        return f"{self.timestamp}:{self.from_account}:{self.to_account}:{self.amount}"

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
# Last line
