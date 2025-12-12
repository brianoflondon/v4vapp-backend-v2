import time
from typing import Any, Dict, Self, TypeVar

from bson import ObjectId
from nectar.amount import Amount
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_retry import mongo_call
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType

# TypeVar for generics: T is bound to PendingBase or its subclasses
T = TypeVar("T", bound="PendingBase")


class PendingBase(BaseModel):
    id: ObjectId | None = Field(default=None, alias="_id")
    timestamp: float = Field(default_factory=time.time)
    nobroadcast: bool = False
    pending_type: str = Field(
        default="pending_base"
    )  # Renamed from 'type' to avoid keyword conflict
    resend_attempt: int = 0  # Track number of resend attempts

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    @field_serializer("id")
    def serialize_objectid(self, value):
        return str(value) if value is not None else None

    @classmethod
    def collection_name(cls) -> str:
        return "pending"

    @classmethod
    def collection(cls) -> AsyncCollection:
        return InternalConfig.db["pending"]

    @classmethod
    def get_pending_type(cls) -> str:
        # Get the default value from the model's field definition
        return cls.model_fields["pending_type"].default

    @classmethod
    async def list_all(cls: type[T]) -> list[T]:
        # Filter by pending_type to ensure we only instantiate the correct subclass
        all_pending = (
            await InternalConfig.db["pending"]
            .find({"pending_type": cls.get_pending_type()})
            .to_list(length=None)
        )
        return [cls(**doc) for doc in all_pending]

    def __str__(self) -> str:
        """
        Returns a string representation of the PendingBase instance.
        Subclasses can override for more specific details.
        """
        return f"{self.__class__.__name__}({self.id}, timestamp: {self.timestamp})"

    async def save(self) -> Self:
        result = await mongo_call(
            lambda: InternalConfig.db["pending"].insert_one(self.model_dump(exclude={"id"})),
            error_code="db_save_error_pending",
            context=f"pending:{self.timestamp} ({self.pending_type})",  # Updated reference
        )
        self.id = result.inserted_id
        return self

    async def delete(self):
        if not self.id:
            raise ValueError("Cannot delete PendingBase without an ID")
        return await mongo_call(
            lambda: InternalConfig.db["pending"].delete_one({"_id": self.id}),
            error_code="db_delete_error_pending",
            context=f"pending:{self.timestamp} ({self.pending_type})",  # Updated reference
        )


class PendingCustomJson(PendingBase):
    cj_id: str = "v4vapp_transfer"
    send_account: AccNameType = ""
    json_data: Dict[str, Any] | None = None
    active: bool = True
    pending_type: str = "pending_custom_json"  # Added type annotation to satisfy Pydantic

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    def __str__(self) -> str:
        """
        Returns a string representation of the PendingCustomJson instance.
        """
        return f"PendingCustomJson({self.id}, send_account: {self.send_account}, active: {self.active}, cj_id: {self.cj_id})"


class PendingTransaction(PendingBase):
    from_account: AccNameType
    to_account: AccNameType
    amount: Amount
    memo: str
    is_private: bool = False
    value: float = 0.0
    symbol: str = "HIVE"
    currency: Currency = Currency.HIVE
    pending_type: str = "pending_transaction"  # Added type annotation to satisfy Pydantic

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    def __init__(self, **data):
        super().__init__(**data)
        self.value = self.amount.amount if self.amount else 0.0
        self.symbol = self.amount.symbol if self.amount else "HIVE"
        self.currency = Currency(self.symbol.lower())

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
    def name(cls) -> str:
        """
        Returns the name of the class in snake_case format.
        """
        return "pending_transaction"

    @classmethod
    async def list_all_hbd(cls) -> list["PendingTransaction"]:
        all_pending = (
            await InternalConfig.db["pending"]
            .find(
                {"currency": Currency.HBD, "pending_type": cls.get_pending_type()}
            )  # Updated to use get_pending_type
            .to_list(length=None)
        )
        return [cls(**doc) for doc in all_pending]

    @classmethod
    async def list_all_hive(cls) -> list["PendingTransaction"]:
        all_pending = (
            await InternalConfig.db["pending"]
            .find(
                {"currency": Currency.HIVE, "pending_type": cls.get_pending_type()}
            )  # Updated to use get_pending_type
            .to_list(length=None)
        )
        return [cls(**doc) for doc in all_pending]

    @classmethod
    async def list_all_str(cls) -> list[str]:
        all_pending = await cls.list_all()
        return [str(pending) for pending in all_pending]

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

    @classmethod
    async def find(cls, **kwargs) -> "PendingTransaction | None":
        data = await InternalConfig.db["pending"].find_one(
            {**kwargs, "pending_type": cls.get_pending_type()}
        )  # Updated to use get_pending_type
        if data:
            return cls.model_validate(data)
        return None

    def __str__(self) -> str:
        """
        Returns a string representation of the PendingTransaction instance, including
        its id, source and destination accounts, amount, and memo.
        """
        return f"{self.from_account} -> {self.to_account}, {self.amount}, {self.memo}"

    def __repr__(self) -> str:
        """
        Returns a string representation of the PendingTransaction instance, including
        its id, source and destination accounts, amount, and memo.
        """
        return f"PendingTransaction({self.id}, {self.from_account} -> {self.to_account}, {self.amount}, {self.memo})"

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {self.name(): self.model_dump()}


# Last line
