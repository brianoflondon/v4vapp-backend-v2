from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.accounting.account_type import AccountAny, AccountType, AssetAccount, LiabilityAccount
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.crypto_prices import Currency


class LedgerEntry(BaseModel):
    """
    Represents a ledger entry in the accounting system.
    """
    group_id: str = Field(..., description="Group ID for the ledger entry")
    timestamp: datetime = Field(..., description="Timestamp of the ledger entry")
    description: str = Field(..., description="Description of the ledger entry")
    amount: float = Field(..., description="Amount of the ledger entry")
    unit: Currency = Field(..., description="Unit of the ledger entry")
    debit_account: AccountAny = Field(..., description="Account to be debited")
    credit_account: AccountAny = Field(..., description="Account to be credited")

    model_config = ConfigDict()


    def __init__(self, **data):
        super().__init__(**data)
        self.timestamp = datetime.now(tz=timezone.utc)


if __name__ == "__main__":
    journal_entry = LedgerEntry(
        group_id="example_group",
        timestamp=datetime.now(tz=timezone.utc),
        description="Example transaction",
        amount=100.0,
        unit=Currency.HIVE,
        debit_account=AssetAccount(name="Customer Hive Deposits", sub="brianoflondon"),
        credit_account=LiabilityAccount(name="Customer Hive Liability", sub="v4vapp"),
    )


    print(journal_entry)  # Example usage


    # def add_entry(self, db_client: MongoDBClient) -> None:
    #     """
    #     Adds the ledger entry to the database.

    #     Args:
    #         db_client (MongoDbClient): The database client to use for adding the entry.
    #     """
    #     async with db_client:
    #         await db_client.add_ledger_entry(
    #             group_id=self.group_id,
    #             timestamp=self.timestamp,
    #             description=self.description,
    #             amount=self.amount,
    #             unit=self.unit,
    #             debit_account=self.debit_account,
    #             credit_account=self.credit_account
    #         )