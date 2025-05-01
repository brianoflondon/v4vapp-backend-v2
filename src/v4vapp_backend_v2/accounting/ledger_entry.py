from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.accounting.account_type import AccountAny, AssetAccount, LiabilityAccount
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.op_all import OpAny


class LedgerEntry(BaseModel):
    """
    Represents a ledger entry in the accounting system.
    """

    group_id: str = Field("", description="Group ID for the ledger entry")
    timestamp: datetime = Field(None, description="Timestamp of the ledger entry")
    description: str = Field("", description="Description of the ledger entry")
    amount: float = Field(0.0, description="Amount of the ledger entry")
    unit: Currency = Field(None, description="Unit of the ledger entry")
    conv: CryptoConv = Field(None, description="Conversion details for the ledger entry")
    debit: AccountAny = Field(None, description="Account to be debited")
    credit: AccountAny = Field(None, description="Account to be credited")
    op: OpAny = Field(None, description="Associated Hive operation")

    model_config = ConfigDict()

    def __init__(self, **data):
        super().__init__(**data)

    def customer_deposit(self, hive_op: OpAny = None, server_account: str = None) -> None:
        """
        Set the customer deposit account based on the Hive operation.
        """

        if hive_op and server_account:
            if hive_op.to_account == server_account:
                debit = AssetAccount(
                    name="Customer Deposits Hive",
                    sub=hive_op.to_account,
                )
                credit = LiabilityAccount(
                    name="Customer Liability Hive",
                    sub=hive_op.from_account,
                )
            elif hive_op.from_account == server_account:
                debit = LiabilityAccount(
                    name="Customer Liability Hive",
                    sub=hive_op.to_account,
                )
                credit = AssetAccount(
                    name="Customer Deposits Hive",
                    sub=hive_op.from_account,
                )
            self.group_id = hive_op.group_id
            self.timestamp = hive_op.timestamp
            self.description = hive_op.d_memo
            self.unit = hive_op.unit
            self.amount = hive_op.amount_decimal
            self.conv = hive_op.conv
            self.debit = debit
            self.credit = credit
            self.op = hive_op
        else:
            raise ValueError("Either hive_op or server_account must be provided")
