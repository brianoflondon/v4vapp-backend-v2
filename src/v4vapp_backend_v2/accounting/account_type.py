from enum import StrEnum
from typing import Literal, Union

from pydantic import BaseModel, ConfigDict, Field


# Define Account Types as a StrEnum for validation
class AccountType(StrEnum):
    ASSET = "Asset"
    LIABILITY = "Liability"
    EQUITY = "Equity"
    REVENUE = "Revenue"
    EXPENSE = "Expense"


# MARK: Base class for all accounts
class Account(BaseModel):
    name: str = Field(..., description="Name of the ledger account")
    account_type: AccountType = Field(..., description="Type of account")
    sub: str = Field("", description="Sub-account name for more specific categorization")

    model_config = ConfigDict(use_enum_values=True)


# MARK: Asset Accounts
class AssetAccount(Account):
    """
    Represents an asset account in the accounting system.
    """

    name: Literal["Customer Hive Deposits"] = Field(..., description="Specific asset account name")
    account_type: Literal[AccountType.ASSET] = Field(
        AccountType.ASSET, description="Type of account"
    )

    def __init__(self, name="", sub=""):
        super().__init__(name=name, sub=sub)
        self.account_type = AccountType.ASSET
        self.name = name
        self.sub = sub


# MARK: Liability Accounts
class LiabilityAccount(Account):
    name: Literal[
        "Customer Lightning Liability",
        "Customer Hive Liability",
        "Tax Liabilities",
    ] = Field(..., description="Specific liability account name")
    account_type: Literal[AccountType.LIABILITY] = Field(
        AccountType.LIABILITY, description="Type of account"
    )

    def __init__(self, name="", sub=""):
        super().__init__(name=name, sub=sub)
        self.account_type = AccountType.LIABILITY
        self.name = name
        self.sub = sub


# MARK: Equity Accounts
class EquityAccount(Account):
    name: Literal["Owner's Capital", "Retained Earnings", "Dividends/Distributions"] = Field(
        ..., description="Specific equity account name"
    )
    account_type: Literal[AccountType.EQUITY] = Field(
        AccountType.EQUITY, description="Type of account"
    )

    def __init__(self, name="", sub=""):
        super().__init__(name=name, sub=sub)
        self.account_type = AccountType.EQUITY
        self.name = name
        self.sub = sub


# MARK: Revenue Accounts
class RevenueAccount(Account):
    name: Literal["Hive Fees", "HBD Fees", "Sats Fees", "DHF Income", "Other Income"] = Field(
        ..., description="Specific revenue account name"
    )
    account_type: Literal[AccountType.REVENUE] = Field(
        AccountType.REVENUE, description="Type of account"
    )

    def __init__(self, name="", sub=""):
        super().__init__(name=name, sub=sub)
        self.account_type = AccountType.REVENUE
        self.name = name
        self.sub = sub


# MARK: Expense Accounts
class ExpenseAccount(Account):
    name: Literal[
        "Hosting Expenses Privex",
        "Hosting Expenses Voltage",
    ] = Field(..., description="Specific expense account name")
    account_type: Literal[AccountType.EXPENSE] = Field(
        AccountType.EXPENSE, description="Type of account"
    )

    def __init__(self, name="", sub=""):
        super().__init__(name=name, sub=sub)
        self.account_type = AccountType.EXPENSE
        self.name = name
        self.sub = sub


AccountAny = Union[AssetAccount, LiabilityAccount, EquityAccount, RevenueAccount, ExpenseAccount]
