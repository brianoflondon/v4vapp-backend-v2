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


# Base class for all accounts
class Account(BaseModel):
    name: str = Field(..., description="Name of the ledger account")
    account_type: AccountType = Field(..., description="Type of account")

    model_config = ConfigDict(use_enum_values=True)


# Asset Accounts
class AssetAccount(Account):
    """
    Represents an asset account in the accounting system.
    """

    name: Literal[
        "Cash (Fiat)",
        "Bitcoin Wallet",
        "Ethereum Wallet",
        "Binance Hive Wallet",
        "V4VApp Treasury",
        "V4VApp DHF",
        "Customer Bitcoin Deposits",
        "Customer Ethereum Deposits",
        "Accounts Receivable",
        "Prepaid Expenses",
        "Fixed Assets",
        "Intangible Assets",
    ] = Field(..., description="Specific asset account name")
    account_type: Literal[AccountType.ASSET] = Field(
        AccountType.ASSET, description="Type of account"
    )


# Liability Accounts
class LiabilityAccount(Account):
    name: Literal[
        "Customer Bitcoin Liability",
        "Customer Ethereum Liability",
        "Accounts Payable",
        "Accrued Expenses",
        "Loans Payable",
        "Tax Liabilities",
    ] = Field(..., description="Specific liability account name")
    account_type: Literal[AccountType.LIABILITY] = Field(
        AccountType.LIABILITY, description="Type of account"
    )


# Equity Accounts
class EquityAccount(Account):
    name: Literal["Owner's Capital", "Retained Earnings", "Dividends/Distributions"] = Field(
        ..., description="Specific equity account name"
    )
    account_type: Literal[AccountType.EQUITY] = Field(
        AccountType.EQUITY, description="Type of account"
    )


# Revenue Accounts
class RevenueAccount(Account):
    name: Literal["Hive Fees", "HBD Fees", "Sats Fees", "DHF Income", "Other Income"] = Field(
        ..., description="Specific revenue account name"
    )
    account_type: Literal[AccountType.REVENUE] = Field(
        AccountType.REVENUE, description="Type of account"
    )


# Expense Accounts
class ExpenseAccount(Account):
    name: Literal[
        "Hosting Expenses Privex",
        "Hosting Expenses Voltage",
        "Hosting Expenses Other",
        "Transaction Fees (Outgoing)",
        "Salaries and Wages",
        "Rent and Utilities",
        "Software Subscriptions",
        "Marketing Expenses",
        "Professional Fees",
        "Insurance",
        "Tax Expenses",
        "Depreciation/Amortization",
        "Miscellaneous Expenses",
    ] = Field(..., description="Specific expense account name")
    account_type: Literal[AccountType.EXPENSE] = Field(
        AccountType.EXPENSE, description="Type of account"
    )


AccountAny = Union[AssetAccount, LiabilityAccount, EquityAccount, RevenueAccount, ExpenseAccount]
