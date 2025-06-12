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
    CONTRA_ASSET = "Contra Asset"


# MARK: Base class for all accounts
class Account(BaseModel):
    name: str = Field(..., description="Name of the ledger account")
    account_type: AccountType = Field(..., description="Type of account")
    sub: str = Field("", description="Sub-account name for more specific categorization")

    model_config = ConfigDict(use_enum_values=True)

    def __repr__(self) -> str:
        return f"{self.name} ({self.account_type}) - Sub: {self.sub}"

    def __str__(self) -> str:
        return f"{self.name} ({self.account_type}) - Sub: {self.sub}"

    def __eq__(self, other):
        if not isinstance(other, Account):
            return NotImplemented
        return (
            self.name == other.name
            and self.account_type == other.account_type
            and self.sub == other.sub
        )

    def __hash__(self):
        return hash((self.name, self.account_type, self.sub))


# MARK: Asset Accounts
class AssetAccount(Account):
    """
    Represents an asset account in the accounting system.
    Assets INCREASE with a DEBIT and DECREASE with a CREDIT.
    Attributes:
        name (Literal): The specific name of the asset account. Must be one of:
            - "Customer Deposits Hive"
            - "Customer Deposits Lightning"
            - "Treasury Hive"
            - "Treasury Lightning"
        account_type (Literal[AccountType.ASSET]): The type of account, which is always set to `AccountType.ASSET`.
    """

    name: Literal[
        "Customer Deposits Hive",
        "Customer Deposits Lightning",
        "Escrow Hive",
        "Treasury Hive",
        "Treasury Lightning",
        "Exchange Deposits Hive",
        "Exchange Deposits Lightning",
    ] = Field(..., description="Specific asset account name")
    account_type: Literal[AccountType.ASSET] = Field(
        AccountType.ASSET, description="Type of account"
    )

    def __init__(self, name="", sub="", account_type: AccountType = AccountType.ASSET):
        super().__init__(name=name, sub=sub, account_type=account_type)
        self.account_type = AccountType.ASSET
        self.name = name
        self.sub = sub


# MARK: Contra Asset Accounts
class ContraAssetAccount(Account):
    """
    Represents a contra asset account (e.g., 'Converted Assets Out').
    Contra assets DECREASE total assets and INCREASE with a CREDIT.
    """

    name: Literal[
        "Converted Hive Offset",
        "External Lightning Payments",
        # Add more as needed
    ] = Field(..., description="Specific contra asset account name")
    account_type: Literal[AccountType.CONTRA_ASSET] = Field(
        AccountType.CONTRA_ASSET, description="Type of account"
    )

    def __init__(self, name="", sub="", account_type: AccountType = AccountType.CONTRA_ASSET):
        super().__init__(name=name, sub=sub, account_type=account_type)
        self.account_type = AccountType.CONTRA_ASSET
        self.name = name
        self.sub = sub


# MARK: Liability Accounts
class LiabilityAccount(Account):
    """
    LiabilityAccount is a subclass of Account that represents a specific type of liability account.
    Liabilities INCREASE with a CREDIT and DECREASE with a DEBIT.
    Attributes:
        name (Literal): The specific name of the liability account. Must be one of:
            - "Customer Liability Hive"
            - "Customer Liability Lightning"
            - "Tax Liabilities"
        account_type (Literal[AccountType.LIABILITY]): The type of account, which is always set to `AccountType.LIABILITY`.
    Methods:
        __init__(name: str = "", sub: str = ""):
            Initializes a LiabilityAccount instance with the specified name and sub-account.
            Overrides the account_type to `AccountType.LIABILITY`.
    """

    name: Literal[
        "Customer Liability Hive",
        "Customer Liability Lightning",
        "Lightning Payment Clearing",
        "Owner Loan Payable (funding)",
        "Tax Liabilities",
    ] = Field(..., description="Specific liability account name")
    account_type: Literal[AccountType.LIABILITY] = Field(
        AccountType.LIABILITY, description="Type of account"
    )

    def __init__(self, name="", sub="", account_type: AccountType = AccountType.LIABILITY):
        super().__init__(name=name, sub=sub, account_type=account_type)
        self.account_type = AccountType.LIABILITY
        self.name = name
        self.sub = sub


# MARK: Equity Accounts
class EquityAccount(Account):
    """
    Represents an equity account in the accounting system.
    Equity accounts INCREASE with a CREDIT and DECREASE with a DEBIT.
    Attributes:
        name (Literal): The specific name of the equity account. Must be one of:
            - "Owner's Capital"
            - "Retained Earnings"
            - "Dividends/Distributions"
        account_type (Literal[AccountType.EQUITY]): The type of account, which is always set to `AccountType.EQUITY`.
    Methods:
        __init__(name: str = "", sub: str = ""):
            Initializes an EquityAccount instance with the specified name and sub-account.
            Overrides the account_type to `AccountType.EQUITY`.
    """

    name: Literal["Owner's Capital", "Retained Earnings", "Dividends/Distributions"] = Field(
        ..., description="Specific equity account name"
    )
    account_type: Literal[AccountType.EQUITY] = Field(
        AccountType.EQUITY, description="Type of account"
    )

    def __init__(self, name="", sub="", account_type: AccountType = AccountType.EQUITY):
        super().__init__(name=name, sub=sub, account_type=account_type)
        self.account_type = AccountType.EQUITY
        self.name = name
        self.sub = sub


# MARK: Revenue Accounts
class RevenueAccount(Account):
    name: Literal["Fee Income Hive", "Fee Income Lightning", "DHF Income", "Other Income"] = Field(
        ..., description="Specific revenue account name"
    )
    account_type: Literal[AccountType.REVENUE] = Field(
        AccountType.REVENUE, description="Type of account"
    )

    def __init__(self, name="", sub="", account_type: AccountType = AccountType.REVENUE):
        super().__init__(name=name, sub=sub, account_type=account_type)
        self.account_type = AccountType.REVENUE
        self.name = name
        self.sub = sub


# MARK: Expense Accounts
class ExpenseAccount(Account):
    name: Literal[
        "Hosting Expenses Privex",
        "Hosting Expenses Voltage",
        "Fee Expenses Lightning",
        "Fee Expenses Hive",
    ] = Field(..., description="Specific expense account name")
    account_type: Literal[AccountType.EXPENSE] = Field(
        AccountType.EXPENSE, description="Type of account"
    )

    def __init__(self, name="", sub="", account_type: AccountType = AccountType.EXPENSE):
        super().__init__(name=name, sub=sub, account_type=account_type)
        self.account_type = AccountType.EXPENSE
        self.name = name
        self.sub = sub


AccountAny = Union[
    AssetAccount,
    ContraAssetAccount,
    LiabilityAccount,
    EquityAccount,
    RevenueAccount,
    ExpenseAccount,
]


if __name__ == "__main__":
    # Example usage
    asset_account = AssetAccount(name="Customer Deposits Hive", sub="v4vapp")
    liability_account = LiabilityAccount(name="Customer Liability Hive", sub="Sub-account 2")
    equity_account = EquityAccount(name="Owner's Capital", sub="Sub-account 3")
    revenue_account = RevenueAccount(name="Fee Income", sub="Sub-account 4")
    expense_account = ExpenseAccount(name="Hosting Expenses Privex", sub="Sub-account 5")

    print(asset_account)
    print(liability_account)
    print(equity_account)
    print(revenue_account)
    print(expense_account)
