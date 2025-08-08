import re
from enum import StrEnum
from typing import Literal, Union

from pydantic import BaseModel, Field


# Define Account Types as a StrEnum for validation
class AccountType(StrEnum):
    DIVIDEND = "Dividend"
    EXPENSE = "Expense"
    ASSET = "Asset"
    LIABILITY = "Liability"
    EQUITY = "Equity"
    REVENUE = "Revenue"


NORMAL_DEBIT_ACCOUNTS = [
    AccountType.DIVIDEND,
    AccountType.EXPENSE,
    AccountType.ASSET,
]
NORMAL_CREDIT_ACCOUNTS = [AccountType.LIABILITY, AccountType.EQUITY, AccountType.REVENUE]


# MARK: Base class for all accounts
class LedgerAccount(BaseModel):
    name: str = Field(..., description="Name of the ledger account")
    account_type: AccountType = Field(..., description="Type of account")
    sub: str = Field("", description="Sub-account name for more specific categorization")
    contra: bool = Field(
        False, description="Indicates if this is a contra account (default: False) Contra"
    )

    # model_config = ConfigDict(use_enum_values=True)

    def __repr__(self) -> str:
        contra_str = " (Contra)" if self.contra else ""
        return f"{self.name} ({self.account_type}) - Sub: {self.sub}{contra_str}"

    def __str__(self) -> str:
        contra_str = " (Contra)" if self.contra else ""
        return f"{self.name} ({self.account_type}) - Sub: {self.sub}{contra_str}"

    def __eq__(self, other):
        if not isinstance(other, LedgerAccount):
            return NotImplemented
        return (
            self.name == other.name
            and self.account_type == other.account_type
            and self.sub == other.sub
        )

    def __hash__(self):
        return hash((self.name, self.account_type, self.sub))

    @classmethod
    def from_string(cls, s: str):
        """
        Parse a string like 'Customer Deposits Hive (Asset) - Sub: devser.v4vapp'
        or 'Customer Deposits Hive (Asset) - Sub: devser.v4vapp (Contra)'
        and return an Account (or subclass) instance.
        """
        # Allow optional (Contra) at the end
        pattern = r"^(.*?) \((.*?)\) - Sub: (.*?)(?: \(Contra\))?$"
        match = re.match(pattern, s)
        if not match:
            raise ValueError(f"String does not match expected format: {s}")
        name, account_type_str, sub = match.groups()
        contra = s.strip().endswith("(Contra)")
        account_type_str = account_type_str.strip()

        # Convert string to AccountType enum
        try:
            account_type = AccountType(account_type_str)
        except ValueError:
            raise ValueError(f"Unknown account type: {account_type_str}")

        # Try to instantiate the correct subclass based on account_type
        for subclass in cls.__subclasses__():
            if (
                hasattr(subclass, "account_type")
                and getattr(subclass, "account_type") == account_type
            ):
                return subclass(name=name.strip(), sub=sub.strip(), contra=contra)
        # Fallback to base class if no subclass matches
        return cls(name=name.strip(), account_type=account_type, sub=sub.strip(), contra=contra)


# MARK: Asset Accounts
class AssetAccount(LedgerAccount):
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
        "Treasury Keepsats",
        "Exchange Deposits Hive",
        "Exchange Deposits Lightning",
        "Converted Hive Offset",
        "Converted Keepsats Offset",
        "External Lightning Payments",
        "Keepsats Lightning Movements",
        "Unset",
    ] = Field(..., description="Specific asset account name")
    account_type: Literal[AccountType.ASSET] = Field(
        AccountType.ASSET, description="Type of account"
    )

    def __init__(
        self,
        name: str = "",
        sub: str = "",
        account_type: AccountType = AccountType.ASSET,
        contra: bool = False,
    ):
        super().__init__(name=name, sub=sub, account_type=account_type, contra=contra)
        self.account_type = AccountType.ASSET
        self.name = name
        self.sub = sub
        self.contra = contra


# MARK: Liability Accounts
class LiabilityAccount(LedgerAccount):
    """
    LiabilityAccount is a subclass of Account that represents a specific type of liability account.
    Liabilities INCREASE with a CREDIT and DECREASE with a DEBIT.
    Attributes:
        name (Literal): The specific name of the liability account. Must be one of:
            - "Customer Liability"
        account_type (Literal[AccountType.LIABILITY]): The type of account, which is always set to `AccountType.LIABILITY`.
    Methods:
        __init__(name: str = "", sub: str = ""):
            Initializes a LiabilityAccount instance with the specified name and sub-account.
            Overrides the account_type to `AccountType.LIABILITY`.
    """

    name: Literal[
        "Customer Liability",
        "Keepsats Hold",
        "VSC Liability",
        "Owner Loan Payable (funding)",
    ] = Field(..., description="Specific liability account name")
    account_type: Literal[AccountType.LIABILITY] = Field(
        AccountType.LIABILITY, description="Type of account"
    )

    def __init__(
        self, name="", sub="", account_type: AccountType = AccountType.LIABILITY, contra=False
    ):
        super().__init__(name=name, sub=sub, account_type=account_type, contra=contra)
        self.account_type = AccountType.LIABILITY
        self.name = name
        self.sub = sub
        self.contra = contra


# MARK: Equity Accounts
class EquityAccount(LedgerAccount):
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

    def __init__(
        self, name="", sub="", account_type: AccountType = AccountType.EQUITY, contra=False
    ):
        super().__init__(name=name, sub=sub, account_type=account_type, contra=contra)
        self.account_type = AccountType.EQUITY
        self.name = name
        self.sub = sub
        self.contra = contra


# MARK: Revenue Accounts
class RevenueAccount(LedgerAccount):
    name: Literal[
        "Fee Income Hive",
        "Fee Income Lightning",
        "Fee Income Keepsats",
        "DHF Income",
        "Other Income",
    ] = Field(..., description="Specific revenue account name")
    account_type: Literal[AccountType.REVENUE] = Field(
        AccountType.REVENUE, description="Type of account"
    )

    def __init__(
        self, name="", sub="", account_type: AccountType = AccountType.REVENUE, contra=False
    ):
        super().__init__(name=name, sub=sub, account_type=account_type, contra=contra)
        self.account_type = AccountType.REVENUE
        self.name = name
        self.sub = sub
        self.contra = contra


# MARK: Expense Accounts
class ExpenseAccount(LedgerAccount):
    name: Literal[
        "Hosting Expenses Privex",
        "Hosting Expenses Voltage",
        "Fee Expenses Lightning",
        "Fee Expenses Hive",
    ] = Field(..., description="Specific expense account name")
    account_type: Literal[AccountType.EXPENSE] = Field(
        AccountType.EXPENSE, description="Type of account"
    )

    def __init__(
        self, name="", sub="", account_type: AccountType = AccountType.EXPENSE, contra=False
    ):
        super().__init__(name=name, sub=sub, account_type=account_type, contra=contra)
        self.account_type = AccountType.EXPENSE
        self.name = name
        self.sub = sub
        self.contra = contra


LedgerAccountAny = Union[
    AssetAccount,
    LiabilityAccount,
    EquityAccount,
    RevenueAccount,
    ExpenseAccount,
]


if __name__ == "__main__":
    # Example usage
    asset_account = AssetAccount(name="Customer Deposits Hive", sub="v4vapp")
    liability_account = LiabilityAccount(name="VSC Liability", sub="Sub-account 2")
    equity_account = EquityAccount(name="Owner's Capital", sub="Sub-account 3")
    revenue_account = RevenueAccount(name="Fee Income", sub="Sub-account 4")
    expense_account = ExpenseAccount(name="Hosting Expenses Privex", sub="Sub-account 5")

    print(asset_account)
    print(liability_account)
    print(equity_account)
    print(revenue_account)
    print(expense_account)
