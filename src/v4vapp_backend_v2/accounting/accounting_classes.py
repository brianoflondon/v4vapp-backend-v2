from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from pydantic import BaseModel, RootModel

from v4vapp_backend_v2.accounting.converted_summary_class import ConvertedSummary
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import Currency

"""
Helper classes for accounting summaries, including account balances and lightning conv summaries.
"""


@dataclass
class UnitSummary:
    final_balance: float = 0.0
    converted: ConvertedSummary = field(default_factory=ConvertedSummary)


@dataclass
class AccountBalanceSummary:
    """
    Represents a summary of account balances, including totals and detailed line items.

    Attributes:
        unit_summaries (Dict[str, UnitSummary]): A dictionary mapping unit names to their summaries.
        total_usd (float): The total account balance in USD.
        total_sats (float): The total account balance in satoshis.
        line_items (List[str]): A list of line item descriptions related to the account balance.
        output_text (str): A formatted string representation of the summary for output purposes.
    """

    unit_summaries: Dict[str, UnitSummary] = field(default_factory=dict)
    total_usd: float = 0.0
    total_sats: float = 0.0
    total_hive: float = 0.0
    total_hbd: float = 0.0
    total_msats: float = 0.0
    line_items: List[str] = field(default_factory=list)
    output_text: str = ""


@dataclass
class LedgerConvSummary(ConvertedSummary):
    """
    Represents a summary of ledger conversions or any transactions,
    including total amounts and a formatted output.

    Attributes:
        cust_id (str): The customer ID associated with the summary.
        account (LedgerAccount | None): The ledger account associated with the summary.
        as_of_date (datetime): The date and time when the summary was generated.
        age (int): The age in seconds for filtering purposes.
        by_ledger_type (Dict[str, ConvertedSummary]): A dictionary mapping ledger types to their
        ledger_entries (List[LedgerEntry]): A list of ledger entries associated with the summary.
        net_balance (ConvertedSummary | None): The net balance calculated from the ledger entries.
    """

    cust_id: str = ""
    account: LedgerAccount | None = None
    as_of_date: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    age: timedelta | None = None
    by_ledger_type: Dict[str, ConvertedSummary] = field(default_factory=dict)
    ledger_entries: List[LedgerEntry] = field(default_factory=list)
    net_balance: ConvertedSummary = field(default_factory=ConvertedSummary)


@dataclass
class LightningLimitSummary:
    """
    Represents a summary of lightning conv limits for an account.

    Attributes:
        total_sats (int): Total lightning conv in satoshis.
        total_msats (int): Total lightning conv in millisatoshis.
        output_text (str): A formatted string representation of the lightning conv limits.
    """

    conv_summary: LedgerConvSummary
    total_sats: float
    total_msats: float
    output_text: str
    limit_ok: bool


class AccountBalanceLine(BaseModel):
    group_id: str | None = None
    short_id: str | None = None
    ledger_type: str | None = None
    timestamp: datetime | None = None
    description: str | None = None
    cust_id: str | None = None
    op_type: str | None = None
    account_type: str | None = None
    name: str | None = None
    sub: str | None = None
    contra: bool | None = None
    amount: float | None = None
    amount_signed: float | None = None
    unit: str | None = None
    conv: CryptoConv | None = None
    conv_signed: CryptoConv | None = None
    side: str | None = None
    amount_running_total: float | None = None
    conv_running_total: ConvertedSummary | None = None


class LedgerAccountDetails(LedgerAccount):
    balances: Dict[Currency, List[AccountBalanceLine]] = {}


class AccountBalances(RootModel):
    root: List[LedgerAccountDetails]


# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
# This is the last line# This is the last line
