from dataclasses import dataclass, field
from typing import Dict, List

from v4vapp_backend_v2.accounting.account_type import LedgerAccount


@dataclass
class ConvertedSummary:
    hive: float
    hbd: float
    usd: float
    sats: float
    msats: float


@dataclass
class UnitSummary:
    final_balance: float
    converted: ConvertedSummary


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
class LightningSpendSummary:
    """
    Represents a summary of lightning spend, including total amounts and a formatted output.

    Attributes:
        total_hive (float): Total amount spent in Hive.
        total_hbd (float): Total amount spent in HBD.
        total_usd (float): Total amount spent in USD.
        total_sats (float): Total amount spent in satoshis.
        total_msats (float): Total amount spent in millisatoshis.
        output_text (str): A formatted string representation of the lightning spend summary.
    """

    account: LedgerAccount | None = None
    age: int = 0  # Age in seconds, used for filtering
    total_hive: float = 0.0
    total_hbd: float = 0.0
    total_usd: float = 0.0
    total_sats: float = 0.0
    total_msats: float = 0.0
