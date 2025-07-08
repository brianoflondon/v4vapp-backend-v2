from dataclasses import dataclass, field
from typing import Dict, List

"""
Helper classes for accounting summaries, including account balances and lightning spend summaries.
"""


@dataclass
class ConvertedSummary:
    hive: float = 0.0
    hbd: float = 0.0
    usd: float = 0.0
    sats: float = 0.0
    msats: float = 0.0


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
class LightningConvSummary(ConvertedSummary):
    """
    Represents a summary of lightning conversion, including total amounts and a formatted output.

    Attributes:
        cust_id (str): The customer ID associated with the summary.
        age (int): The age in seconds for filtering purposes.
        by_ledger_type (Dict[str, ConvertedSummary]): A dictionary mapping ledger types to their
        output_text (str): A formatted string representation of the lightning spend summary.
    """

    cust_id: str = ""
    age: int = 0  # Age in seconds, used for filtering
    # total_hive: float = 0.0
    # total_hbd: float = 0.0
    # total_usd: float = 0.0
    # total_sats: float = 0.0
    # total_msats: float = 0.0
    by_ledger_type: Dict[str, ConvertedSummary] = field(default_factory=dict)


@dataclass
class LightningLimitSummary:
    """
    Represents a summary of lightning spend limits for an account.

    Attributes:
        total_sats (int): Total lightning spend in satoshis.
        total_msats (int): Total lightning spend in millisatoshis.
        output_text (str): A formatted string representation of the lightning spend limits.
    """

    spend_summary: LightningConvSummary
    total_sats: float
    total_msats: float
    output_text: str
    limit_ok: bool
