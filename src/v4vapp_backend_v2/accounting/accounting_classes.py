from dataclasses import field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator
from pydantic.dataclasses import dataclass

from v4vapp_backend_v2.accounting.converted_summary_class import ConvertedSummary
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.currency_class import Currency

"""
Helper classes for accounting summaries, including account balances and lightning conv summaries.
"""


@dataclass
class UnitSummary:
    final_balance: Decimal = Decimal(0)
    converted: ConvertedSummary = field(default_factory=ConvertedSummary)


@dataclass
class AccountBalanceSummary:
    """
    Represents a summary of account balances, including totals and detailed line items.

    Attributes:
        unit_summaries (Dict[str, UnitSummary]): A dictionary mapping unit names to their summaries.
        total_usd (Decimal): The total account balance in USD.
        total_sats (Decimal): The total account balance in satoshis.
        line_items (List[str]): A list of line item descriptions related to the account balance.
        output_text (str): A formatted string representation of the summary for output purposes.
    """

    unit_summaries: Dict[str, UnitSummary] = field(default_factory=dict)
    total_usd: Decimal = Decimal(0)
    total_sats: Decimal = Decimal(0)
    total_hive: Decimal = Decimal(0)
    total_hbd: Decimal = Decimal(0)
    total_msats: Decimal = Decimal(0)
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
        total_sats (Decimal): Total lightning conv in satoshis.
        total_msats (Decimal): Total lightning conv in millisatoshis.
        output_text (str): A formatted string representation of the lightning conv limits.
    """

    conv_summary: LedgerConvSummary
    total_sats: Decimal = Decimal(0)
    total_msats: Decimal = Decimal(0)
    output_text: str = ""
    limit_ok: bool = False


class AccountBalanceLine(BaseModel):
    group_id: str = ""
    short_id: str = ""
    ledger_type: str = ""
    timestamp: datetime = datetime.now(tz=timezone.utc)
    description: str = ""
    user_memo: str = ""
    cust_id: str = ""
    op_type: str = ""
    account_type: str = ""
    name: str = ""
    sub: str = ""
    contra: bool = False
    amount: Decimal = Decimal(0)
    amount_signed: Decimal = Decimal(0)
    unit: str = ""
    conv: CryptoConv = CryptoConv()
    conv_signed: CryptoConv = CryptoConv()
    side: str = Field("", description="The side of the transaction, e.g., 'debit' or 'credit'")
    amount_running_total: Decimal = Decimal(0)
    conv_running_total: ConvertedSummary = ConvertedSummary()


class LedgerAccountDetails(LedgerAccount):
    """
    LedgerAccountDetails extends LedgerAccount to provide detailed balance information for multiple currencies,
    including HIVE, HBD, USD, MSATS, and SATS. It maintains running totals and conversion summaries for each currency,
    and provides a formatted printout for Keepsats integration.
    Attributes:
        balances (Dict[Currency, List[AccountBalanceLine]]): Mapping of currency to its balance lines.
        balances_totals (Dict[Currency, ConvertedSummary]): Mapping of currency to its converted summary totals.
        hive (Decimal): Latest running total for HIVE currency.
        hbd (Decimal): Latest running total for HBD currency.
        usd (Decimal): Latest running total for USD currency.
        msats (Decimal): Latest running total for MSATS currency.
        sats (Decimal): Latest running total for SATS currency (derived from MSATS).
        conv_total (ConvertedSummary): Aggregated conversion total across all currencies.
    Methods:
        __init__(**data): Initializes the LedgerAccountDetails instance, computes currency totals and conversion summaries.
        keepsats_printout() -> str: Returns a formatted string representation of the account details for Keepsats.
    """

    balances: Dict[Currency, List[AccountBalanceLine]] = Field(
        default_factory=dict, description="Complete details for all transactions in each currency"
    )
    balances_totals: Dict[Currency, ConvertedSummary] = Field(
        default_factory=dict,
        description="Conversions to every currency for each currency which has a balance, including conversion summaries",
    )
    balances_net: Dict[Currency, Decimal] = Field(
        default_factory=dict, description="Net balance in each currency that has a balance"
    )
    combined_balance: List[AccountBalanceLine] = Field(
        default_factory=list,
        description="Combined list of all balance lines across all currencies sorted by timestamp ascending",
    )
    last_transaction_date: datetime | None = None
    hive: Decimal = Decimal(0)
    hbd: Decimal = Decimal(0)
    usd: Decimal = Decimal(0)
    msats: Decimal = Decimal(0)
    sats: Decimal = Decimal(0)
    conv_total: ConvertedSummary = ConvertedSummary()

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **data):
        super().__init__(**data)

        if Currency.HIVE in self.balances:
            self.hive = round(self.balances[Currency.HIVE][-1].amount_running_total, 3)
            self.conv_total += self.balances[Currency.HIVE][-1].conv_running_total
        if Currency.HBD in self.balances:
            self.hbd = round(self.balances[Currency.HBD][-1].amount_running_total, 3)
            self.conv_total += self.balances[Currency.HBD][-1].conv_running_total
        if Currency.USD in self.balances:
            self.usd = round(self.balances[Currency.USD][-1].amount_running_total, 3)
            self.conv_total += self.balances[Currency.USD][-1].conv_running_total
        if Currency.MSATS in self.balances:
            self.msats = self.balances[Currency.MSATS][-1].amount_running_total
            self.conv_total += self.balances[Currency.MSATS][-1].conv_running_total
            self.sats = round(self.msats / 1000, 0)

        for currency, balance_lines in self.balances.items():
            if balance_lines:
                self.balances_totals[currency] = balance_lines[-1].conv_running_total
                self.balances_net[currency] = balance_lines[-1].amount_running_total
            else:
                self.balances_totals[currency] = ConvertedSummary()
                self.balances_net[currency] = Decimal(0)

        self.combined_balance = sorted(
            [line for lines in self.balances.values() for line in lines], key=lambda x: x.timestamp
        )
        for line in self.combined_balance:
            line.conv_running_total += ConvertedSummary.from_crypto_conv(line.conv_signed)

    def __str__(self) -> str:
        """
        Returns a string representation of the account name.
        This is used for logging and display purposes.
        """
        return self.balances_printout()

    def balances_printout(self) -> str:
        """
        Returns a formatted string representation of the account details for Keepsats.
        """
        # Fixed column widths
        col1_width = 50
        col2_width = 12

        # Create separator
        separator = f"+{'-' * (col1_width + 2)}+{'-' * (col2_width + 2)}+"

        lines = [separator]

        # Add account name row with value (truncate if too long)
        account_name_str = LedgerAccount.__str__(self)
        if len(account_name_str) > col1_width:
            account_name_str = account_name_str[: col1_width - 3] + "..."

        # Get the main value to display on account line
        main_value = ""
        if Currency.HIVE in self.balances_totals:
            conv_summary = self.balances_totals[Currency.HIVE]
            main_value = f"{conv_summary.hive:.3f}" if conv_summary.hive else "0.000"
        elif Currency.MSATS in self.balances_totals:
            conv_summary = self.balances_totals[Currency.MSATS]
            sats_value = int(conv_summary.sats) if conv_summary.sats else 0
            main_value = f"{sats_value:,}"

        lines.append(f"| {account_name_str:<{col1_width}} | {main_value:>{col2_width}} |")

        # Add currency data for additional currencies
        for currency, conv_summary in self.balances_totals.items():
            if currency == Currency.HIVE:
                value = f"{conv_summary.hive:.3f}" if conv_summary.hive else "0.000"
                lines.append(f"| {'HIVE':<{col1_width}} | {value:>{col2_width}} |")
            elif currency == Currency.MSATS:
                sats_value = int(conv_summary.sats) if conv_summary.sats else 0
                value = f"{sats_value:,}"
                lines.append(f"| {'SATS':<{col1_width}} | {value:>{col2_width}} |")
            lines.append(separator)

        return "\n".join(lines)


class AccountBalances(RootModel):
    root: List[LedgerAccountDetails]


class StrippedAccountBalanceLine(BaseModel):
    short_id: str = ""
    ledger_type: str = ""
    timestamp: datetime = datetime.now(tz=timezone.utc)
    description: str = ""
    user_memo: str = ""
    cust_id: str = ""
    op_type: str = ""
    account_type: str = ""
    name: str = ""
    sub: str = ""
    contra: bool = False
    amount: Decimal = Decimal(0)
    amount_signed: Decimal = Decimal(0)
    unit: str = ""
    conv: CryptoConv = CryptoConv()
    conv_signed: CryptoConv = CryptoConv()
    side: str = Field("", description="The side of the transaction, e.g., 'debit' or 'credit'")
    amount_running_total: Decimal = Decimal(0)
    conv_running_total: ConvertedSummary = ConvertedSummary()


class StrippedLedgerAccountDetails(LedgerAccount):
    """
    Stripped-down version of LedgerAccountDetails without group_id in balance lines.
    """

    balances: Dict[Currency, List[StrippedAccountBalanceLine]] = Field(
        default_factory=dict, description="Complete details for all transactions in each currency"
    )
    balances_totals: Dict[Currency, ConvertedSummary] = Field(
        default_factory=dict,
        description="Totals for each currency, including conversion summaries",
    )
    balances_net: Dict[Currency, Decimal] = Field(
        default_factory=dict, description="Net balances for each currency"
    )
    last_transaction_date: datetime | None = None
    hive: Decimal = Decimal(0)
    hbd: Decimal = Decimal(0)
    usd: Decimal = Decimal(0)
    msats: Decimal = Decimal(0)
    sats: Decimal = Decimal(0)
    conv_total: ConvertedSummary = ConvertedSummary()

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def strip_group_id(cls, data):
        if isinstance(data, dict) and "balances" in data:
            balances = data["balances"]
            if isinstance(balances, dict):
                for currency, lines in balances.items():
                    if isinstance(lines, list):
                        for line in lines:
                            if isinstance(line, dict) and "group_id" in line:
                                del line["group_id"]
        return data

    def __init__(self, **data):
        super().__init__(**data)

        if Currency.HIVE in self.balances:
            self.hive = round(self.balances[Currency.HIVE][-1].amount_running_total, 3)
            self.conv_total += self.balances[Currency.HIVE][-1].conv_running_total
        if Currency.HBD in self.balances:
            self.hbd = round(self.balances[Currency.HBD][-1].amount_running_total, 3)
            self.conv_total += self.balances[Currency.HBD][-1].conv_running_total
        if Currency.USD in self.balances:
            self.usd = round(self.balances[Currency.USD][-1].amount_running_total, 3)
            self.conv_total += self.balances[Currency.USD][-1].conv_running_total
        if Currency.MSATS in self.balances:
            self.msats = self.balances[Currency.MSATS][-1].amount_running_total
            self.conv_total += self.balances[Currency.MSATS][-1].conv_running_total
            self.sats = round(self.msats / 1000, 0)

        for currency, balance_lines in self.balances.items():
            if balance_lines:
                self.balances_totals[currency] = balance_lines[-1].conv_running_total
                self.balances_net[currency] = balance_lines[-1].amount_running_total
            else:
                self.balances_totals[currency] = ConvertedSummary()
                self.balances_net[currency] = Decimal(0)

    def __str__(self) -> str:
        """
        Returns a string representation of the account name.
        This is used for logging and display purposes.
        """
        return self.balances_printout()

    def balances_printout(self) -> str:
        """
        Returns a formatted string representation of the account details for Keepsats.
        """
        # Fixed column widths
        col1_width = 50
        col2_width = 12

        # Create separator
        separator = f"+{'-' * (col1_width + 2)}+{'-' * (col2_width + 2)}+"

        lines = [separator]

        # Add account name row with value (truncate if too long)
        account_name_str = LedgerAccount.__str__(self)
        if len(account_name_str) > col1_width:
            account_name_str = account_name_str[: col1_width - 3] + "..."

        # Get the main value to display on account line
        main_value = ""
        if Currency.HIVE in self.balances_totals:
            conv_summary = self.balances_totals[Currency.HIVE]
            main_value = f"{conv_summary.hive:.3f}" if conv_summary.hive else "0.000"
        elif Currency.MSATS in self.balances_totals:
            conv_summary = self.balances_totals[Currency.MSATS]
            sats_value = int(conv_summary.sats) if conv_summary.sats else 0
            main_value = f"{sats_value:,}"

        lines.append(f"| {account_name_str:<{col1_width}} | {main_value:>{col2_width}} |")

        # Add currency data for additional currencies
        for currency, conv_summary in self.balances_totals.items():
            if currency == Currency.HIVE:
                value = f"{conv_summary.hive:.3f}" if conv_summary.hive else "0.000"
                lines.append(f"| {'HIVE':<{col1_width}} | {value:>{col2_width}} |")
            elif currency == Currency.MSATS:
                sats_value = int(conv_summary.sats) if conv_summary.sats else 0
                value = f"{sats_value:,}"
                lines.append(f"| {'SATS':<{col1_width}} | {value:>{col2_width}} |")
            lines.append(separator)

        return "\n".join(lines)


# This is the last line# This is the last line
