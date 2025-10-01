from dataclasses import field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List

from nectar.amount import Amount
from pydantic import BaseModel, ConfigDict, Field, RootModel
from pydantic.dataclasses import dataclass
from tabulate import tabulate

from v4vapp_backend_v2.accounting.converted_summary_class import ConvertedSummary
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType, LedgerTypeIcon
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
    ledger_type_str: str = ""
    link: str = ""
    icon: str = ""
    timestamp: datetime = datetime.now(tz=timezone.utc)
    timestamp_unix: float = 0.0
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
    conv: CryptoConv | None = None
    conv_signed: CryptoConv = CryptoConv()
    side: str = Field("", description="The side of the transaction, e.g., 'debit' or 'credit'")
    amount_running_total: Decimal = Decimal(0)
    conv_running_total: ConvertedSummary = ConvertedSummary()

    def __init__(self, **data):
        super().__init__(**data)
        if not self.icon:
            try:
                lt = LedgerType(self.ledger_type)
                self.ledger_type_str = lt.capitalized
                self.icon = LedgerTypeIcon.get(lt, "❓")
            except ValueError:
                self.icon = "❓"


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
        default_factory=dict,
        description="Complete details for all transactions in each currency, each list is for a separate currency",
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
    hive: Decimal = Field(Decimal(0), description="Latest running total for HIVE currency")
    hbd: Decimal = Field(Decimal(0), description="Latest running total for HBD currency")
    usd: Decimal = Field(Decimal(0), description="Latest running total for USD currency")
    msats: Decimal = Field(Decimal(0), description="Latest running total for MSATS currency")
    sats: Decimal = Field(Decimal(0), description="Latest running total for SATS currency")
    conv_total: ConvertedSummary = Field(
        ConvertedSummary(),
        description="Aggregated conversion total across all currencies",
    )
    in_progress_msats: Decimal = Field(
        Decimal(0),
        description="Net amount of keepsats currently held (HOLD_KEEPSATS - RELEASE_KEEPSATS)",
    )

    model_config = ConfigDict(populate_by_name=True)

    @property
    def hive_amount(self) -> Amount:
        """Returns the HIVE balance as an Amount object."""
        return Amount(f"{self.hive:.3f} HIVE")

    @property
    def hbd_amount(self) -> Amount:
        """Returns the HBD balance as an Amount object."""
        return Amount(f"{self.hbd:.3f} HBD")

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
            self.sats = Decimal(self.msats / Decimal(1000)).quantize(
                Decimal("1"), rounding="ROUND_HALF_UP"
            )

        for currency, balance_lines in self.balances.items():
            if balance_lines:
                self.balances_totals[currency] = balance_lines[-1].conv_running_total
                self.balances_net[currency] = balance_lines[-1].amount_running_total
            else:
                self.balances_totals[currency] = ConvertedSummary()
                self.balances_net[currency] = Decimal(0)

        # Create copies of the balance lines, filter out unwanted ledger types, and delete the .conv item from each line
        combined_lines = []
        for lines in self.balances.values():
            for line in lines:
                if line.ledger_type not in ["hold_k", "release_k"]:
                    line_copy = line.model_copy()
                    line_copy.conv = None
                    combined_lines.append(line_copy)
        self.combined_balance = sorted(combined_lines, key=lambda x: x.timestamp)

        if self.combined_balance:
            # Initialize the first line's running total
            self.combined_balance[0].conv_running_total = ConvertedSummary.from_crypto_conv(
                self.combined_balance[0].conv_signed
            )
            self.combined_balance[0].timestamp_unix = (
                self.combined_balance[0].timestamp.timestamp() * 1000
            )

            # Calculate running totals for subsequent lines
            for i in range(1, len(self.combined_balance)):
                self.combined_balance[i].timestamp_unix = (
                    self.combined_balance[i].timestamp.timestamp() * 1000
                )
                line = self.combined_balance[i]
                prev_line = self.combined_balance[i - 1]
                line.conv_running_total = (
                    prev_line.conv_running_total
                    + ConvertedSummary.from_crypto_conv(line.conv_signed)
                )

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
        # Prepare table data
        table_data = []

        # Add account name row
        account_name_str = LedgerAccount.__str__(self)

        # Get the main value to display on account line
        main_value = ""
        if Currency.HIVE in self.balances_totals:
            conv_summary = self.balances_totals[Currency.HIVE]
            main_value = f"{conv_summary.hive:.3f}" if conv_summary.hive else "0.000"
        elif Currency.MSATS in self.balances_totals:
            conv_summary = self.balances_totals[Currency.MSATS]
            sats_value = int(conv_summary.sats) if conv_summary.sats else 0
            main_value = f"{sats_value:,}"

        table_data.append([account_name_str, main_value])

        # Add currency data for additional currencies
        for currency, conv_summary in self.balances_totals.items():
            if currency == Currency.HIVE:
                value = f"{conv_summary.hive:.3f}" if conv_summary.hive else "0.000"
                table_data.append(["HIVE", value])
            elif currency == Currency.MSATS:
                sats_value = int(conv_summary.sats) if conv_summary.sats else 0
                value = f"{sats_value:,}"
                table_data.append(["SATS", value])

        return tabulate(table_data, headers=["Account/Currency", "Balance"], tablefmt="fancy_grid")

    def remove_balances(self) -> "LedgerAccountDetails":
        """
        Remove all balances from this LedgerAccountDetails instance by setting the balances attribute to an empty dictionary.

        This method creates a deep copy of the current instance, clears its balances, and returns the modified copy.
        The original instance remains unchanged.

        Returns:
            LedgerAccountDetails: A new LedgerAccountDetails instance with balances removed (set to an empty dict).
        """
        copy_balance = self.model_copy()
        copy_balance.balances = {}
        return copy_balance

    def to_api_response(self, hive_accname: str, line_items: bool = False) -> dict:
        """
        Returns a dictionary representation of the account balance details, with numeric values
        rounded to 3 decimal places (half up) where applicable, formatted for API responses.
        All numeric values are returned as floats.

        Args:
            hive_accname (str): The Hive account name.
            line_items (bool): If True, includes the full account balance object in 'all_transactions';
                               otherwise, an empty list.

        Returns:
            dict: A dictionary with the specified keys and rounded float values.
        """
        in_progress_sats = round(
            Decimal(self.in_progress_msats / 1000).quantize(
                Decimal("1"), rounding="ROUND_HALF_UP"
            ),
            0,
        )
        return {
            "hive_accname": hive_accname,
            "net_msats": round(float(self.msats), 0),
            "net_hive": round(float(self.hive), 3),
            "net_usd": round(float(self.usd), 3),
            "net_hbd": round(float(self.hbd), 3),
            "net_sats": float(round(self.sats, 0)),
            "in_progress_sats": float(in_progress_sats),
            "all_transactions": self if line_items else [],
        }


class AccountBalances(RootModel):
    root: List[LedgerAccountDetails]


# This is the last line# This is the last line
# This is the last line# This is the last line
