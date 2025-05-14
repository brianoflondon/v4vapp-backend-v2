from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.accounting.account_type import AccountAny

# from v4vapp_backend_v2.actions.tracked_all import TrackedAny
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import Currency


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
    op: Any = Field(None, description="Associated Hive operation")

    model_config = ConfigDict()

    def __init__(self, **data):
        super().__init__(**data)

    @property
    def credit_debit(self) -> tuple[AccountAny, AccountAny]:
        """
        Returns a tuple of the credit and debit accounts.
        """
        return self.credit, self.debit

    def __repr__(self) -> str:
        """
        Returns a data representation of the LedgerEntry.
        """
        return f"LedgerEntry(group_id={self.group_id}, timestamp={self.timestamp}, description={self.description}, amount={self.amount}, unit={self.unit}, debit={self.debit}, credit={self.credit})"

    def __str__(self) -> str:
        """
        Returns a string representation of the LedgerEntry.
        """
        return f"{self.timestamp}   , description={self.description}, amount={self.amount}, unit={self.unit}, debit={self.debit}, credit={self.credit})"

    def print_journal_entry(self) -> str:
        """
        Prints a formatted journal entry matching the style in the provided image, including the unit of the account.

        Returns:
            str: A string representation of the journal entry.
        """
        # Format the timestamp as "MMM DD, YYYY" (e.g., "May 14, 2025")
        # formatted_date = self.timestamp.strftime("%b %d, %Y").upper() if self.timestamp else "N/A"

        formatted_date = f"{self.timestamp:%b %d, %Y %H:%M}  "  # Add extra space for formatting

        # Prepare the account names, ensuring they're properly formatted
        debit_account = self.debit.name if self.debit else "N/A"
        credit_account = self.credit.name if self.credit else "N/A"

        # Format the amount with 2 decimal places and include the unit
        unit_str = f" {self.unit.value}" if self.unit else ""  # e.g., " USD" or empty if no unit
        formatted_amount = f"{self.amount:.2f}{unit_str}" if self.amount else f"0.00{unit_str}"

        # Create the journal entry string
        entry = (
            f"\n"
            f"J/E NUMBER: {self.group_id or '#####'}\n"
            f"DATE\n{formatted_date}\n\n"
            f"{'ACCOUNT':<40} {' ' * 20} {'DEBIT':>15} {'CREDIT':>15}\n"  # Adjusted spacing for unit
            f"{'-' * 100}\n"  # Adjusted line length to match wider columns
            f"{debit_account:<40} {self.debit.sub:>20} {formatted_amount:>15} {'':>15}\n"
            f"{' ' * 4}{credit_account:<40} {self.credit.sub:>20} {'':>15} {formatted_amount:>15}\n\n"
            f"DESCRIPTION\n{self.description or 'N/A'}"
            f"\n{'=' * 100}\n"  # Adjusted line length to match wider columns
        )
        return entry

    @classmethod
    def collection(cls) -> str:
        """
        Returns the name of the collection associated with this model.

        This method is used to determine where the operation data will be stored
        in the database.

        Returns:
            str: The name of the collection.
        """
        return "ledger"

    def draw_t_diagram(self) -> str:
        """
        Draws a T-diagram for the LedgerEntry, showing account names, sub-values, account types, memo, and conversion values.
        """
        # Extract fields
        debit_name = self.debit.name if self.debit else ""
        debit_sub = self.debit.sub if self.debit and self.debit.sub else ""
        debit_type = (
            self.debit.account_type if self.debit and self.debit.account_type else "Unknown"
        )
        credit_name = self.credit.name if self.credit else ""
        credit_sub = self.credit.sub if self.credit and self.credit.sub else ""
        credit_type = (
            self.credit.account_type if self.credit and self.credit.account_type else "Unknown"
        )
        description = self.description or ""
        amount = self.amount
        unit = self.unit.value if self.unit else ""
        conv = self.conv

        # Truncate description if too long
        max_desc_len = 50
        if len(description) > max_desc_len:
            description = description[: max_desc_len - 3] + "..."

        # Define column widths
        account_width = max(len(debit_name), len(credit_name), 35)  # Increased to 35
        sub_width = max(len(debit_sub), len(credit_sub), 20)  # Increased to 20
        type_width = max(len(debit_type), len(credit_type), 10)  # Width for account type
        # Total width for each side: account + type + sub + parentheses + spaces
        side_width = account_width + type_width + sub_width + 4  # 2 for (), 2 for spaces
        # Total width includes both sides, 3 borders (|, |, |), 1 space
        total_width = side_width * 2 + 4 + 3

        # Build T-diagram
        lines = []

        # Header
        lines.append("=" * total_width)
        lines.append(f"{self.group_id:^{total_width}}")
        lines.append("=" * total_width)

        # Account headers
        lines.append(f"| {'Debit':<{side_width}} | {'Credit':<{side_width}} |")
        lines.append(f"| {'-' * side_width} | {'-' * side_width} |")

        # Account names, types, and sub-values
        debit_display = f"{debit_name} ({debit_type})"
        credit_display = f"{credit_name} ({credit_type})"
        lines.append(
            f"| {debit_display:<{account_width + type_width + 2}} {debit_sub:<{sub_width + 1}} | "
            f"{credit_display:<{account_width + type_width + 2}} {credit_sub:<{sub_width + 1}} |"
        )

        # Amount and unit
        amount_str = f"{amount:.3f} {unit}"
        lines.append(f"| {amount_str:<{side_width}} | {amount_str:<{side_width}} |")

        # Footer
        lines.append("=" * total_width)

        # Description
        lines.append(f"Description: {description}")
        lines.append("-" * total_width)

        # Conversion values
        if conv:
            lines.append("Conversion Values (at time of entry):")
            lines.append(f"{'Currency':<10} | {'Value':>10} | {'Rate':>15}")
            lines.append(f"{'-' * 10}-+-{'-' * 10}-+-{'-' * 15}")
            lines.append(f"{'HIVE':<10} | {conv.hive:>10.3f} | {conv.sats_hive:>15.2f} Sats/HIVE")
            lines.append(f"{'HBD':<10} | {conv.hbd:>10.3f} | {conv.sats_hbd:>15.2f} Sats/HBD")
            lines.append(f"{'USD':<10} | {conv.usd:>10.3f} |")
            lines.append(f"{'SATS':<10} | {conv.sats:>10} |")
            lines.append(f"{'BTC':<10} | {conv.btc:>10.8f} |")
            if conv.fetch_date:
                lines.append(f"Fetched: {conv.fetch_date.strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append(f"Source: {conv.source}")
        lines.append("=" * total_width)

        return "\n".join(lines)
