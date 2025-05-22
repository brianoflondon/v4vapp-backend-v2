from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.accounting.account_type import AccountAny
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import Currency


class LedgerEntry(BaseModel):
    """
    Represents a ledger entry in the accounting system, supporting multi-currency transactions.
    """

    group_id: str = Field("", description="Group ID for the ledger entry")
    timestamp: datetime = Field(
        datetime.now(tz=timezone.utc), description="Timestamp of the ledger entry"
    )
    description: str = Field("", description="Description of the ledger entry")
    debit_amount: float = Field(0.0, description="Amount of the debit transaction")
    debit_unit: Currency = Field(
        default=Currency.HIVE, description="Unit of the debit transaction"
    )
    debit_conv: CryptoConv = Field(
        default_factory=CryptoConv, description="Conversion details for the debit transaction"
    )
    credit_amount: float = Field(0.0, description="Amount of the credit transaction")
    credit_unit: Currency = Field(
        default=Currency.HIVE, description="Unit of the credit transaction"
    )
    credit_conv: CryptoConv = Field(
        default_factory=CryptoConv, description="Conversion details for the credit transaction"
    )
    debit: AccountAny | None = Field(None, description="Account to be debited")
    credit: AccountAny | None = Field(None, description="Account to be credited")
    op: TrackedAny = Field(..., description="Associated operation")

    model_config = ConfigDict()

    def __init__(self, **data):
        super().__init__(**data)

    @property
    def is_completed(self) -> bool:
        """
        Returns True if the LedgerEntry is completed, False otherwise.
        """
        if not self.debit and not self.credit:
            return False
        if not self.debit_amount and not self.credit_amount:
            return False
        return True

    @property
    def credit_debit(self) -> tuple[AccountAny | None, AccountAny | None]:
        """
        Returns a tuple of the credit and debit accounts.
        """
        return self.credit, self.debit

    def __repr__(self) -> str:
        """
        Returns a data representation of the LedgerEntry.
        """
        return (
            f"LedgerEntry(group_id={self.group_id}, timestamp={self.timestamp}, description={self.description}, "
            f"debit_amount={self.debit_amount}, debit_unit={self.debit_unit}, "
            f"credit_amount={self.credit_amount}, credit_unit={self.credit_unit}, "
            f"debit={self.debit}, credit={self.credit})"
        )

    def __str__(self) -> str:
        """
        Returns a string representation of the LedgerEntry.
        """
        return self.print_journal_entry()

    def print_journal_entry(self) -> str:
        """
        Prints a formatted journal entry, showing different currencies for debit and credit if applicable.

        Returns:
            str: A string representation of the journal entry.
        """
        if not self.is_completed or not self.debit or not self.credit:
            # If the entry is not completed, show a warning
            return (
                f"WARNING: LedgerEntry is not completed. Missing debit or credit account.\n"
                f"{'=' * 100}\n"
            )

        formatted_date = f"{self.timestamp:%b %d, %Y %H:%M}  "  # Add extra space for formatting

        # Prepare the account names
        debit_account = self.debit.name if self.debit else "N/A"
        credit_account = self.credit.name if self.credit else "N/A"

        # Format the amounts with 2 decimal places and include the units
        debit_unit_str = f" {self.debit_unit.value}" if self.debit_unit else ""
        credit_unit_str = f" {self.credit_unit.value}" if self.credit_unit else ""
        formatted_debit_amount = (
            f"{self.debit_amount:.2f}{debit_unit_str}"
            if self.debit_amount
            else f"0.00{debit_unit_str}"
        )
        formatted_credit_amount = (
            f"{self.credit_amount:.2f}{credit_unit_str}"
            if self.credit_amount
            else f"0.00{credit_unit_str}"
        )

        # Create the journal entry string
        entry = (
            f"\n"
            f"J/E NUMBER: {self.group_id or '#####'}\n"
            f"DATE\n{formatted_date}\n\n"
            f"{'ACCOUNT':<40} {' ' * 20} {'DEBIT':>15} {'CREDIT':>15}\n"
            f"{'-' * 100}\n"
            f"{debit_account:<40} {self.debit.sub:>20} {formatted_debit_amount:>15} {'':>15}\n"
            f"{' ' * 4}{credit_account:<40} {self.credit.sub:>20} {'':>15} {formatted_credit_amount:>15}\n\n"
            f"DESCRIPTION\n{self.description or 'N/A'}"
            f"\n{'=' * 100}\n"
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
        Draws a T-diagram for the LedgerEntry, showing account names, sub-values, account types, memo,
        and conversion values for both debit and credit sides.
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
        debit_amount = self.debit_amount
        debit_unit = self.debit_unit.value if self.debit_unit else ""
        credit_amount = self.credit_amount
        credit_unit = self.credit_unit.value if self.credit_unit else ""
        debit_conv = self.debit_conv
        credit_conv = self.credit_conv

        # Truncate description if too long
        max_desc_len = 50
        if len(description) > max_desc_len:
            description = description[: max_desc_len - 3] + "..."

        # Define column widths
        account_width = max(len(debit_name), len(credit_name), 35)
        sub_width = max(len(debit_sub), len(credit_sub), 20)
        type_width = max(len(debit_type), len(credit_type), 10)
        side_width = account_width + type_width + sub_width + 4
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

        # Amounts and units
        debit_amount_str = f"{debit_amount:.3f} {debit_unit}"
        credit_amount_str = f"{credit_amount:.3f} {credit_unit}"
        lines.append(f"| {debit_amount_str:<{side_width}} | {credit_amount_str:<{side_width}} |")

        # Footer
        lines.append("=" * total_width)

        # Description
        lines.append(f"Description: {description}")
        lines.append("-" * total_width)

        # Conversion values for debit
        if debit_conv:
            lines.append("Debit Conversion Values (at time of entry):")
            lines.append(f"{'Currency':<10} | {'Value':>10} | {'Rate':>15}")
            lines.append(f"{'-' * 10}-+-{'-' * 10}-+-{'-' * 15}")
            lines.append(
                f"{'HIVE':<10} | {debit_conv.hive:>10.3f} | {debit_conv.sats_hive:>15.2f} Sats/HIVE"
            )
            lines.append(
                f"{'HBD':<10} | {debit_conv.hbd:>10.3f} | {debit_conv.sats_hbd:>15.2f} Sats/HBD"
            )
            lines.append(f"{'USD':<10} | {debit_conv.usd:>10.3f} |")
            lines.append(f"{'SATS':<10} | {debit_conv.sats:>10} |")
            lines.append(f"{'BTC':<10} | {debit_conv.btc:>10.8f} |")
            if debit_conv.fetch_date:
                lines.append(f"Fetched: {debit_conv.fetch_date.strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append(f"Source: {debit_conv.source}")
            lines.append("-" * total_width)

        # Conversion values for credit
        if credit_conv:
            lines.append("Credit Conversion Values (at time of entry):")
            lines.append(f"{'Currency':<10} | {'Value':>10} | {'Rate':>15}")
            lines.append(f"{'-' * 10}-+-{'-' * 10}-+-{'-' * 15}")
            lines.append(
                f"{'HIVE':<10} | {credit_conv.hive:>10.3f} | {credit_conv.sats_hive:>15.2f} Sats/HIVE"
            )
            lines.append(
                f"{'HBD':<10} | {credit_conv.hbd:>10.3f} | {credit_conv.sats_hbd:>15.2f} Sats/HBD"
            )
            lines.append(f"{'USD':<10} | {credit_conv.usd:>10.3f} |")
            lines.append(f"{'SATS':<10} | {credit_conv.sats:>10} |")
            lines.append(f"{'BTC':<10} | {credit_conv.btc:>10.8f} |")
            if credit_conv.fetch_date:
                lines.append(f"Fetched: {credit_conv.fetch_date.strftime('%Y-%m-%d %H:%M:%S')}")
            lines.append(f"Source: {credit_conv.source}")
        lines.append("=" * total_width)

        return "\n".join(lines)
