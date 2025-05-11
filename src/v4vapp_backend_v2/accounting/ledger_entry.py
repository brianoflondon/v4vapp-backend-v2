from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.accounting.account_type import AccountAny, AssetAccount, LiabilityAccount
from v4vapp_backend_v2.actions.tracked_all import TrackedAny, tracked_type
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
    op: TrackedAny = Field(None, description="Associated Hive operation")

    model_config = ConfigDict()

    def __init__(self, **data):
        super().__init__(**data)

    @property
    def credit_debit(self) -> tuple[AccountAny, AccountAny]:
        """
        Returns a tuple of the credit and debit accounts.
        """
        return self.credit, self.debit

    def owners_loan(self, op: TrackedAny) -> Self:
        """
        Process a hive operation for an owner's loan.
        This method sets the debit and credit accounts based on the operation type.
        """
        if tracked_type(op) != "transfer":
            raise ValueError("Operation must be of type 'transfer'")
        self.group_id = op.group_id
        self.timestamp = op.timestamp
        self.description = op.d_memo
        self.unit = op.unit
        self.amount = op.amount_decimal
        self.conv = op.conv
        self.debit = AssetAccount(name="Treasury Hive", sub=op.to_account)
        self.credit = LiabilityAccount(name="Owners Loan Payable", sub=op.from_account)
        self.op = op
        return self


# class HiveServerTransfer(LedgerEntry):
#     """
#     Represents a ledger entry for Hive operations involving a server account.
#     Transfers to and from the server account are treated as customer deposits and liabilities.
#     Transfers to and from the treasury account are treated as internal transfers.
#     Attributes:
#         hive_op (OpAny): The Hive operation associated with this ledger entry.
#         server_account (str): The server account involved in the transaction.
#         treasury_account (str): The treasury account involved in the transaction.

#     """

#     def __init__(self, hive_op: OpAny, server_account: str, treasury_account: str) -> None:
#         if not hive_op:
#             raise ValueError("hive_op must be provided")
#         if not server_account:
#             raise ValueError("server_account must be provided")
#         if not treasury_account:
#             raise ValueError("treasury_account must be provided")
#         if server_account not in [hive_op.to_account, hive_op.from_account]:
#             raise ValueError(
#                 f"{server_account} must be either the to: {hive_op.to_account} "
#                 f"or from: {hive_op.from_account} of the hive_op"
#             )

#         super().__init__()
#         if treasury_account == hive_op.to_account and server_account == hive_op.from_account:
#             # Server to Treasury transfer
#             debit = AssetAccount(
#                 name="Treasury Hive",
#                 sub=hive_op.to_account,
#             )
#             credit = AssetAccount(
#                 name="Customer Deposits Hive",
#                 sub=hive_op.from_account,
#             )
#         elif treasury_account == hive_op.from_account and server_account == hive_op.to_account:
#             # Treasury to Server transfer
#             debit = AssetAccount(
#                 name="Customer Deposits Hive",
#                 sub=hive_op.to_account,
#             )
#             credit = AssetAccount(
#                 name="Treasury Hive",
#                 sub=hive_op.from_account,
#             )
#         elif hive_op.to_account == server_account:
#             # Customer deposit
#             debit = AssetAccount(
#                 name="Customer Deposits Hive",
#                 sub=hive_op.to_account,
#             )
#             credit = LiabilityAccount(
#                 name="Customer Liability Hive",
#                 sub=hive_op.from_account,
#             )
#         elif hive_op.from_account == server_account:
#             # Customer withdrawal
#             debit = LiabilityAccount(
#                 name="Customer Liability Hive",
#                 sub=hive_op.to_account,
#             )
#             credit = AssetAccount(
#                 name="Customer Deposits Hive",
#                 sub=hive_op.from_account,
#             )
#         else:
#             # This code should never be reached because of the earlier check
#             # but it's here for safety
#             raise ValueError(
#                 f"Server account {server_account} is not involved in the "
#                 f"transaction ({hive_op.to_account}, {hive_op.from_account} {hive_op.d_memo})"
#             )
#         self.group_id = hive_op.group_id
#         self.timestamp = hive_op.timestamp
#         self.description = hive_op.d_memo
#         self.unit = hive_op.unit
#         self.amount = hive_op.amount_decimal
#         self.conv = hive_op.conv
#         self.debit = debit
#         self.credit = credit
#         self.op = hive_op


def draw_t_diagram(entry: LedgerEntry) -> str:
    """
    Draws a T-diagram for a LedgerEntry, showing account names, sub-values, memo, and conversion values.
    """
    # Extract fields
    debit_name = entry.debit.name
    debit_sub = entry.debit.sub or ""
    credit_name = entry.credit.name
    credit_sub = entry.credit.sub or ""
    description = entry.description
    amount = entry.amount
    unit = entry.unit.value if entry.unit else ""
    conv = entry.conv

    # Truncate description if too long
    max_desc_len = 50
    if len(description) > max_desc_len:
        description = description[: max_desc_len - 3] + "..."

    # Define column widths
    account_width = max(len(debit_name), len(credit_name), 20)
    sub_width = max(len(debit_sub), len(credit_sub), 10)
    total_width = account_width * 2 + sub_width * 2 + 7  # 7 for borders and spaces

    # Build T-diagram
    lines = []

    # Header
    lines.append("=" * total_width)
    lines.append(f"{'T-Diagram':^{total_width}}")
    lines.append("=" * total_width)

    # Account headers
    lines.append(
        f"| {'Debit':<{account_width + sub_width + 2}} | {'Credit':<{account_width + sub_width + 2}} |"
    )
    lines.append(
        f"| {'-' * (account_width + sub_width + 2)} | {'-' * (account_width + sub_width + 2)} |"
    )

    # Account names and sub-values
    lines.append(
        f"| {debit_name:<{account_width}} ({debit_sub:<{sub_width}}) | "
        f"{credit_name:<{account_width}} ({credit_sub:<{sub_width}}) |"
    )

    # Amount and unit
    lines.append(
        f"| {amount:>{account_width}.3f} {unit:<{sub_width}} | "
        f"{amount:>{account_width}.3f} {unit:<{sub_width}} |"
    )

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
