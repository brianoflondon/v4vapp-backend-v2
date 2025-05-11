from typing import Any, override

from nectar import Hive
from pydantic import ConfigDict, Field

# from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, draw_t_diagram
from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import seconds_only_time_diff
from v4vapp_backend_v2.hive.hive_extras import decode_memo
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_base import OpBase


class TransferBase(OpBase):
    from_account: AccNameType = Field(alias="from")
    to_account: AccNameType = Field(alias="to")
    amount: AmountPyd = Field(description="Amount being transferred")
    memo: str = Field("", description="Memo associated with the transfer")
    conv: CryptoConv = CryptoConv()
    d_memo: str = Field("", description="Decoded memo string")

    model_config = ConfigDict(populate_by_name=True)
    # Defined as a CLASS VARIABLE outside the

    model_config = ConfigDict(
        populate_by_name=True,
    )

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)
        hive_inst: Hive = hive_event.get("hive_inst", OpBase.hive_inst)
        self.post_process(hive_inst=hive_inst)
        if self.last_quote.get_age() > 600.0:
            self.update_quote_sync(AllQuotes().get_binance_quote())
        self.update_conv()

    def post_process(self, hive_inst: Hive) -> None:
        if self.memo.startswith("#") and hive_inst:
            self.d_memo = decode_memo(memo=self.memo, hive_inst=hive_inst)
        else:
            self.d_memo = self.memo

    @property
    def amount_decimal(self) -> float:
        """Convert string amount to decimal with proper precision"""
        return self.amount.amount_decimal

    @property
    def unit(self) -> Currency:
        """Get the unit of the amount"""
        return self.amount.unit

    @property
    def amount_str(self) -> str:
        return self.amount.__str__()

    @property
    def recurrence_str(self) -> str:
        """
        Generates a string representation of the transfer operation, including the
        sender, recipient, amount, and memo.

        Returns:
            str: A formatted string containing details about the transfer.
        """
        if hasattr(self, "recurrence"):
            return f" Execution: {self.executions} every {self.recurrence} hours"
        if hasattr(self, "remaining_executions"):
            return f" Remaining: {self.remaining_executions}"
        return ""

    @property
    @override
    def log_str(self) -> str:
        time_diff = seconds_only_time_diff(self.timestamp)
        log_str = (
            f"{self.from_account:<17} "
            f"sent {self.amount.fixed_width_str(14)} "
            f"to {self.to_account:<17}{self.recurrence_str} "
            f" - {self.lightning_memo[:30]:>30} "
            f"{time_diff} ago {self.age_str} "
            f"{self.link} {self.op_in_trx:>3}"
        )
        return log_str

    @property
    @override
    def notification_str(self) -> str:
        """
        Generates a notification string summarizing a transfer operation. Adds a flag
        to prevent a link preview.

        Returns:
            str: A formatted string containing details about the transfer, including:
                 - Sender's account as a markdown link.
                 - Amount transferred as a string.
                 - Recipient's account as a markdown link.
                 - Converted USD value and equivalent in satoshis.
                 - Memo associated with the transfer.
                 - A markdown link for additional context.
                 - A hashtag indicating no preview.
        """
        ans = (
            f"{self.from_account.markdown_link} sent {self.amount_str} "
            f"to {self.to_account.markdown_link}{self.recurrence_str} "
            f"{self.conv.notification_str} {self.lightning_memo} {self.markdown_link}{self.age_str} no_preview"
        )
        return ans

    @override
    async def process(self) -> None:
        """
        Processes the transfer operation. This method is a placeholder and should
        be overridden in subclasses to provide specific processing logic.

        Returns:
            None
        """
        await self.lock_op()
        server_account = InternalConfig().config.hive.server_account.name
        treasury_account = InternalConfig().config.hive.treasury_account.name
        funding_account = InternalConfig().config.hive.funding_account.name
        exchange_account = InternalConfig().config.hive.exchange_account.name
        # Check if the transfer is between the server account and the treasury account

        match (self.from_account, self.to_account):
            case (server_account, treasury_account):
                # this is from the server account to the treasury account
                logger.info(
                    f"Transfer from server account to treasury account: {self.from_account} -> {self.to_account}"
                )
                pass
            case (treasury_account, server_account):
                # This is a transfer from the server account
                logger.info(
                    f"Transfer from treasury account to server account: {self.from_account} -> {self.to_account}"
                )
                pass
            case (funding_account, treasury_account):
                # This is a transfer between two different accounts
                logger.info(
                    f"Transfer from funding account to treasury account: {self.from_account} -> {self.to_account}"
                )
                # ledger_entry = LedgerEntry().owners_loan(self)
                # draw_t_diagram(ledger_entry)
                pass
            case (treasury_account, funding_account):
                # This is a transfer between the treasury account and the funding account
                logger.info(
                    f"Transfer from treasury account to funding account: {self.from_account} -> {self.to_account}"
                )
                pass
            case (treasury_account, exchange_account):
                # This is a transfer between the treasury account and the exchange account
                logger.info(
                    f"Transfer from treasury account to exchange account: {self.from_account} -> {self.to_account}"
                )
                pass
            case (exchange_account, treasury_account):
                # This is a transfer between two different accounts
                logger.info(
                    f"Transfer from exchange account to treasury account: {self.from_account} -> {self.to_account}"
                )
                pass
            case (server_account, _):
                # This is a transfer from the server account to any other account
                # except the treasury, funding or exchange accounts
                logger.info(
                    f"Transfer from server account to another account: {self.from_account} -> {self.to_account}"
                )
                pass
            case (_, server_account):
                # This is a transfer to the server account from any other account
                # except the treasury, funding or exchange accounts
                logger.info(
                    f"Transfer to server account from another account: {self.from_account} -> {self.to_account}"
                )
                pass
            case _:
                # This is a transfer between two different accounts
                logger.info(
                    f"Transfer between two different accounts: {self.from_account} -> {self.to_account}"
                )
                pass

        # HIVE to LND Transfers
        if self.to_account == server_account:
            if self.d_memo.startswith("lnbc"):
                try:
                    self.lightning_memo = await decode_any_lightning_string(
                        input=self.d_memo, ignore_limits=True
                    )
                    print(f"Decoded Lightning memo: {self.lightning_memo}")
                    await self.unlock_op()
                except Exception as e:
                    self.lightning_memo = f"Error decoding: {e}"

        # Deposit of Hive to Treasury
        elif self.to_account == treasury_account:
            if self.d_memo.startswith("lnbc"):
                try:
                    self.lightning_memo = await decode_any_lightning_string(
                        input=self.d_memo, ignore_limits=True
                    )
                    print(f"Decoded Lightning memo: {self.lightning_memo}")
                    await self.unlock_op()
                except Exception as e:
                    self.lightning_memo = f"Error decoding: {e}"

        await self.unlock_op()


class Transfer(TransferBase):
    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)
        super().__init__(**hive_event)
