import asyncio
from datetime import datetime, timezone
from typing import Any, ClassVar

from beem import Hive  # type: ignore
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import seconds_only
from v4vapp_backend_v2.hive.hive_extras import decode_memo, get_hive_block_explorer_link
from v4vapp_backend_v2.hive_models.op_base import OpBase

from .amount_pyd import AmountPyd


class TransferRaw(OpBase):
    # trx_id: str
    # op_in_trx: int = 0
    # type: str
    amount: AmountPyd
    block_num: int
    from_account: str = Field(alias="from")
    memo: str
    timestamp: datetime
    to_account: str = Field(alias="to")
    trx_num: int

    model_config = ConfigDict(
        populate_by_name=True,
    )

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)


class Transfer(TransferRaw):
    d_memo: str = ""
    conv: CryptoConv = CryptoConv()

    model_config = ConfigDict(populate_by_name=True)
    # Defined as a CLASS VARIABLE outside the
    last_quote: ClassVar[QuoteResponse] = QuoteResponse()

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)
        hive_inst: Hive | None = hive_event.get("hive_inst", None)
        self.post_process(hive_inst=hive_inst)
        if self.last_quote.get_age() > 600.0:
            # raise ValueError("HiveTransaction.last_quote is too old")
            try:
                asyncio.run(self.update_quote())
            except RuntimeError:
                loop = asyncio.get_running_loop()
                loop.run_until_complete(self.update_quote())
        self.update_conv()

    def post_process(self, hive_inst: Hive | None = None) -> None:
        if self.memo.startswith("#") and hive_inst:
            self.d_memo = decode_memo(memo=self.memo, hive_inst=hive_inst)
        else:
            self.d_memo = self.memo

    @classmethod
    async def update_quote(cls, quote: QuoteResponse | None = None) -> None:
        """
        Asynchronously updates the last quote for the class.

        If a quote is provided, it sets the last quote to the provided quote.
        If no quote is provided, it fetches all quotes and sets the last quote
        to the fetched quote.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, fetches all quotes.

        Returns:
            None
        """
        if quote:
            cls.last_quote = quote
        else:
            all_quotes = AllQuotes()
            await all_quotes.get_all_quotes()
            cls.last_quote = all_quotes.quote

    def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion for the transaction.

        If a quote is provided, it sets the conversion to the provided quote.
        If no quote is provided, it uses the last quote to set the conversion.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, uses the last quote.
        """
        self.conv = CryptoConversion(
            amount=self.amount.beam, quote=self.last_quote
        ).conversion

    @property
    def amount_decimal(self) -> float:
        """Convert string amount to decimal with proper precision"""
        return self.amount.amount_decimal

    @property
    def amount_str(self) -> str:
        return self.amount.__str__()

    @property
    def log_str(self) -> str:
        log_link = get_hive_block_explorer_link(self.trx_id, markdown=False)
        time_diff = seconds_only(datetime.now(tz=timezone.utc) - self.timestamp)
        log_str = (
            f"{self.from_account:<17} "
            f"sent {self.amount.fixed_width_str(14)} "
            f"to {self.to_account:<17} "
            f" - {self.d_memo[:30]:>30} "
            f"{time_diff} ago "
            f"{log_link} {self.op_in_trx:>3}"
        )
        return log_str

    @property
    def notification_str(self) -> str:
        markdown_link = (
            get_hive_block_explorer_link(self.trx_id, markdown=True) + " no_preview"
        )
        ans = (
            f"{self.from_account} sent {self.amount_str} to {self.to_account} "
            f"(${self.conv.usd:>.2f} {self.conv.sats:,.0f} sats) {self.d_memo} {markdown_link}"
        )
        return ans
