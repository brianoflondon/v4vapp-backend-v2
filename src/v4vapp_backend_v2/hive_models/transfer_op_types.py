import asyncio
from datetime import datetime
from typing import Any, ClassVar, Dict, Optional

from beem import Hive  # type: ignore
from beem.amount import Amount  # type: ignore
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.hive.hive_extras import decode_memo, get_event_id


class AmountPyd(BaseModel):
    amount: str
    nai: str
    precision: int

    @property
    def decimal_amount(self) -> float:
        """Convert string amount to decimal with proper precision"""
        return float(self.amount) / (10**self.precision)

    @property
    def beam(self) -> Amount:
        return Amount(self.amount, self.nai)


class Transfer(BaseModel):
    id: str = Field(alias="_id")
    amount: AmountPyd
    block_num: int
    from_account: str = Field(alias="from")
    memo: str
    op_in_trx: int = 0
    timestamp: datetime
    to_account: str = Field(alias="to")
    trx_id: str
    trx_num: int
    type: str

    model_config = ConfigDict(
        populate_by_name=True,
    )

    def __init__(self, **hive_event: Any) -> None:
        if "id" not in hive_event and "_id" in hive_event:
            trx_id = hive_event.get("trx_id", "")
            op_in_trx = hive_event.get("op_in_trx", 0)
            if op_in_trx == 0:
                hive_event["id"] = str(trx_id)
            else:
                hive_event["id"] = str(f"{trx_id}_{op_in_trx}")

        super().__init__(**hive_event)


class TransferEnhanced(Transfer):
    d_memo: str = ""
    conv: CryptoConv = CryptoConv()

    model_config = ConfigDict(populate_by_name=True)
    # Definied as a CLASS VARIABLE outside the
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
        self.conv = CryptoConversion(amount=self.amount.beam, quote=self.last_quote).conversion
