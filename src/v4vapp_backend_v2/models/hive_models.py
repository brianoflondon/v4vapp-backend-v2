import asyncio
import os
import pickle
from datetime import datetime, timezone
from timeit import default_timer as timer
from typing import Any, ClassVar

from beem.amount import Amount  # type: ignore
from colorama import Fore
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion

# from v4vapp_backend_v2.helpers.general_purpose_funcs import (
#     detect_convert_keepsats,
#     detect_keepsats,
#     detect_paywithsats,
# )
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import seconds_only
from v4vapp_backend_v2.helpers.hive_extras import (
    HiveTransactionTypes,
    decode_memo,
    get_blockchain_instance,
    get_event_id,
    get_hive_block_explorer_link,
    get_hive_client,
)


class HiveTransactionFlags(BaseModel):
    encrypted: bool = False
    decrypted: bool = False
    processed: bool = False
    answer_trx_id: str = ""
    hive_result: dict = {}
    pay_result: dict = {}
    streaming: bool = False
    success: bool = False
    confirmed: bool = False
    in_transition: bool = False
    keep_sats: bool = False
    pay_with_keep_sats: bool = False
    fee_sats: float = 0.0
    force_send_sats: int = 0


class HiveTransaction(BaseModel):
    id: str = Field(..., alias="_id")
    trx_id: str
    timestamp: datetime
    type: HiveTransactionTypes
    found: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    op_in_trx: int
    hive_from: str = Field(..., alias="from")
    hive_to: str = Field(..., alias="to")
    amount: dict = {}
    amount_str: str = ""
    amount_decimal: str = ""
    amount_symbol: str = ""
    amount_value: float = 0.0
    conv: CryptoConv = CryptoConv()
    memo: str
    d_memo: str = ""
    d_memo_extra: str = ""
    block_num: int

    # Definied as a CLASS VARIABLE outside the
    last_quote: ClassVar[QuoteResponse] = QuoteResponse()

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **post: Any) -> None:
        post["_id"] = get_event_id(post)
        super().__init__(**post)
        amount = Amount(self.amount)
        self.amount_str = str(amount)
        self.amount_decimal = str(amount.amount_decimal)
        self.amount_symbol = amount.symbol
        self.amount_value = amount.amount
        if self.last_quote.age > 600.0:
            # raise ValueError("HiveTransaction.last_quote is too old")
            try:
                asyncio.run(self.update_quote())
            except RuntimeError:
                loop = asyncio.get_running_loop()
                loop.run_until_complete(self.update_quote())
        self.update_conv()
        hive_inst = post.get("hive_inst", None)
        if "hive_inst" in post:
            self.d_memo = decode_memo(hive_inst=hive_inst, memo=self.memo)
        elif "memo_keys" in post:
            self.d_memo = decode_memo(memo_keys=post["memo_keys"], memo=self.memo)
        else:
            self.d_memo = self.memo

    @property
    def encrypted(self) -> bool:
        return self.d_memo != self.memo

    @property
    def notification_str(self) -> str:
        markdown_link = (
            get_hive_block_explorer_link(self.trx_id, markdown=True) + " no_preview"
        )
        ans = (
            f"{self.hive_from} sent {self.amount_str} to {self.hive_to} "
            f"(${self.conv.usd:>.2f}) {self.d_memo} {markdown_link}"
        )
        return ans

    @property
    def log_str(self) -> str:

        log_link = get_hive_block_explorer_link(self.trx_id, markdown=False)
        time_diff = seconds_only(datetime.now(tz=timezone.utc) - self.timestamp)
        log_str = (
            f"{self.hive_from:<17} "
            f"sent {float(self.amount_decimal):12,.3f} {self.amount_symbol:>4} "
            f"to {self.hive_to:<17} "
            f" - {self.d_memo[:30]:>30} "
            f"{time_diff} ago "
            f"{log_link} {self.op_in_trx:>3}"
        )
        return log_str

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
        amount = Amount(self.amount)
        self.conv = CryptoConversion(amount=amount, quote=self.last_quote).conversion


if __name__ == "__main__":
    internal_config = InternalConfig()
    hive_config = internal_config.config.hive
    memo_keys = hive_config.memo_keys
    hive = get_hive_client(keys=memo_keys)
    # get_current_block_num = hive.get_dynamic_global_properties().get(
    #     "head_block_number"
    # )
    get_current_block_num = 94094293
    blockchain = get_blockchain_instance(hive_instance=hive)
    op_in_trx = 0
    last_trx_id = ""

    all_posts = []
    try:
        if os.path.exists("tests/data/hive/sample_hive_transactions.pkl"):
            with open("tests/data/hive/sample_hive_transactions.pkl", "rb") as f:
                all_posts = pickle.load(f)
        else:
            for post in blockchain.stream(
                opNames=["transfer", "recurrent_transfer"],
                start=get_current_block_num - 250,
                stop=get_current_block_num + 750,
                max_batch_size=20,
            ):

                if last_trx_id == post["trx_id"]:
                    op_in_trx += 1
                else:
                    op_in_trx = 0
                    last_trx_id = post["trx_id"]
                post["op_in_trx"] = op_in_trx
                all_posts.append(post)

            with open("tests/data/hive/sample_hive_transactions.pkl", "wb") as f:
                pickle.dump(all_posts, f)

    except TypeError as e:
        logger.info(f"Error: {e}")

    start = timer()
    for post in all_posts:
        hive_trx = HiveTransaction(**post, hive_inst=hive)
        # asyncio.run(hive_trx.process_hive_event())
        if hive_trx.encrypted:
            logger.info(
                f"{Fore.YELLOW}{hive_trx.id}  {hive_trx.conv.hive:>7.2f} "
                f"{hive_trx.conv.usd:>7.2f} {hive_trx.d_memo}"
            )
        else:
            logger.info(
                f"{hive_trx.id}  {hive_trx.conv.hive:>7.2f} "
                f"{hive_trx.conv.usd:>7.2f} {hive_trx.d_memo}"
            )
        print(hive_trx)

    end = timer()
    logger.info(f"Time taken: {end - start}")
