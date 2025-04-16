from datetime import datetime
from typing import Any, ClassVar

from nectar import Hive
from pydantic import ConfigDict, Field

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import seconds_only_time_diff
from v4vapp_backend_v2.hive.hive_extras import decode_memo
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.op_base import OpBase

from .amount_pyd import AmountPyd


class TransferRaw(OpBase):
    from_account: AccNameType = Field(alias="from")
    to_account: AccNameType = Field(alias="to")
    amount: AmountPyd
    memo: str
    timestamp: datetime

    model_config = ConfigDict(
        populate_by_name=True,
    )

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)


class Transfer(TransferRaw):
    """
    Transfer class represents a transaction operation with additional processing
    and conversion functionalities.

    Important to note: the `last_quote` class variable is used to store the last quote
    and needs to be fetched asyncronously. The `update_quote` method is used to update
    the last quote.

    This class extends the TransferRaw class and provides methods for handling
    transaction details, updating conversion rates, and generating log and
    notification strings. It also includes mechanisms for decoding memos and
    validating the age of the last quote.

    Attributes:
        d_memo (str): Decoded memo string. Defaults to an empty string.
        conv (CryptoConv): Conversion object for the transaction. Defaults to a new CryptoConv instance.
        model_config (ConfigDict): Configuration for the model, with `populate_by_name` set to True.
        last_quote (ClassVar[QuoteResponse]): Class-level variable to store the last quote.

    Methods:
        __init__(**hive_event: Any) -> None:
            Initializes the Transfer object, processes the hive event, and updates the conversion.

        post_process(hive_inst: Hive | None = None) -> None:
            Processes the memo field and decodes it if necessary.

        update_quote(cls, quote: QuoteResponse | None = None) -> None:
            Asynchronously updates the last quote for the class. If no quote is provided,
            fetches all quotes and sets the last quote to the fetched quote.

        update_conv(quote: QuoteResponse | None = None) -> None:
            Updates the conversion for the transaction using the provided quote or the last quote.

        amount_decimal -> float:
            Property that converts the string amount to a decimal with proper precision.

        amount_str -> str:
            Property that returns the string representation of the amount.

        log_str -> str:
            Property that generates a log string with transaction details, including a link
            to the Hive block explorer.

        notification_str -> str:
            Property that generates a notification string with transaction details, including
            a markdown link to the Hive block explorer.
    """

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
            self.update_quote_sync(AllQuotes().get_binance_quote())
        self.update_conv()

    def post_process(self, hive_inst: Hive | None = None) -> None:
        if self.memo.startswith("#") and hive_inst:
            self.d_memo = decode_memo(memo=self.memo, hive_inst=hive_inst)
        else:
            self.d_memo = self.memo

    @classmethod
    def update_quote_sync(cls, quote: QuoteResponse) -> None:
        """
        Synchronously updates the last quote for the class.

        Args:
            quote (QuoteResponse | None): The quote to update.

        Returns:
            None
        """
        cls.last_quote = quote

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
        quote = quote or self.last_quote
        self.conv = CryptoConversion(amount=self.amount.beam, quote=quote).conversion

    @property
    def is_watched(self) -> bool:
        """
        Check if the transfer is to a watched user.

        Returns:
            bool: True if the transfer is to a watched user, False otherwise.
        """
        if Transfer.watch_users:
            if (
                self.to_account in Transfer.watch_users
                or self.from_account in Transfer.watch_users
            ):
                return True
        return False

    @property
    def amount_decimal(self) -> float:
        """Convert string amount to decimal with proper precision"""
        return self.amount.amount_decimal

    @property
    def amount_str(self) -> str:
        return self.amount.__str__()

    @property
    def log_str(self) -> str:
        time_diff = seconds_only_time_diff(self.timestamp)
        log_str = (
            f"{self.from_account:<17} "
            f"sent {self.amount.fixed_width_str(14)} "
            f"to {self.to_account:<17} "
            f" - {self.d_memo[:30]:>30} "
            f"{time_diff} ago {self.age_str}"
            f"{self.link} {self.op_in_trx:>3}"
        )
        return log_str

    @property
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
            f"{self.from_account.markdown_link} sent {self.amount_str} to {self.to_account.markdown_link} "
            f"{self.conv.notification_str} {self.d_memo} {self.markdown_link}{self.age_str} no_preview"
        )
        return ans


# TODO #45 Add Recurrent Transfer type
