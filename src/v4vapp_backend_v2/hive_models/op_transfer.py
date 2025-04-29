from typing import Any

from nectar import Hive
from pydantic import ConfigDict, Field

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes
from v4vapp_backend_v2.hive.hive_extras import decode_memo
from v4vapp_backend_v2.hive_models.op_base import OpBase, TransferBase


class Transfer(TransferBase):
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

    d_memo: str = Field("", description="Decoded memo string")
    conv: CryptoConv = CryptoConv()

    model_config = ConfigDict(populate_by_name=True)
    # Defined as a CLASS VARIABLE outside the

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
