import re
from typing import Any, override

from nectar.hive import Hive
from pydantic import ConfigDict, Field

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    detect_keepsats,
    detect_paywithsats,
    find_short_id,
    paywithsats_amount,
    seconds_only_time_diff,
)
from v4vapp_backend_v2.hive.hive_extras import decode_memo
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_base import OpBase


class TransferBase(OpBase):
    """
    TransferBase is a subclass of OpBase that represents a transfer operation in the system.
    It encapsulates details about the transfer, including sender, recipient, amount, memo,
    and additional metadata. The class provides methods and properties to process and
    retrieve information about the transfer.

    Attributes:
        from_account (AccNameType): The account initiating the transfer.
        to_account (AccNameType): The account receiving the transfer.
        amount (AmountPyd): The amount being transferred, with precision and unit.
        memo (str): A memo associated with the transfer, defaulting to an empty string.
        conv (CryptoConv): A conversion object for cryptocurrency-related operations.
        d_memo (str): A decoded memo string, if applicable.

    Class Variables:
        model_config (ConfigDict): Configuration for the model, enabling population by alias.

    Methods:
        __init__(**hive_event: Any): Initializes the TransferBase object, processes the
            transfer details, and updates conversion rates if necessary.
        post_process(hive_inst: Hive): Processes the memo to decode it if it starts with
            a hash (#) and a Hive instance is provided.

    Properties:
        amount_decimal (float): Converts the string amount to a decimal with proper precision.
        unit (Currency): Retrieves the unit of the amount.
        amount_str (str): Returns the string representation of the amount.
        recurrence_str (str): Generates a string representation of the transfer's recurrence
            details, if applicable.
        log_str (str): A formatted string summarizing the transfer operation for logging
            purposes. Overrides the base class implementation.
        notification_str (str): A formatted string summarizing the transfer operation for
            notifications, including sender, recipient, amount, memo, and additional context.
            Overrides the base class implementation.
    """

    from_account: AccNameType = Field(alias="from")
    to_account: AccNameType = Field(alias="to")
    amount: AmountPyd = Field(description="Amount being transferred")
    memo: str = Field("", description="Memo associated with the transfer")
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
        if not self.amount:
            raise ValueError("Amount is required for transfer operations")

    def post_process(self, hive_inst: Hive) -> None:
        """
        Post-processes the memo field and decodes it if necessary.
        This method checks the `d_memo` and `memo` attributes of the instance.
        If `d_memo` exists and does not start with a "#", the method exits early.
        Otherwise, if `memo` starts with a "#" and a `Hive` instance is provided,
        the method decodes the `memo` using the `decode_memo` function and assigns
        the result to `d_memo`. If these conditions are not met, `d_memo` is set
        to the value of `memo`.
        Args:
            hive_inst (Hive): An instance of the Hive class used for decoding the memo.
        """
        if not self.memo:
            self.d_memo = ""
            return
        if (
            self.d_memo
            and not self.d_memo.startswith(
                "#"
            )  # This catches d_memos which legitimately start with a #
            or self.d_memo
            and self.d_memo
            != self.memo  # This catches d_memos which are already decoded and start with #
        ):
            return
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

        Overridden in the sub classes for recurrent_transfer and fill_recurrent_transfer.

        Returns:
            str: A formatted string containing details about the transfer.
        """
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
        if self.conv:
            conversion_str = self.conv.notification_str
        else:
            conversion_str = ""
        ans = (
            f"{self.from_account.markdown_link} sent {self.amount_str} "
            f"to {self.to_account.markdown_link}{self.recurrence_str} "
            f"{conversion_str} {self.lightning_memo} {self.markdown_link}{self.age_str} no_preview"
        )
        return ans

    @property
    def lightning_memo(self) -> str:
        """
        Removes and shortens a lightning invoice from a memo for output.

        Returns:
            str: The shortened memo string.
        """
        # Regex pattern to capture 'lnbc' followed by numbers and one letter
        if not self.d_memo:
            return ""
        pattern = r"(lnbc\d+[a-zA-Z])"
        match = re.search(pattern, self.d_memo)
        if match:
            # Replace the entire memo with the matched lnbc pattern
            memo = f"⚡️{match.group(1)}...{self.d_memo[-5:]}"
        else:
            memo = f"💬{self.d_memo}"
        return memo

    @property
    def extract_reply_short_id(self) -> str:
        """
        Determines if the transfer is a reply to another transfer.

        Returns:
            str: The short_id if it is found or an empty string.
        """
        if not self.d_memo:
            return ""
        short_id = find_short_id(self.d_memo)
        if not short_id:
            return ""
        return short_id

    @property
    def keepsats(self) -> bool:
        """
        Checks if the transfer memo indicates a keepsats operation.

        Returns:
            bool: True if the memo indicates a keepsats operation, False otherwise.
        """
        return detect_keepsats(self.d_memo)

    @property
    def paywithsats(self) -> bool:
        """
        Checks if the transfer memo indicates a paywithsats operation.

        Returns:
            bool: True if the memo indicates a paywithsats operation, False otherwise.
        """
        return detect_paywithsats(self.d_memo)

    @property
    def paywithsats_amount(self) -> int:
        """
        Extracts and returns the 'paywithsats' amount from the memo if present.
        This is in sats, not msats.

        Returns:
            int: The amount specified in the memo after 'paywithsats:', or 0 if not present or not applicable.

        Notes:
            - The memo is expected to be in the format "paywithsats:amount".
            - If 'paywithsats' is not enabled or the memo does not match the expected format, returns 0.
        """
        return paywithsats_amount(self.d_memo)

    async def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion for the transaction.

        If the subclass has a `conv` object, update it with the latest quote.
        If a quote is provided, it sets the conversion to the provided quote.
        If no quote is provided, it uses the last quote to set the conversion.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, uses the last quote.
        """

        if not quote:
            quote = await TrackedBaseModel.nearest_quote(self.timestamp)
        self.conv = CryptoConversion(amount=self.amount, quote=quote).conversion
        if self.change_amount:
            self.change_conv = CryptoConversion(amount=self.change_amount, quote=quote).conversion

    def max_send_amount_msats(self) -> int:
        """
        Calculate the maximum amount that can be sent after deducting fees and estimating Lightning fees.
        This method calculates the maximum amount that can be sent in millisatoshis (msats) based on
        the Hive or HBD value of the transfer, the conversion to millisatoshis,

        Args:
            self (TrackedTransfer): The tracked transfer object.

        Returns:
            int: The maximum amount that can be sent after fees and fee estimates.
        """
        if self.paywithsats:
            return self.paywithsats_amount * 1_000
        lnd_config = InternalConfig().config.lnd_config
        amount_sent = self.conv.msats - self.conv.msats_fee
        max_payment_amount = amount_sent - lnd_config.lightning_fee_base_msats
        fee_estimate = int(max_payment_amount * lnd_config.lightning_fee_estimate_ppm / 1_000_000)
        return max_payment_amount - fee_estimate


class Transfer(TransferBase):
    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)
