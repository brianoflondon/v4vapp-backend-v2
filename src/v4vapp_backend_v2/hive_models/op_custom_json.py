import json
import re
from typing import List

from pydantic import Field

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    detect_hbd,
    detect_keepsats,
    paywithsats_amount,
)
from v4vapp_backend_v2.hive.hive_extras import process_user_memo
from v4vapp_backend_v2.hive_models.custom_json_data import (
    CustomJsonData,
    custom_json_test_data,
    custom_json_test_id,
)
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.process.lock_str_class import CustIDType


class CustomJson(OpBase):
    cj_type: str = Field(alias="type")
    cj_id: str = Field(alias="id")
    json_data: CustomJsonData = Field(alias="json")
    required_auths: List[str]
    required_posting_auths: List[str]

    cust_id: CustIDType = Field(
        default="", description="Customer ID determined from the `required_auths` field"
    )

    # Extra Fields

    def __init__(self, **data):
        """
        Initializes an instance of the class with the provided data.

        Args:
            **data: Arbitrary keyword arguments containing the data to initialize the instance.

        Raises:
            ValueError: If the provided JSON data is invalid or cannot be parsed.

        Notes:
            - If `custom_json_test_data` returns a `SpecialJsonType`, the `json` field in the data
              is validated and parsed using the `model_validate` method of `SpecialJsonType`.
            - If the `json_data` attribute contains a `sats` field and `last_quote` is available,
              a cryptocurrency conversion is performed using the `CryptoConversion` class.
              ONLY if `last_quote` is not None and `last_quote.hive_hbd` is not 0.
            - The `super().__init__(**data)` call initializes the base class with the provided data.
            - The `conv` attribute is set to `None` by default.
        """
        SpecialJsonType = custom_json_test_data(data)
        if SpecialJsonType is not None:
            try:
                if isinstance(data["json"], str):
                    json_data = json.loads(data["json"])
                else:
                    json_data = data["json"]
                json_object = SpecialJsonType.model_validate(json_data)
                data["json"] = json_object
            except ValueError as e:
                raise ValueError(
                    f"Invalid JSON data for operation ID {data['id']}: {data['json']} - {e}"
                )
        super().__init__(**data)

        # Only if the from is the required auth account OR the server can we send sats around
        # The customer is the from account.
        try:
            if self.json_data and (
                hasattr(self.json_data, "from_account") or hasattr(self.json_data, "to_account")
            ):
                if self.required_auths and self.required_auths[0]:
                    if (
                        self.json_data.from_account in self.required_auths
                        or self.required_auths[0]
                        in InternalConfig().config.hive.server_account_names
                    ):
                        self.cust_id = self.json_data.from_account

                if self.conv is not None and self.conv.is_unset():
                    if (
                        getattr(self.json_data, "sats", None) is not None
                        or getattr(self.json_data, "msats", None) is not None
                    ):
                        quote = TrackedBaseModel.last_quote
                        if getattr(self.json_data, "sats", None) is not None and not hasattr(
                            self.json_data, "msats"
                        ):
                            setattr(
                                self.json_data, "msats", getattr(self.json_data, "sats", 0) * 1000
                            )

                        msats_value = (
                            getattr(self.json_data, "msats", 0)
                            if hasattr(self.json_data, "msats")
                            else 0
                        )
                        self.conv = CryptoConversion(
                            value=msats_value,
                            conv_from=Currency.MSATS,
                            quote=quote,
                        ).conversion
        except Exception as e:
            logger.error(
                f"Error initializing CustomJson: {e}",
                extra={"notification": False, **self.log_extra},
            )

    @property
    def is_watched(self) -> bool:
        """
        Check if the transfer is to a watched user.

        Returns:
            bool: True if the transfer is to a watched user, False otherwise.
        """
        if (
            self.required_auths
            and self.required_auths[0] in InternalConfig().config.hive.server_account_names
        ):
            return True
        if OpBase.watch_users:
            if custom_json_test_id(self.cj_id):
                # Check if the transfer is to a watched user
                if self.json_data.to_account in OpBase.watch_users:
                    return True
                # Check if the transfer is from a watched user
                if self.json_data.from_account in OpBase.watch_users:
                    return True
        return False

    # MARK: Methods to surface CustomJsonData if it exists
    @property
    def paywithsats(self) -> bool:
        """
        Checks if the transfer memo indicates a paywithsats operation.

        Returns:
            bool: True if there is a memo, custom_json operations are always paywithsats.
        """
        return True if self.memo else False

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
        return paywithsats_amount(self.memo)

    def max_send_amount_msats(self) -> int:
        """
        Returns the maximum amount in msats that can be sent.

        Returns:
            int: The maximum amount in msats.
        """
        msats = getattr(self.json_data, "msats", None)
        if self.json_data and msats is not None:
            return msats
        return 0

    @property
    def memo(self) -> str:
        """
        Returns the memo associated with the transfer.

        Returns:
            str: The memo string.
        """
        if hasattr(self.json_data, "memo"):
            return self.json_data.memo
        return ""

    @property
    def d_memo(self) -> str:
        """
        Returns the decoded memo associated with the transfer.
        CustomJsonData doesn't have encoded memo, so just return the memo.

        Returns:
            str: The decoded memo string.
        """
        return self.memo

    @property
    def user_memo(self) -> str:
        """
        Returns the user memo, which is the decoded memo if available,
        otherwise returns the original memo.

        Returns:
            str: The user memo.
        """
        # this is where #clean needs to be evaluated
        return process_user_memo(self.memo)

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
    def detect_hbd(self) -> bool:
        """
        Checks if the transfer is in HBD (Hive Backed Dollar).

        Returns:
            bool: True if the transfer is in HBD, False otherwise.
        """
        if hasattr(self.json_data, "memo"):
            return detect_hbd(self.json_data.memo)
        return False

    @property
    def to_account(self) -> str:
        """
        Returns the account to which the transfer is made.

        Returns:
            str: The account to which the transfer is made.
        """
        if hasattr(self.json_data, "to_account"):
            return self.json_data.to_account
        return ""

    @property
    def from_account(self) -> str:
        """
        Returns the account from which the transfer is made.

        Returns:
            str: The account from which the transfer is made.
        """
        if hasattr(self.json_data, "from_account"):
            return self.json_data.from_account
        return ""

    @property
    def keepsats(self) -> bool:
        """
        Checks if the transfer memo indicates a keepsats operation.

        Returns:
            bool: True if the memo indicates a keepsats operation, False otherwise.
        """
        return detect_keepsats(self.memo)

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
        if (
            getattr(self.json_data, "sats", None) is not None
            or getattr(self.json_data, "msats", None) is not None
        ):
            if not quote:
                quote = await TrackedBaseModel.nearest_quote(self.timestamp)
            if getattr(self.json_data, "sats", None) is not None and not hasattr(
                self.json_data, "msats"
            ):
                setattr(self.json_data, "msats", getattr(self.json_data, "sats", 0) * 1000)

            msats_value = (
                getattr(self.json_data, "msats", 0) if hasattr(self.json_data, "msats") else 0
            )
            self.conv = CryptoConversion(
                value=msats_value,
                conv_from=Currency.MSATS,
                quote=quote,
            ).conversion

    @property
    def log_str(self) -> str:
        # check if self.json_data has method log_str
        if hasattr(self.json_data, "log_str"):
            return f"{self.json_data.log_str} {self.link}"
        return f"{self.block_num:,} | {self.age:.2f} | {self.timestamp:%Y-%m-%d %H:%M:%S} {self.realm:<8} | {self.cj_id[:19]:>20} | {self.op_in_trx:<3} | {self.link}"

    @property
    def notification_str(self) -> str:
        if hasattr(self.json_data, "notification_str"):
            return f"{self.json_data.notification_str} {self.markdown_link}"
        return f"{self.block_num:,} | {self.age:.2f} | {self.timestamp:%Y-%m-%d %H:%M:%S} {self.realm:<8} | {self.cj_id[:19]:>20} | {self.op_in_trx:<3} | {self.markdown_link}"

    def notification_str(self) -> str:
        if hasattr(self.json_data, "notification_str"):
            return f"{self.json_data.notification_str} {self.markdown_link}"
        return f"{self.block_num:,} | {self.age:.2f} | {self.timestamp:%Y-%m-%d %H:%M:%S} {self.realm:<8} | {self.cj_id[:19]:>20} | {self.op_in_trx:<3} | {self.markdown_link}"
