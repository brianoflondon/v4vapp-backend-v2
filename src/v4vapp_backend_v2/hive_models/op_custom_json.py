import json
from typing import List

from pydantic import Field

from v4vapp_backend_v2.actions.cust_id_class import CustIDType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import detect_paywithsats
from v4vapp_backend_v2.hive_models.custom_json_data import (
    CustomJsonData,
    custom_json_test_data,
    custom_json_test_id,
)
from v4vapp_backend_v2.hive_models.op_base import OpBase


class CustomJson(OpBase):
    type: str
    cj_id: str = Field(alias="id")
    json_data: CustomJsonData = Field(alias="json")
    required_auths: List[str]
    required_posting_auths: List[str]

    cust_id: CustIDType = Field(
        default="", alias="Customer ID determined from the `required_auths` field"
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
            if self.is_watched and self.json_data and hasattr(self.json_data, "from_account"):
                if self.required_auths and self.required_auths[0]:
                    if (
                        self.json_data.from_account == self.required_auths[0]
                        or self.required_auths[0]
                        in InternalConfig().config.hive.server_account_names
                    ):
                        self.cust_id = self.json_data.from_account

                if self.conv.sats_hbd == 0:
                    if getattr(self.json_data, "sats", None) is not None:
                        if (
                            TrackedBaseModel.last_quote
                            and not TrackedBaseModel.last_quote.hive_hbd == 0
                            and hasattr(self.json_data, "sats")
                        ):
                            self.conv = CryptoConversion(
                                value=getattr(self.json_data, "sats", 0),
                                conv_from=Currency.SATS,
                                quote=TrackedBaseModel.last_quote,
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

    @property
    def paywithsats(self) -> bool:
        """
        Checks if the transfer memo indicates a paywithsats operation.

        Returns:
            bool: True if the memo indicates a paywithsats operation, False otherwise.
        """
        return detect_paywithsats(self.json_data.memo)

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

        if getattr(self.json_data, "sats", None) is not None:
            if self.conv.sats_hbd == 0:
                if not quote:
                    quote = await TrackedBaseModel.nearest_quote(self.timestamp)
                self.conv = CryptoConversion(
                    value=getattr(self.json_data, "sats", 0),
                    conv_from=Currency.SATS,
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
