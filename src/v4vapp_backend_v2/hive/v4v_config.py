import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import List

import httpx
from nectar.account import Account
from nectar.hive import Hive
from pydantic import BaseModel, Field, model_validator

from v4vapp_backend_v2.config.setup import InternalConfig, logger

# from helpers.cryptoprices import CryptoConversion, CryptoPrices
from v4vapp_backend_v2.hive.hive_extras import get_hive_client, get_verified_hive_client

CONFIG_ROOT_KEY = "v4vapp_hiveconfig"
ICON = "⚙️v"


class V4VConfigRateLimits(BaseModel):
    """Class for holding the hourly rate limits for using the Lightning exchange"""

    hours: int = Field(0, description="Number of hours for the rate limit.")
    sats: Decimal = Field(Decimal(0), description="Limit in satoshis for the rate limit.")

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_limit_field(cls, data):
        """Handle legacy 'limit' field from Hive data and map it to 'sats'"""
        if isinstance(data, dict) and "limit" in data and "sats" not in data:
            # Map 'limit' to 'sats' for backward compatibility
            data = data.copy()  # Don't modify original
            data["sats"] = data.pop("limit")
        return data

    def __repr__(self) -> str:
        return super().__repr__()

    def md_table(self, hive: float, HBD: float) -> str:
        return (
            f"| {self.hours:>3.0f} hours | {self.sats:>7,.0f} | "
            f"{hive:>7,.1f} Hive | {HBD:>7,.1f} HBD |\n"
        )


class V4VConfigData(BaseModel):
    """Class for fetching and storing some config settings on Hive"""

    hive_return_fee: Decimal = Field(
        Decimal(0.002), description="Fee for returning Hive transactions."
    )
    conv_fee_percent: Decimal = Field(
        Decimal(0.015), description="Conversion fee percentage for transactions."
    )
    conv_fee_sats: Decimal = Field(
        Decimal(50), description="Conversion fee in satoshis for transactions."
    )
    minimum_invoice_payment_sats: Decimal = Field(
        Decimal(250), description="Minimum invoice payment in satoshis."
    )
    maximum_invoice_payment_sats: Decimal = Field(
        Decimal(100_000), description="Maximum invoice payment in satoshis."
    )
    # Used by `reply_with_hive` when a lightning payment is being returned.  If the
    # received sats amount is below this threshold we prefer to send a custom_json
    # notification instead of a Hive transfer.  This was historically a hard‑coded
    # value (50/500 sats) so exposing it here makes it adjustable via Hive config.
    force_custom_json_payment_sats: Decimal = Field(
        Decimal(500), description="Below this amount (sats) force custom_json reply instead of Hive transfer."
    )
    max_acceptable_lnd_fee_msats: Decimal = Field(
        Decimal(500_000), description="Maximum acceptable Lightning Network fee in millisatoshis."
    )
    closed_get_lnd: bool = Field(
        False, description="Flag to indicate if the LND gateway is closed."
    )
    closed_get_hive: bool = Field(
        False, description="Flag to indicate if the Hive gateway is closed."
    )
    v4v_frontend_iri: str = Field("", description="IRI for the V4V frontend.")
    v4v_api_iri: str = Field("", description="IRI for the V4V API.")
    v4v_fees_streaming_sats_to_hive_percent: Decimal = Field(
        Decimal(0.03), description="Fee percentage for streaming sats to Hive."
    )
    lightning_rate_limits: List[V4VConfigRateLimits] = Field(
        default_factory=lambda: [
            V4VConfigRateLimits(hours=4, sats=Decimal(200_000 * 2)),
            V4VConfigRateLimits(hours=72, sats=Decimal(200_000 * 4)),
            V4VConfigRateLimits(hours=168, sats=Decimal(200_000 * 6)),
        ],
        description="Rate limits for Lightning transactions.",
    )
    dynamic_fees_url: str = Field("", description="URL for dynamic fees.")
    dynamic_fees_permlink: str = Field("", description="Permlink for dynamic fees.")
    # server_id: str = Field("", description="Server Hive Account.")
    # treasury_id: str = Field("", description="Treasury Hive Account.")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.check_and_sort_rate_limits()

    def check_and_sort_rate_limits(self) -> tuple[bool, int]:
        """
        Checks if lightning_rate_limits is sorted in ascending order by hours.
        If not, sorts it in place.
        Returns a tuple: (was_sorted, max_hours)
        """
        rate_limits = self.lightning_rate_limits
        was_sorted = all(
            rate_limits[i].hours <= rate_limits[i + 1].hours for i in range(len(rate_limits) - 1)
        )
        if not was_sorted:
            rate_limits.sort(key=lambda rl: rl.hours)
        max_hours = max((rl.hours for rl in rate_limits), default=0)
        return was_sorted, max_hours

    @property
    def max_rate_limit_hours(self) -> int:
        """
        Returns the maximum hours from the lightning_rate_limits.
        """
        return max((rl.hours for rl in self.lightning_rate_limits), default=0)


class V4VConfig:
    _instance = None
    data: V4VConfigData = V4VConfigData()
    hive: Hive | None = None
    server_accname: str | None = None
    timestamp: datetime | None = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(V4VConfig, cls).__new__(cls)
        return cls._instance

    def __init__(self, server_accname: str = "", hive: Hive | None = None, *args, **kwargs):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            super().__init__(*args, **kwargs)
            if not server_accname:
                server_accname = InternalConfig().server_id
            self.server_accname = server_accname
            self.hive = hive or get_hive_client()
            self.fetch()
            logger.info(
                f"{ICON} V4VConfig initialized {self.server_accname}", extra={**self.log_extra}
            )
            return
        if hive:
            self.hive = hive

        if server_accname and self.server_accname != server_accname:
            logger.info(
                f"{ICON} Server account name changed from {self.server_accname} to {server_accname}"
            )
            self.server_accname = server_accname
            self.fetch()

    @property
    def log_extra(self) -> dict:
        return {"hive_config": self.data.model_dump(), "timestamp": self.timestamp}

    def check(self) -> None:
        """
        Checks if the Hive configuration data is valid.

        This method verifies if the configuration data has been fetched and
        if it is not empty.

        If the data is older than 1 hour, it will fetch new data.

        """
        if self.timestamp and self.data and isinstance(self.data, V4VConfigData):
            if (datetime.now(tz=timezone.utc) - self.timestamp).total_seconds() > 3600:
                logger.info(
                    f"{ICON} HiveConfig data is older than 1 hour, fetching new data.",
                    extra={**self.log_extra},
                )
                self.fetch()
        if not self.data:
            logger.warning(
                f"{ICON} HiveConfig data is empty or invalid, fetching new data.",
                extra={**self.log_extra},
            )
            self.fetch()

    def fetch(self) -> bool:
        """
        Synchronizes configuration data from the Hive blockchain.

        This method fetches the configuration settings stored in the Hive blockchain
        for a given server account name. If no server account name is provided, it
        defaults to the first account name specified in the internal configuration.
        The fetched settings are validated and stored in the `self.data` attribute.

        Args:
            server_accname (str): The server account name to fetch the configuration
                from. If not provided, the default account name from the internal
                configuration is used.

        Raises:
            Exception: Logs an error if there is an issue fetching or processing
                the settings from the Hive blockchain.

        Returns:
            bool: True if the settings were successfully fetched and validated, False otherwise.

        Logging:
            - Logs an info message when settings are successfully fetched and validated.
            - Logs an info message if no settings are found for the given account.
            - Logs an error message if an exception occurs during the process.
        """

        try:
            if not self.server_accname:
                # Uses the default values and doesn't check Hive.
                logger.warning(f"{ICON} No server account name provided, using default values.")
                self.data = V4VConfigData()
                return False

            metadata = self._get_posting_metadata()
            if metadata:
                existing_hive_config_raw = metadata.get(CONFIG_ROOT_KEY)
                if existing_hive_config_raw:
                    self.data = V4VConfigData.model_validate(existing_hive_config_raw)
                    self.timestamp = datetime.now(tz=timezone.utc)
                    logger.debug(
                        f"{ICON} Fetched settings from Hive. {self.server_accname}",
                        extra={**self.log_extra},
                    )
                    return True
            else:
                metadata = {}
                logger.warning(
                    f"{ICON} No settings found in Hive. {self.server_accname}",
                )
                self.data = V4VConfigData()
                return False
        except Exception as ex:
            self.data = V4VConfigData()
            logger.warning(
                f"{ICON} Error fetching settings from Hive: {ex} using default values.",
                extra={"hive_config": self.data.model_dump()},
            )
        return True if self.data else False

    async def put(self, hive_client: Hive | None = None) -> None:
        """
        Updates the Hive configuration settings with the provided data.

        This method updates the Hive configuration settings stored in the blockchain
        by comparing the new data with the existing configuration. If the new data
        matches the current settings, no update is performed. Otherwise, the new
        settings are serialized and written to the blockchain.

        Args:
            new_data (HiveConfigData): The new configuration data to be stored in Hive.

        Raises:
            ValueError: If the provided `new_data` is invalid or cannot be serialized.

        Logs:
            - Logs a message if the settings in Hive do not need to change.
            - Logs a message with the transaction ID when the settings are successfully updated.
        """
        if not hive_client:
            hive_client, server_id = await get_verified_hive_client()
        else:
            server_id = self.server_accname or InternalConfig().server_id
        acc = Account(server_id, blockchain_instance=hive_client, lazy=True)
        existing_metadata = self._get_posting_metadata()
        if not existing_metadata:
            existing_metadata = {}
        existing_hive_config_raw = existing_metadata.get(CONFIG_ROOT_KEY)
        if existing_hive_config_raw:
            existing_hive_config = V4VConfigData(**existing_hive_config_raw)
            if self.data == existing_hive_config:
                logger.info(
                    f"{ICON} Settings in Hive do not need to change",
                    extra={"settings": {**self.data.model_dump()}},
                )
                return

        if not self.data or not isinstance(self.data, V4VConfigData):
            logger.warning(f"{ICON} No settings found to update to Hive")
        # If the settings are different, update them in Hive
        # and add the new settings to the metadata
        # Serialize the new settings

        # Fix: Only pop the key if it exists
        if CONFIG_ROOT_KEY in existing_metadata:
            existing_metadata.pop(CONFIG_ROOT_KEY)

        # Custom function to recursively convert Decimals to floats for JSON serialization
        def convert_decimals(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: convert_decimals(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_decimals(item) for item in obj]
            else:
                return obj

        # Get dict with Decimals, then convert to floats
        data_dict = self.data.model_dump()
        converted_data = convert_decimals(data_dict)

        new_meta = {
            **(existing_metadata or {}),
            CONFIG_ROOT_KEY: converted_data,
        }
        self.timestamp = datetime.now(tz=timezone.utc)
        # Overwrite hive params into the Config.
        try:
            logger.info(f"{ICON} Updating Hive settings", extra={"hive_config": new_meta})
            trx = acc.update_account_jsonmetadata(new_meta)
            logger.info(
                f"{ICON} Settings in Hive changed: {trx.get('trx_id')}",
                extra={**self.log_extra, "trx": trx},
            )
            asyncio.create_task(self._update_public_api_server())
            return
        except Exception as ex:
            logger.error(
                f"{ICON} Error updating settings in Hive: {ex} {ex.__class__.__name__}",
                extra={"hive_config": new_meta, **self.log_extra},
            )
            return

    async def _update_public_api_server(self) -> None:
        """
        Asynchronously triggers a configuration reload on the public API server.
        If the `public_api_host` is defined in the internal configuration, this method sends a GET request
        to the `/v1/reload_config` endpoint of the public API server to prompt it to reload its configuration.
        Any HTTP errors encountered during the request are logged.
        Raises:
            None directly, but logs errors if the HTTP request fails.
        """

        if public_api_host := InternalConfig().config.admin_config.public_api_host:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"{public_api_host}/v1/reload_config")
                    logger.info(
                        f"{ICON} Config updated on {public_api_host}: {response.status_code}",
                        extra={"response": response.json()},
                    )
                    response.raise_for_status()
            except httpx.HTTPError as e:
                logger.error(f"{ICON} Error updating public API server: {e}")

    def _get_posting_metadata(self) -> dict | None:
        """
        Retrieves the posting metadata for the current account.

        This method fetches the `posting_json_metadata` field from the account
        on the Hive blockchain and parses it as JSON. If the metadata exists,
        it is returned as a dictionary. If no metadata is found, `None` is returned.

        Returns:
            dict | None: A dictionary containing the posting metadata if available,
            otherwise `None`.

        Raises:
            ValueError: If the `posting_json_metadata` cannot be parsed as valid JSON.
        """
        """Get the posting metadata for the current account"""
        acc = Account(self.server_accname, blockchain_instance=self.hive, lazy=True)
        try:
            # Important to use the [] method not get() to avoid lazy loading problems
            posting_json_metadata = acc["posting_json_metadata"]
        except KeyError:
            posting_json_metadata = None
        if posting_json_metadata:
            try:
                metadata = json.loads(posting_json_metadata)
                return metadata
            except ValueError as e:
                logger.error(
                    f"{ICON} Error parsing posting_json_metadata: {e}",
                    extra={**self.log_extra},
                )
                raise ValueError("Error parsing posting_json_metadata. Invalid JSON format.")
        return None


# Last line
