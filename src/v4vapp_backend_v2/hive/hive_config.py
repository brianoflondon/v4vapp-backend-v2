import json
import logging
from typing import List

from nectar.account import Account
from pydantic import BaseModel, Field

# from helpers.cryptoprices import CryptoConversion, CryptoPrices
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.hive.hive_extras import get_hive_client


class HiveConfigRateLimits(BaseModel):
    """Class for holding the hourly rate limits for using the Lightning exchange"""

    hours: int = Field(0, description="Number of hours for the rate limit.")
    limit: int = Field(0, description="Limit in satoshis for the rate limit.")

    def __repr__(self) -> str:
        return super().__repr__()

    def md_table(self, hive: float, HBD: float) -> str:
        return (
            f"| {self.hours:>3.0f} hours | {self.limit:>7,.0f} | "
            f"{hive:>7,.1f} Hive | {HBD:>7,.1f} HBD |\n"
        )


class HiveConfigData(BaseModel):
    """Class for fetching and storing some config settings on Hive"""

    hive_return_fee: float = Field(0.002, description="Fee for returning Hive transactions.")
    conv_fee_percent: float = Field(
        0.015, description="Conversion fee percentage for transactions."
    )
    conv_fee_sats: int = Field(50, description="Conversion fee in satoshis for transactions.")
    minimum_invoice_payment_sats: int = Field(
        500, description="Minimum invoice payment in satoshis."
    )
    maximum_invoice_payment_sats: int = Field(
        100_000, description="Maximum invoice payment in satoshis."
    )
    max_acceptable_lnd_fee_msats: int = Field(
        500_000, description="Maximum acceptable Lightning Network fee in millisatoshis."
    )
    closed_get_lnd: bool = Field(
        False, description="Flag to indicate if the LND gateway is closed."
    )
    closed_get_hive: bool = Field(
        False, description="Flag to indicate if the Hive gateway is closed."
    )
    v4v_frontend_iri: str = Field("", description="IRI for the V4V frontend.")
    v4v_api_iri: str = Field("", description="IRI for the V4V API.")
    v4v_fees_streaming_sats_to_hive_percent: float = Field(
        0.03, description="Fee percentage for streaming sats to Hive."
    )
    lightning_rate_limits: List[HiveConfigRateLimits] = Field(
        default_factory=lambda: [
            HiveConfigRateLimits(hours=4, limit=100_000 * 2),
            HiveConfigRateLimits(hours=72, limit=100_000 * 4),
            HiveConfigRateLimits(hours=168, limit=100_000 * 6),
        ],
        description="Rate limits for Lightning transactions.",
    )
    dynamic_fees_url: str = Field("", description="URL for dynamic fees.")
    dynamic_fees_permlink: str = Field("", description="Permlink for dynamic fees.")

    def __init__(cls, **kwargs):
        super().__init__(**kwargs)


class HiveConfig:
    _instance = None
    data: HiveConfigData = HiveConfigData()
    server_accname: str = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(HiveConfig, cls).__new__(cls)
        return cls._instance

    def __init__(self, server_accname: str = None, *args, **kwargs):
        if not hasattr(self, "_initialized"):
            super().__init__(*args, **kwargs)
            self._initialized = True
            self.sync_from_hive(server_accname)
            return

        if server_accname and self.server_accname != server_accname:
            logging.info(
                f"Server account name changed from {self.server_accname} to {server_accname}"
            )
            self.sync_from_hive(server_accname=server_accname)

    def sync_from_hive(self, server_accname: str) -> None:
        """Returns podping settings if they exist"""
        # Must use main chain for settings
        try:
            hive = get_hive_client()
            if not server_accname:
                self.server_accname = InternalConfig().config.hive.server_account_names[0]
            else:
                self.server_accname = server_accname
            acc = Account(
                self.server_accname,
                blockchain_instance=hive,
                lazy=True,
            )
            posting_json_metadata = acc.get("posting_json_metadata", None)
            if posting_json_metadata:
                metadata = json.loads(posting_json_metadata)
                o_h_c = metadata.get("v4vapp_hiveconfig")
                if o_h_c:
                    self.data = HiveConfigData.model_validate(o_h_c)
                    logging.info(
                        f"Fetched settings from Hive. {self.server_accname}",
                        extra={"settings": {**o_h_c}},
                    )
            else:
                logging.info(
                    f"No settings found in Hive. {self.server_accname}",
                    extra={self.server_accname: "no settings"},
                )
                self.data = HiveConfigData()
        except Exception as ex:
            logging.error(
                f"Error fetching settings from Hive: {ex}",
                extra={"hive_config": metadata},
            )


# async def put_settings_in_hive(hive_config: HiveConfig) -> dict:
#     """Put the current set of config settings into Hive"""
#     hive = Hive(keys=[Config.SERVER_ACTIVE_KEY])
#     acc = Account(Config.SERVER_ACCOUNT_NAME, blockchain_instance=hive, lazy=True)
#     original_metadata = json.loads(acc["posting_json_metadata"])
#     o_h_c = original_metadata.get("v4vapp_hiveconfig")
#     if o_h_c:
#         original_hive_config = HiveConfig(**o_h_c)
#         if hive_config == original_hive_config:
#             logging.info("Settings in Hive do not need to change")
#             Config.ALL_PARAMS = hive_config
#             return {"message": "no changes"}

#     new_meta = {**original_metadata, "v4vapp_hiveconfig": hive_config.dict()}
#     # Overwrite hive params into the Config.
#     Config.ALL_PARAMS = hive_config
#     tx = acc.update_account_jsonmetadata(new_meta)
#     logging.info(f"Settings in Hive changed: \n{json.dumps(tx, indent=2)}")
#     return tx


# async def sync_environment_with_hive(force_over: bool, new_config: HiveConfig = None) -> dict:
#     """Hive settings take precedence, if they don't exist, take the env
#     settings and write them to Hive. If force_over then always overwrite Hive
#     with .env settings"""
#     if force_over:
#         logging.info("Pushing Environment settings to Hive")
#         tx = await put_settings_in_hive(new_config)
#     else:
#         hive_config = await get_settings_from_hive()
#         if hive_config:
#             logging.info("Updating settings from Hive")
#             Config.ALL_PARAMS = hive_config
#             tx = {"message": "updated settings from Hive"}
#         else:
#             logging.info("Getting settings from Environment - and writing to Hive")
#             tx = await put_settings_in_hive(Config.ALL_PARAMS)
#             logging.info(f"Updated TX: {json.dumps(tx, indent=2, default=str)}")
#     return tx


# def pct_diff(num1: CryptoConversion, num2: CryptoConversion) -> float:
#     return (1 - (num1.HIVE / num2.HIVE)) * 100


# async def update_dynamic_fees_post(nobroadcast: bool = True, force_update: bool = False):
#     """Generate the Hive text for a dynmaic fee page"""
#     c_all = Config.ALL_PARAMS
#     fee_authorperm = c_all.dynamic_fees_url
#     hive = Hive(
#         keys=[Config.SERVER_ACTIVE_KEY],
#         nobroadcast=nobroadcast,
#         node=MAIN_NODES_FULL,
#     )

#     cp = CryptoPrices()
#     current_post = Comment(fee_authorperm, blockchain_instance=hive)
#     old_cp = json.loads(current_post.json_metadata["crypto_prices"])

#     old_conv = CryptoConversion.parse_obj(old_cp["conversion"])
#     new_conv = CryptoConversion(sats=1000)
#     new_conv = await cp.convert_any(new_conv)

#     post_age: timedelta = datetime.now(tz=timezone.utc) - current_post["updated"]
#     price_change = pct_diff(new_conv, old_conv)
#     logging.info(f"Price change since last run for 1000 Sats: {price_change:.1f}%")
#     logging.info(f"Time since last change: {post_age}")

#     if not force_update:
#         if abs(price_change) < 1.0:
#             if post_age < timedelta(seconds=(Config.HIVE_POST_FEE_UPDATE_INTERVAL_HOURS * 3600)):
#                 logging.info("No need to update post")
#                 return {
#                     "updated": False,
#                     "post_age": f"{post_age}",
#                     "post_age_s": post_age.seconds,
#                     "post_timestamp": current_post["updated"],
#                     "price_change": price_change,
#                     "old_conv": old_conv.dict(),
#                     "new_conv": new_conv.dict(),
#                 }

#     fee_post_file = open("hivefuncs/fee_page_template.md", "r")
#     # fee_post = Comment(fee_authorperm)

#     title = f"Hive to Lightning Gateway Fees | {Config.SERVER_ACCOUNT_NAME}"
#     author = Config.SERVER_ACCOUNT_NAME
#     permlink = c_all.dynamic_fees_permlink

#     if not c_all.closed_get_lnd:
#         to_lnd_gateway_status = (
#             f"# {author} [Hive to LND Gateway is OPEN]({c_all.v4v_frontend_iri})"
#         )
#     else:
#         to_lnd_gateway_status = f"# {author} Hive to LND Gateway is CLOSED"

#     if not c_all.closed_get_hive:
#         to_hive_gateway_status = (
#             f"# {author} [LND to Hive Gateway is OPEN]({c_all.v4v_frontend_iri}/hive)"
#         )
#     else:
#         to_hive_gateway_status = f"# {author} LND to Hive Gateway is CLOSED"

#     conv_fee = CryptoConversion(sats=c_all.conv_fee_sats)
#     conv_fee = await cp.convert_any(conv_fee)
#     min_inv = CryptoConversion(sats=c_all.minimum_invoice_payment_sats)
#     min_inv = await cp.convert_any(min_inv)
#     max_inv = CryptoConversion(sats=c_all.maximum_invoice_payment_sats)
#     max_inv = await cp.convert_any(max_inv)

#     search_replace = {
#         "<to_hive_gateway_status>": to_hive_gateway_status,
#         "<to_lnd_gateway_status>": to_lnd_gateway_status,
#         "<date_time_now_utc>": f"UTC: {datetime.now(tz=timezone.utc):%H:%M %d %b %Y}\n",
#         "<conv_fee_sats>": f"{conv_fee.sats:>7,.0f}",
#         "<conv_fee_hive>": f"{conv_fee.HIVE:>7,.3f}",
#         "<conv_fee_HBD>": f"{conv_fee.HBD:>7,.3f}",
#         "<conv_fee_percent>": f"{c_all.conv_fee_percent * 100:.2f}%",
#         "<min_inv_sats>": f"{min_inv.sats:>7,.0f}",
#         "<min_inv_hive>": f"{min_inv.HIVE:>7,.2f}",
#         "<min_inv_HBD>": f"{min_inv.HBD:>7,.2f}",
#         "<max_inv_sats>": f"{max_inv.sats:>7,.0f}",
#         "<max_inv_hive>": f"{max_inv.HIVE:>7,.2f}",
#         "<max_inv_HBD>": f"{max_inv.HBD:>7,.2f}",
#     }

#     logging.debug(json.dumps(search_replace, indent=2))

#     # Main text block search and replace
#     post_body = fee_post_file.read()
#     for key in search_replace:
#         post_body = post_body.replace(key, search_replace[key])

#     # Rate Limits block
#     post_body += "### Rate Limits\n\n"
#     post_body += "Each Hive user is limited to the following amounts in the following periods:\n\n"
#     post_body += "| Hours | Limit sats | Hive | HBD |\n"
#     post_body += "|-|-|-|-|\n"

#     for rate_limit in c_all.lightning_rate_limits:
#         limit = CryptoConversion(sats=rate_limit.limit)
#         limit = await cp.convert_any(limit)
#         post_body += rate_limit.md_table(limit.HIVE, limit.HBD)

#     examples = [1, 2, 5, 10, 20, 50]

#     post_body += "\n\n### HBD to Sats Examples\n\n"
#     post_body += "The following amounts of HBD will give approximately this amount in sats:\n"
#     post_body += "| HBD | Sats |\n|-|-|\n"

#     for ex in examples:
#         value = CryptoConversion(HBD=ex)
#         value = await cp.convert_any(value)
#         sats_after_fee = (value.sats - (value.sats * c_all.conv_fee_percent)) - c_all.conv_fee_sats
#         post_body += f"| {value.HBD:>4.0f} | {sats_after_fee:>7,.0f} |\n"

#     await cp.convert_any(CryptoConversion(sats=1000))
#     json_metadata = {
#         "hive_config": c_all.json(),
#         "crypto_prices": cp.json(),
#         "timestamp": datetime.timestamp(datetime.now(tz=timezone.utc)),
#     }
#     logging.debug(json_metadata)
#     logging.debug(post_body)
#     try:
#         blockchain = Blockchain()
#         logging.info(f"Current block: {blockchain.get_current_block_num()}")
#         tx = hive.post(
#             title=title,
#             body=post_body,
#             author=author,
#             permlink=permlink,
#             tags=["v4vapp", "hive", "lightning", "podcasting2"],
#             app=f"v4vapi/{__version__}",
#             json_metadata=json_metadata,
#         )
#         logging.info(tx)
#         logging.info(f"Fee post updated tx: {tx['trx_id']}")
#         return {"updated": True, "tx": tx}
#     except Exception as ex:
#         logging.exception(ex)
#         logging.error(f"{ex}")
#         return {"updated": False, "tx": "none"}


# async def run():
#     await update_dynamic_fees_post(nobroadcast=False)

#     # hive_config = Config.ALL_PARAMS
#     # hive_config2 = await get_settings_from_hive()
#     # trx = await put_settings_in_hive(hive_config)


# if __name__ == "__main__":
#     asyncio.run(run())
