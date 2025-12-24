import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, ClassVar, Dict, Optional

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field
from pymongo.asynchronous.collection import AsyncCollection

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.decorators import async_time_decorator
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.database.db_retry import mongo_call
from v4vapp_backend_v2.database.db_tools import find_nearest_by_timestamp_server_side
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient


class LNDAmount(BaseModel):
    """Representation of LND amount object (sat / msat) using Decimal for precision."""

    sat: Decimal = Decimal(0)
    msat: Decimal = Decimal(0)

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    @property
    def sat_int(self) -> int:
        """Return satoshi amount as int (rounded if necessary)."""
        return int(self.sat)

    @property
    def msat_int(self) -> int:
        """Return millisatoshis amount as int (rounded if necessary)."""
        return int(self.msat)


class WalletAccountBalance(BaseModel):
    confirmed_balance: Decimal = Decimal(0)
    unconfirmed_balance: Decimal = Decimal(0)

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class WalletBalance(BaseModel):
    total_balance: Decimal = Decimal(0)
    confirmed_balance: Decimal = Decimal(0)
    unconfirmed_balance: Decimal = Decimal(0)
    locked_balance: Decimal = Decimal(0)
    reserved_balance_anchor_chan: Decimal = Decimal(0)

    account_balance: Dict[str, WalletAccountBalance] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    @property
    def total_sats(self) -> Decimal:
        return self.total_balance

    @property
    def confirmed_sats(self) -> Decimal:
        return self.confirmed_balance

    @property
    def unconfirmed_sats(self) -> Decimal:
        return self.unconfirmed_balance


class ChannelBalance(BaseModel):
    # deprecated: balance / pending_open_balance are kept for compatibility
    balance: Decimal = Decimal(0)
    pending_open_balance: Decimal = Decimal(0)

    local_balance: Optional[LNDAmount] = None
    remote_balance: Optional[LNDAmount] = None
    unsettled_local_balance: Optional[LNDAmount] = None
    unsettled_remote_balance: Optional[LNDAmount] = None
    pending_open_local_balance: Optional[LNDAmount] = None
    pending_open_remote_balance: Optional[LNDAmount] = None

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    @property
    def local_sats(self) -> Decimal:
        return self.local_balance.sat if self.local_balance else Decimal(0)

    @property
    def remote_sats(self) -> Decimal:
        return self.remote_balance.sat if self.remote_balance else Decimal(0)

    @property
    def local_msat(self) -> Decimal:
        return self.local_balance.msat if self.local_balance else Decimal(0)

    @property
    def remote_msat(self) -> Decimal:
        return self.remote_balance.msat if self.remote_balance else Decimal(0)


class NodeBalances(BaseModel):
    node: str = ""
    timestamp: datetime = datetime.now(tz=timezone.utc)
    wallet: Optional[WalletBalance] = None
    channel: Optional[ChannelBalance] = None

    db_client: ClassVar[AsyncCollection | None] = None
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    def __init__(self, **data):
        super().__init__(**data)
        if not self.node:
            lnd_config = InternalConfig().config.lnd_config
            if lnd_config and lnd_config.default:
                self.node = lnd_config.default

    @property
    def log_str(self) -> str:
        if not self.channel:
            return f"{self.node} at No channel balance {self.timestamp.isoformat()}"
        return f"{self.node} Channels local: {self.channel.local_sats:,.0f} sats, remote: {self.channel.remote_sats:,.0f} sats {self.timestamp.isoformat()}"

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {"node_balances": self.model_dump()}

    @classmethod
    def collection(cls) -> AsyncCollection:
        if cls.db_client is None:
            cls.db_client = InternalConfig.db["lnd_balances_ts"]
        return cls.db_client

    async def save(self) -> None:
        """Save the balances to the database."""
        mongo_data = convert_decimals_for_mongodb(self.model_dump())
        await mongo_call(lambda: self.collection().insert_one(mongo_data))

    async def fetch_balances(self, lnd_client: LNDClient | None = None) -> None:
        """Fetch and update the balances for this node."""
        balances = await fetch_balances(node=self.node, lnd_client=lnd_client)
        self.wallet = balances.wallet
        self.channel = balances.channel

    async def nearest_balance(self, target: datetime) -> Optional["NodeBalances"]:
        """Fetch the nearest balance document by timestamp for this node."""
        filter = {"node": self.node}
        coll = self.collection()

        doc = await find_nearest_by_timestamp_server_side(
            collection=coll, target=target, ts_field="timestamp", filter_extra=filter
        )
        if doc:
            return NodeBalances.model_validate(doc)
        return None


@async_time_decorator
async def fetch_balances(node: str = "", lnd_client: LNDClient | None = None) -> NodeBalances:
    """Fetch the current Wallet and Channel balances from the default configured LND node.

    Returns a tuple of (WalletBalance, ChannelBalance) or (None, None) if no default node configured
    or if an error occurs.
    """
    lnd_config = InternalConfig().config.lnd_config
    if not lnd_config:
        raise ValueError("LND config not available")

    if not node:
        node = lnd_config.default

    if not node:
        logger.debug("No default LND node configured")
        raise ValueError("No default LND node configured")

    async def _fetch_with_client(client: LNDClient) -> NodeBalances:
        # Run wallet and channel calls concurrently and build NodeBalances
        wallet_task = asyncio.create_task(
            client.call(client.lightning_stub.WalletBalance, lnrpc.WalletBalanceRequest())
        )
        chan_task = asyncio.create_task(
            client.call(client.lightning_stub.ChannelBalance, lnrpc.ChannelBalanceRequest())
        )
        wallet_resp, chan_resp = await asyncio.gather(wallet_task, chan_task)
        wallet_model = protobuf_wallet_to_pydantic(wallet_resp)
        chan_model = protobuf_channel_to_pydantic(chan_resp)
        return NodeBalances(node=node, wallet=wallet_model, channel=chan_model)

    try:
        if lnd_client is None:
            lnd_client = LNDClient(node)
            # we created the client, so use context manager to ensure it is closed
            async with lnd_client as client:
                return await _fetch_with_client(client)
        else:
            # external client: reuse it but do not close it
            return await _fetch_with_client(lnd_client)
    except Exception as e:  # pragma: no cover - log and return None
        logger.warning(
            f"Error fetching balances from default node: {e}", extra={"notification": False}
        )
        raise ValueError(f"Error fetching balances from default node: {e}")


def protobuf_wallet_to_pydantic(wallet_resp: lnrpc.WalletBalanceResponse) -> WalletBalance:
    """Convert a protobuf WalletBalanceResponse to a `WalletBalance` model."""
    if wallet_resp is None:
        return WalletBalance()

    wd = MessageToDict(wallet_resp, preserving_proto_field_name=True)
    # MessageToDict will convert account_balance to mapping of dicts
    try:
        return WalletBalance.model_validate(wd)
    except Exception as e:  # pragma: no cover - defensive logging
        logger.error(f"Error validating WalletBalance: {e}", extra={"wallet_dict": wd})
        raise


def protobuf_channel_to_pydantic(chan_resp: lnrpc.ChannelBalanceResponse) -> ChannelBalance:
    """Convert a protobuf ChannelBalanceResponse to a `ChannelBalance` model."""
    if chan_resp is None:
        return ChannelBalance()

    cd = MessageToDict(chan_resp, preserving_proto_field_name=True)
    # MessageToDict will create nested dicts for Amount objects (sat/msat)
    try:
        return ChannelBalance.model_validate(cd)
    except Exception:  # pragma: no cover - defensive logging
        logger.error("Error validating ChannelBalance", extra={"chan_dict": cd})
        raise


async def main():
    logger.info("Starting LND opening balance task...")
    InternalConfig(config_filename="devhive.config.yaml")
    db_conn = DBConn()
    await db_conn.setup_database()
    balances = NodeBalances()
    await balances.fetch_balances()
    await balances.save()

    if not balances.wallet and not balances.channel:
        logger.warning("No balances available (no default LND node configured or error).")
        return

    if balances.wallet:
        logger.info(
            f"Wallet total: {balances.wallet.total_sats} sats, confirmed: {balances.wallet.confirmed_sats} sats, unconfirmed: {balances.wallet.unconfirmed_sats} sats"
        )
    if balances.channel:
        logger.info(
            f"Channels local: {balances.channel.local_sats} sats, remote: {balances.channel.remote_sats} sats, balance: {balances.channel.balance} sats"
        )

    found_balance = await balances.nearest_balance(
        datetime.now(timezone.utc) - timedelta(minutes=10)
    )
    if found_balance:
        logger.info(
            f"Found nearest balance at {found_balance.timestamp} for node {found_balance.node}"
        )


if __name__ == "__main__":
    asyncio.run(main())
