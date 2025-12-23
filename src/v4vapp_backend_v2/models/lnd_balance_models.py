from decimal import Decimal
from typing import Dict, Optional, Tuple

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import InternalConfig, logger
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
        """Return millisatoshi amount as int (rounded if necessary)."""
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


async def fetch_balances_from_default() -> Tuple[WalletBalance | None, ChannelBalance | None]:
    """Fetch the current Wallet and Channel balances from the default configured LND node.

    Returns a tuple of (WalletBalance, ChannelBalance) or (None, None) if no default node configured
    or if an error occurs.
    """
    try:
        lnd_config = InternalConfig().config.lnd_config
        default_node = lnd_config.default
    except Exception:  # pragma: no cover - defensive
        logger.debug("No LND config/default node configured")
        return None, None

    if not default_node:
        logger.debug("No default LND node configured")
        return None, None

    try:
        async with LNDClient(default_node) as client:
            wallet_resp = await client.call(
                client.lightning_stub.WalletBalance, lnrpc.WalletBalanceRequest()
            )
            chan_resp = await client.call(
                client.lightning_stub.ChannelBalance, lnrpc.ChannelBalanceRequest()
            )
            wallet_model = protobuf_wallet_to_pydantic(wallet_resp)
            chan_model = protobuf_channel_to_pydantic(chan_resp)
            return wallet_model, chan_model
    except Exception as e:  # pragma: no cover - log and return None
        logger.warning(
            f"Error fetching balances from default node: {e}", extra={"notification": False}
        )
        return None, None


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
