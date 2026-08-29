from v4vapp_backend_v2.config.setup import (
    DashConfig,
    DashConnectionConfig,
    DashNetwork,
    DashSettlePolicy,
    InternalConfig,
)

DEFAULT_DASH_WALLET_SENDER = "v4v.app"

__all__ = [
    "DEFAULT_DASH_WALLET_SENDER",
    "DashConfig",
    "DashConnectionConfig",
    "DashNetwork",
    "DashSettlePolicy",
    "dash_connection",
    "default_min_conf",
    "default_settle_policy",
    "wallet_sender",
]


def dash_connection(name: str | None = None) -> DashConnectionConfig | None:
    try:
        return InternalConfig().config.dash_config.connection_config(name)
    except Exception:
        return None


def wallet_sender() -> str:
    """Display name stuffed into DashPay `uri_dashpay` as `sender=`."""
    try:
        configured = InternalConfig().config.dash_config.dash_wallet_sender
    except Exception:
        return DEFAULT_DASH_WALLET_SENDER
    text = (configured or "").strip()
    return text or DEFAULT_DASH_WALLET_SENDER


def default_min_conf(network: DashNetwork) -> int:
    if network == "regtest":
        return 1
    if network == "testnet":
        return 2
    return 6


def default_settle_policy(network: DashNetwork) -> DashSettlePolicy:
    if network == "regtest":
        return "conf_n"
    return "instantsend_or_chainlock"
