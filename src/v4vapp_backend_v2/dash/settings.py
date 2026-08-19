from v4vapp_backend_v2.config.setup import (
    DashConfig,
    DashConnectionConfig,
    DashNetwork,
    DashSettlePolicy,
    InternalConfig,
)

__all__ = [
    "DashConfig",
    "DashConnectionConfig",
    "DashNetwork",
    "DashSettlePolicy",
    "dash_connection",
    "default_min_conf",
    "default_settle_policy",
]


def dash_connection(name: str | None = None) -> DashConnectionConfig | None:
    try:
        return InternalConfig().config.dash_config.connection_config(name)
    except Exception:
        return None


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
