from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

import httpx

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.dash.amounts import to_decimal


@dataclass(frozen=True)
class RateWindow:
    hours: int
    sats: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "sats", to_decimal(self.sats))


@dataclass(frozen=True)
class InvoiceFees:
    conv_fee_sats: Decimal
    routing_fee_sats: Decimal
    total_fee_sats: Decimal
    sats_collect: Decimal
    conv_fee_percent: str
    conv_fee_base_sats: Decimal


@dataclass(frozen=True)
class HiveInvoiceConfig:
    windows: tuple[RateWindow, ...]
    conv_fee_percent: Decimal
    conv_fee_sats: Decimal
    routing_fee_sats: Decimal
    minimum_invoice_payment_sats: Decimal
    maximum_invoice_payment_sats: Decimal


DEFAULT_WINDOWS: tuple[RateWindow, ...] = (
    RateWindow(hours=4, sats=Decimal(600_000)),
    RateWindow(hours=72, sats=Decimal(1_200_000)),
    RateWindow(hours=168, sats=Decimal(2_000_000)),
)

DEFAULT_CONFIG = HiveInvoiceConfig(
    windows=DEFAULT_WINDOWS,
    conv_fee_percent=Decimal("0.029"),
    conv_fee_sats=Decimal(50),
    routing_fee_sats=Decimal(300),
    minimum_invoice_payment_sats=Decimal(1),
    maximum_invoice_payment_sats=Decimal(180_000),
)

_cache: HiveInvoiceConfig | None = None
_cache_until: float = 0.0


def reset_rate_window_cache() -> None:
    global _cache, _cache_until
    _cache = None
    _cache_until = 0.0


def _routing_fee_sats() -> Decimal:
    try:
        conn = InternalConfig().config.dash_config.connection_config()
        if conn is not None:
            return to_decimal(conn.routing_fee_sats)
    except Exception:
        return DEFAULT_CONFIG.routing_fee_sats
    return DEFAULT_CONFIG.routing_fee_sats


def fetch_rate_windows(
    *, client: httpx.Client | None = None, force: bool = False
) -> list[RateWindow]:
    return list(fetch_hive_config(client=client, force=force).windows)


def fetch_hive_config(
    *, client: httpx.Client | None = None, force: bool = False
) -> HiveInvoiceConfig:
    """Live Hive invoice limits/fees. Prefers V4VConfig; HTTP client is for tests."""
    global _cache, _cache_until
    now = time.monotonic()
    if not force and _cache is not None and now < _cache_until:
        return _cache

    routing = _routing_fee_sats()

    if client is not None:
        try:
            resp = client.get("https://api.v4v.app/v1")
            resp.raise_for_status()
            parsed = _parse_config(resp.json().get("config") or {}, routing)
            _cache, _cache_until = parsed, now + 60
            return parsed
        except Exception:
            if _cache is not None:
                return _cache
            return _defaults(routing)

    try:
        from v4vapp_backend_v2.hive.v4v_config import V4VConfig

        v4v = V4VConfig()
        v4v.check()
        data = v4v.data
        windows = (
            tuple(
                RateWindow(hours=int(w.hours), sats=to_decimal(w.sats))
                for w in data.lightning_rate_limits
            )
            or DEFAULT_WINDOWS
        )
        parsed = HiveInvoiceConfig(
            windows=windows,
            conv_fee_percent=to_decimal(data.conv_fee_percent),
            conv_fee_sats=to_decimal(data.conv_fee_sats),
            routing_fee_sats=routing,
            minimum_invoice_payment_sats=to_decimal(data.minimum_invoice_payment_sats),
            maximum_invoice_payment_sats=to_decimal(data.maximum_invoice_payment_sats),
        )
        _cache, _cache_until = parsed, now + 60
        return parsed
    except Exception:
        if _cache is not None:
            return _cache
        return _defaults(routing)


def calculate_invoice_fees(sats: Decimal, cfg: HiveInvoiceConfig) -> InvoiceFees:
    """conv_fee_percent * sats + conv_fee_sats, plus a fixed routing pad."""
    sats_d = to_decimal(sats)
    conv = (cfg.conv_fee_percent * sats_d + cfg.conv_fee_sats).to_integral_value(
        rounding=ROUND_HALF_UP
    )
    routing = cfg.routing_fee_sats
    return InvoiceFees(
        conv_fee_sats=conv,
        routing_fee_sats=routing,
        total_fee_sats=conv + routing,
        sats_collect=sats_d + conv + routing,
        conv_fee_percent=str(cfg.conv_fee_percent),
        conv_fee_base_sats=cfg.conv_fee_sats,
    )


def _defaults(routing_fee_sats: Decimal) -> HiveInvoiceConfig:
    return HiveInvoiceConfig(
        windows=DEFAULT_WINDOWS,
        conv_fee_percent=DEFAULT_CONFIG.conv_fee_percent,
        conv_fee_sats=DEFAULT_CONFIG.conv_fee_sats,
        routing_fee_sats=to_decimal(routing_fee_sats),
        minimum_invoice_payment_sats=DEFAULT_CONFIG.minimum_invoice_payment_sats,
        maximum_invoice_payment_sats=DEFAULT_CONFIG.maximum_invoice_payment_sats,
    )


def _parse_config(raw: dict, routing_fee_sats: Decimal) -> HiveInvoiceConfig:
    windows = _parse_windows(raw.get("lightning_rate_limits") or [])
    if not windows:
        windows = list(DEFAULT_WINDOWS)
    percent = to_decimal(raw.get("conv_fee_percent") or DEFAULT_CONFIG.conv_fee_percent)
    return HiveInvoiceConfig(
        windows=tuple(windows),
        conv_fee_percent=percent,
        conv_fee_sats=to_decimal(raw.get("conv_fee_sats") or DEFAULT_CONFIG.conv_fee_sats),
        routing_fee_sats=to_decimal(routing_fee_sats),
        minimum_invoice_payment_sats=to_decimal(
            raw.get("minimum_invoice_payment_sats") or DEFAULT_CONFIG.minimum_invoice_payment_sats
        ),
        maximum_invoice_payment_sats=to_decimal(
            raw.get("maximum_invoice_payment_sats") or DEFAULT_CONFIG.maximum_invoice_payment_sats
        ),
    )


def _parse_windows(raw: list[object]) -> list[RateWindow]:
    windows: list[RateWindow] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        hours = int(item.get("hours") or 0)
        sats = to_decimal(item.get("sats") or 0)
        if hours > 0 and sats > 0:
            windows.append(RateWindow(hours=hours, sats=sats))
    windows.sort(key=lambda w: w.hours)
    return windows
