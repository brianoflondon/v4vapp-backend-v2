from datetime import UTC, datetime, timedelta

import httpx
import pytest

from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.limits.check import check_cust_rate_limit
from v4vapp_backend_v2.dash.limits.hive_config import (
    RateWindow,
    calculate_invoice_fees,
    fetch_hive_config,
    fetch_rate_windows,
    reset_rate_window_cache,
)
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState


class _Cursor:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def __aiter__(self) -> "_Cursor":
        self._iter = iter(self._rows)
        return self

    async def __anext__(self) -> dict:
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _Coll:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def find(self, query: dict) -> _Cursor:
        matched = []
        for doc in self.rows:
            if doc.get("cust_id") != query.get("cust_id"):
                continue
            if doc.get("state") not in query.get("state", {}).get("$in", []):
                continue
            if doc.get("settled_at") < query.get("settled_at", {}).get("$gte"):
                continue
            matched.append(doc)
        return _Cursor(matched)


class _Db:
    def __init__(self, rows: list[dict]) -> None:
        self.col = _Coll(rows)

    def __getitem__(self, name: str) -> _Coll:
        assert name == COL_INVOICES
        return self.col


def test_fetch_rate_windows_from_v1_payload() -> None:
    reset_rate_window_cache()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "config": {
                    "lightning_rate_limits": [
                        {"hours": 168, "sats": 2000000.0},
                        {"hours": 4, "sats": 600000.0},
                        {"hours": 72, "sats": 1200000.0},
                    ]
                }
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    windows = fetch_rate_windows(client=client, force=True)
    assert [(w.hours, w.sats) for w in windows] == [
        (4, 600_000),
        (72, 1_200_000),
        (168, 2_000_000),
    ]


def test_fetch_hive_config_fees_and_bounds() -> None:
    reset_rate_window_cache()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "config": {
                    "conv_fee_percent": 0.029,
                    "conv_fee_sats": 50,
                    "minimum_invoice_payment_sats": 1,
                    "maximum_invoice_payment_sats": 180000,
                    "lightning_rate_limits": [{"hours": 4, "sats": 600000}],
                }
            },
        )

    cfg = fetch_hive_config(
        client=httpx.Client(transport=httpx.MockTransport(handler)), force=True
    )
    assert cfg.minimum_invoice_payment_sats == 1
    assert cfg.maximum_invoice_payment_sats == 180_000
    fees = calculate_invoice_fees(25_000, cfg)
    assert fees.conv_fee_sats == 775
    assert fees.routing_fee_sats == 300
    assert fees.total_fee_sats == 1_075
    assert fees.sats_collect == 26_075


@pytest.mark.asyncio
async def test_check_includes_requested_sats_and_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "v4vapp_backend_v2.dash.limits.check.fetch_rate_windows",
        lambda: [RateWindow(4, 600_000)],
    )
    now = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
    paid = [
        {
            "cust_id": "app-a",
            "state": DashInvoiceState.SETTLED.value,
            "sats_credited": 400_000,
            "settled_at": now - timedelta(hours=1),
        }
    ]
    with pytest.raises(ApiError) as exc:
        await check_cust_rate_limit(_Db(paid), cust_id="app-a", extra_sats=250_000, now=now)
    assert exc.value.status_code == 422
    assert exc.value.code == "rate_limit_exceeded"
    check = exc.value.extra["limit_check"]
    assert check["periods"]["4"]["sats"] == 650_000
    assert check["sats_freed"] == 400_000
    assert check["expiry"].startswith("2026-08-13T15:00:00")
