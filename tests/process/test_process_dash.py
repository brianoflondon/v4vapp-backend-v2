import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from bson import ObjectId

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.conversion.exchange_rebalance import RebalanceDirection
from v4vapp_backend_v2.dash.amounts import DUFFS_PER_DASH, dash_from_duffs
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.process.process_dash import (
    conv_group_id,
    fee_group_id,
    invoice_group_key,
    invoice_short_id,
    park_dash_test_payment,
    park_group_id,
    post_invoice_settlement,
    quote_from_invoice_snapshot,
)


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        Path(test_config_path, "logging/"),
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


class _InvoiceColl:
    def __init__(self) -> None:
        self.updates: list[tuple[Any, Any]] = []

    async def update_one(self, query: dict[str, Any], update: dict[str, Any]) -> None:
        self.updates.append((query, update))


class _Db:
    def __init__(self) -> None:
        self.coll = _InvoiceColl()

    def __getitem__(self, name: str) -> _InvoiceColl:
        assert name == COL_INVOICES
        return self.coll


def _settled_doc(**overrides: Any) -> dict[str, Any]:
    invoice_id = ObjectId()
    doc: dict[str, Any] = {
        "_id": invoice_id,
        "external_id": "ext-1",
        "address": "yTestDashAddress11111111111111111",
        "cust_id": "hive-customer",
        "state": DashInvoiceState.SETTLED.value,
        "network": "testnet",
        "memo": "pay ln",
        "sats_requested": Decimal(25000),
        "sats_collect": Decimal(25150),
        "sats_credited": Decimal(25000),
        "duffs_quoted": Decimal(80000000),
        "duffs_received": Decimal(80000000),
        "fees": {
            "conv_fee_sats": Decimal(100),
            "routing_fee_sats": Decimal(50),
            "total_fee_sats": Decimal(150),
            "sats_collect": Decimal(25150),
            "conv_fee_percent": "0.002",
            "conv_fee_base_sats": Decimal(50),
        },
        "quote": {
            "source": "test",
            "fetched_at": "2026-01-01T00:00:00Z",
            "btc_usd": "83000",
            "dash_usd": "32.5",
            "dash_btc": "0.00039157",
            "sats_per_dash": "39156.6265",
            "ttl_s": 60,
        },
        "settled_at": datetime(2026, 1, 1, 12, tzinfo=UTC),
    }
    doc.update(overrides)
    return doc


@pytest.fixture
def ledger_store(monkeypatch: pytest.MonkeyPatch) -> list[LedgerEntry]:
    saved: list[LedgerEntry] = []

    async def fake_save(self: LedgerEntry, ignore_duplicates: bool = False, **_kwargs: Any):
        if any(existing.group_id == self.group_id for existing in saved):
            if ignore_duplicates:
                return None
            raise RuntimeError("duplicate")
        saved.append(self)
        return object()

    async def fake_load(group_id: str) -> LedgerEntry | None:
        for entry in saved:
            if entry.group_id == group_id:
                return entry
        return None

    monkeypatch.setattr(LedgerEntry, "save", fake_save)
    monkeypatch.setattr(LedgerEntry, "load", fake_load)

    def _drop_task(coro):
        coro.close()

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.asyncio.create_task",
        _drop_task,
    )
    return saved


@pytest.mark.asyncio
async def test_open_invoice_posts_nothing(ledger_store: list[LedgerEntry]):
    doc = _settled_doc(state=DashInvoiceState.OPEN.value, sats_credited=None)
    entries = await post_invoice_settlement(doc)
    assert entries == []
    assert ledger_store == []


@pytest.mark.asyncio
async def test_settled_invoice_posts_conversion_and_fee(ledger_store: list[LedgerEntry]):
    doc = _settled_doc()
    db = _Db()
    entries = await post_invoice_settlement(doc, db=db)
    assert len(entries) == 2
    conv, fee = entries
    assert conv.ledger_type is LedgerType.CONV_DASH_TO_SATS
    assert conv.group_id == conv_group_id(invoice_group_key(doc))
    assert conv.debit_unit is Currency.DUFFS
    assert conv.credit_unit is Currency.MSATS
    assert conv.debit.name == "Treasury Dash"
    assert conv.debit.sub == "dash-testnet"
    assert conv.credit.name == "VSC Liability"
    assert conv.short_id == invoice_short_id(invoice_group_key(doc))
    assert conv.short_id == doc["address"][:8]
    assert len(conv.short_id) == 8
    assert conv.credit.sub != "hive-customer"
    assert conv.cust_id != "hive-customer"
    assert conv.debit_amount == Decimal(80000000)
    assert fee.ledger_type is LedgerType.FEE_INCOME
    assert fee.group_id == fee_group_id(invoice_group_key(doc))
    assert fee.credit.name == "Fee Income Dash"
    assert fee.debit_amount == Decimal(150) * Decimal(1000)
    assert len(db.coll.updates) == 1
    stamped = db.coll.updates[0][1]["$set"]
    assert stamped["ledger_group_id"] == conv.group_id
    assert "ledger_posted_at" in stamped


@pytest.mark.asyncio
async def test_second_call_does_not_double_post(ledger_store: list[LedgerEntry]):
    doc = _settled_doc()
    first = await post_invoice_settlement(doc)
    second = await post_invoice_settlement(doc)
    assert len(ledger_store) == 2
    assert [e.group_id for e in second] == [e.group_id for e in first]


@pytest.mark.asyncio
async def test_no_fee_when_total_fee_zero(ledger_store: list[LedgerEntry]):
    doc = _settled_doc(
        fees={"total_fee_sats": Decimal(0)},
        sats_collect=Decimal(25000),
    )
    entries = await post_invoice_settlement(doc)
    assert len(entries) == 1
    assert entries[0].ledger_type is LedgerType.CONV_DASH_TO_SATS


def test_quote_snapshot_does_not_live_fetch():
    quote = quote_from_invoice_snapshot(
        {
            "source": "locked",
            "fetched_at": "2026-01-01T00:00:00Z",
            "btc_usd": "83000",
            "dash_usd": "32.5",
            "dash_btc": "0.00039157",
            "sats_per_dash": "39156.6265",
            "ttl_s": 60,
        }
    )
    assert quote.source == "locked"
    assert quote.dash_usd == Decimal("32.5")
    assert quote.sats_per_dash_p > 0


def test_one_dash_duffs_constant():
    assert DUFFS_PER_DASH == Decimal(100_000_000)


@pytest.mark.asyncio
async def test_settled_invoice_queues_dash_btc_sell(
    ledger_store: list[LedgerEntry], monkeypatch: pytest.MonkeyPatch
):
    captured: dict[str, Any] = {}

    def fake_queue(**kwargs: Any) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash._queue_dash_sell",
        fake_queue,
    )
    doc = _settled_doc()
    await post_invoice_settlement(doc)
    assert captured["duffs_received"] == Decimal(80000000)
    assert captured["currency"] is Currency.DASH
    assert captured["server_id"]
    assert captured["invoice_id"] == invoice_group_key(doc)
    assert dash_from_duffs(captured["duffs_received"]) == Decimal("0.8")


@pytest.mark.asyncio
async def test_queue_dash_sell_calls_rebalance_with_dash_pair(monkeypatch: pytest.MonkeyPatch):
    from v4vapp_backend_v2.process.process_dash import _queue_dash_sell

    captured: dict[str, Any] = {}

    async def fake_rebalance(**kwargs: Any):
        captured.update(kwargs)

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.rebalance_queue_task",
        fake_rebalance,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.asyncio.create_task",
        asyncio.create_task,
    )
    _queue_dash_sell(
        duffs_received=DUFFS_PER_DASH,
        invoice_id="inv1",
        short_id="ext-1",
        server_id="server",
        currency=Currency.DASH,
    )
    await asyncio.sleep(0)
    assert captured["base_asset"] == "DASH"
    assert captured["quote_asset"] == "BTC"
    assert captured["direction"] is RebalanceDirection.SELL_BASE_FOR_QUOTE
    assert captured["hive_qty"] == Decimal(1)
    assert captured["tracked_op"].cust_id == "server"


@pytest.mark.asyncio
async def test_pay_skips_when_no_bolt11(monkeypatch: pytest.MonkeyPatch):
    from v4vapp_backend_v2.process.process_dash import pay_dash_lightning

    called = False

    async def boom(**_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr("v4vapp_backend_v2.process.process_dash.decode_any_lightning_string", boom)
    await pay_dash_lightning(_settled_doc())
    assert called is False


OUR_PUBKEY = "02" + "ab" * 32
OTHER_PUBKEY = "03" + "cd" * 32


def _enable_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    from v4vapp_backend_v2.config.setup import InternalConfig

    ic = InternalConfig()
    monkeypatch.setattr(ic.config.development, "enabled", True)


@pytest.mark.asyncio
async def test_pay_probes_when_payouts_disabled(monkeypatch: pytest.MonkeyPatch):
    from v4vapp_backend_v2.lnd_grpc.lnd_functions import LNDPaymentError
    from v4vapp_backend_v2.process.process_dash import (
        lightning_probe_only,
        pay_dash_lightning,
    )

    _enable_dev(monkeypatch)
    assert lightning_probe_only(OTHER_PUBKEY) is True
    captured: dict[str, Any] = {}

    class _PayReq:
        value_msat = 25_000_000
        destination = OTHER_PUBKEY

    async def fake_decode(**_kwargs):
        return _PayReq()

    async def fake_send(**kwargs):
        captured.update(kwargs)
        raise LNDPaymentError("lnbc FAILED: FAILURE_REASON_INCORRECT_PAYMENT_DETAILS")

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.decode_any_lightning_string",
        fake_decode,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.send_lightning_to_pay_req",
        fake_send,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.LNDClient",
        lambda **_k: object(),
    )
    doc = _settled_doc(lightning_invoice="lnbc250u1ptestinvoice")
    probed = await pay_dash_lightning(doc)
    assert probed is True
    assert captured["probe_only"] is True
    assert captured["group_id"] == invoice_group_key(doc)
    assert captured["amount_msat"] == Decimal(25_000_000)
    assert captured["cust_id"] == doc["cust_id"]


@pytest.mark.asyncio
async def test_pay_sends_when_payouts_enabled(monkeypatch: pytest.MonkeyPatch):
    from v4vapp_backend_v2.process.process_dash import pay_dash_lightning

    captured: dict[str, Any] = {}

    class _PayReq:
        value_msat = 25_000_000
        destination = OTHER_PUBKEY

    class _Payment:
        status = "SUCCEEDED"

    async def fake_decode(**_kwargs):
        return _PayReq()

    async def fake_send(**kwargs):
        captured.update(kwargs)
        return _Payment()

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.decode_any_lightning_string",
        fake_decode,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.send_lightning_to_pay_req",
        fake_send,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.LNDClient",
        lambda **_k: object(),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.lightning_probe_only",
        lambda destination="": False,
    )
    doc = _settled_doc(lightning_invoice="lnbc250u1ptestinvoice")
    probed = await pay_dash_lightning(doc)
    assert probed is False
    assert captured["probe_only"] is False
    assert captured["cust_id"] == doc["cust_id"]


@pytest.mark.asyncio
async def test_park_moves_net_sats_to_dash_payment_tests(
    ledger_store: list[LedgerEntry],
):
    from v4vapp_backend_v2.config.setup import InternalConfig

    doc = _settled_doc()
    entry = await park_dash_test_payment(doc)
    assert entry is not None
    assert entry.ledger_type is LedgerType.DASH_TEST_PAY
    assert entry.group_id == park_group_id(invoice_group_key(doc))
    assert entry.short_id == invoice_short_id(invoice_group_key(doc))
    assert entry.debit.name == "VSC Liability"
    assert entry.debit.sub == InternalConfig().server_id
    assert entry.credit.name == "Dash Payment Tests"
    assert entry.credit.sub == InternalConfig().server_id
    assert entry.debit_amount == Decimal(25000) * Decimal(1000)
    again = await park_dash_test_payment(doc)
    assert again.group_id == entry.group_id
    assert len(ledger_store) == 1


@pytest.mark.asyncio
async def test_pay_skips_after_probe_stamp():
    from v4vapp_backend_v2.process.process_dash import pay_dash_lightning

    doc = _settled_doc(
        lightning_invoice="lnbc250u1ptestinvoice",
        lightning_probed_at=datetime.now(tz=UTC),
    )
    probed = await pay_dash_lightning(doc)
    assert probed is True


def test_lightning_probe_only_production_never_probes():
    from v4vapp_backend_v2.process.process_dash import lightning_probe_only

    assert lightning_probe_only(OUR_PUBKEY) is False
    assert lightning_probe_only(OTHER_PUBKEY) is False
    assert lightning_probe_only() is False


def test_lightning_probe_only_dev_self_pay_and_external(
    monkeypatch: pytest.MonkeyPatch,
):
    from types import SimpleNamespace

    from v4vapp_backend_v2.config.setup import InternalConfig
    from v4vapp_backend_v2.process.process_dash import lightning_probe_only

    _enable_dev(monkeypatch)
    ic = InternalConfig()
    monkeypatch.setattr(type(ic), "node_pubkey", property(lambda self: OUR_PUBKEY))
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_dash.dash_connection",
        lambda: SimpleNamespace(lightning_payments_enabled=True),
    )
    assert lightning_probe_only(OUR_PUBKEY) is False
    assert lightning_probe_only(OTHER_PUBKEY) is True


def test_lightning_probe_only_dev_payments_off(monkeypatch: pytest.MonkeyPatch):
    from v4vapp_backend_v2.process.process_dash import lightning_probe_only

    _enable_dev(monkeypatch)
    assert lightning_probe_only(OTHER_PUBKEY) is True
    assert lightning_probe_only(OUR_PUBKEY) is True
