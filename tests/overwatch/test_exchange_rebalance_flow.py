"""Exchange and treasury rebalances must not open customer payment flows.

A Hive transfer from an exchange hot wallet (exchange_config hive_accounts)
or another exchange/treasury account is an internal move. Overwatch used to
open hive_to_keepsats, hive_to_keepsats_external, hive_transfer_paywithsats
and hive_transfer_failure for it, then stall. Customer deposits still open
and still stall when their stages are missing.

Account names below live only in the test double passed to Overwatch as
config. Production code reads exchange_config and hive_config.
"""

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.process.overwatch_flows import (
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    HIVE_TRANSFER_FAILURE_FLOW,
    HIVE_TRANSFER_PAYWITHSATS_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import (
    FlowEvent,
    FlowInstance,
    FlowStatus,
    Overwatch,
)

# Names exist only in this test's config double. None are hardcoded in Overwatch.
_SERVER = "someaccount"
_TREASURY = "v4vapp.tre"
_EXCHANGE_ROLE = "fiction"
_EXCHANGE_HOT = ["bdhivesteem", "binance-hot2"]

_TS = datetime(2026, 11, 22, 6, 2, 16, tzinfo=timezone.utc)
_REBALANCE_SHORT_ID = "5080_4d82e2_1"
_REBALANCE_GROUP_ID = "111_4d82e215e68c7360caea1c746aec83de03bbe5d3_1_virtual"
_AMOUNT = "9999.990 HIVE"
_CUSTOMER_FLOW_NAMES = {
    "hive_to_keepsats",
    "hive_to_keepsats_external",
    "hive_transfer_paywithsats",
    "hive_transfer_failure",
}


def _register() -> Overwatch:
    Overwatch.reset()
    ow = Overwatch()
    Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
    Overwatch.register_flow(HIVE_TRANSFER_PAYWITHSATS_FLOW)
    Overwatch.register_flow(HIVE_TRANSFER_FAILURE_FLOW)
    Overwatch._loaded_from_redis = True
    return ow


def _transfer(
    *,
    from_account: str,
    to_account: str,
    short_id: str,
    group_id: str,
    amount: str = _AMOUNT,
    timestamp: datetime | None = None,
) -> SimpleNamespace:
    when = timestamp or _TS
    return SimpleNamespace(
        group_id=group_id,
        short_id=short_id,
        op_type="transfer",
        from_account=from_account,
        to_account=to_account,
        amount_str=amount,
        conv=None,
        timestamp=when,
        memo="deposit",
        log_str=f"{from_account} sent {amount} to {to_account}",
        log_extra={},
        json_data=None,
        cj_id="",
    )


def _rebalance_ledger(
    ledger_type: LedgerType = LedgerType.EXCHANGE_TO_TREASURY,
    *,
    group_id: str = _REBALANCE_GROUP_ID,
    short_id: str = _REBALANCE_SHORT_ID,
    cust_id: str = "binance_convert",
) -> LedgerEntry:
    return LedgerEntry(
        group_id=group_id,
        short_id=short_id,
        ledger_type=ledger_type,
        timestamp=_TS,
        cust_id=cust_id,
        description="Exchange to Treasury transfer",
        debit_amount=Decimal("9999.990"),
        credit_amount=Decimal("9999.990"),
        debit_unit=Currency.HIVE,
        credit_unit=Currency.HIVE,
    )


def _classified(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if "Classified exchange rebalance" in record.getMessage()
    ]


class _ExchangeConfig:
    def __init__(self, accounts: list[str]) -> None:
        self._accounts = accounts

    def all_hive_exchange_accounts(self) -> list[str]:
        return list(self._accounts)


class _HiveAccount:
    alternate_names: list[str] = []


class _HiveConfig:
    exchange_account_names = [_EXCHANGE_ROLE]
    treasury_account_names = [_TREASURY]
    server_account_names = [_SERVER]
    extended_all_account_names = [_SERVER, _TREASURY, "v4vapp.dhf", _EXCHANGE_ROLE]
    exchange_account = _HiveAccount()


class _AppConfig:
    def __init__(self, accounts: list[str]) -> None:
        self.exchange_config = _ExchangeConfig(accounts)
        self.hive_config = _HiveConfig()


class _InternalConfig:
    """Stand-in for InternalConfig. No Redis. Tests set the account lists."""

    def __init__(self, accounts: list[str]) -> None:
        self.config = _AppConfig(accounts)


_exchange_accounts: list[str] = list(_EXCHANGE_HOT)


@pytest.fixture(autouse=True)
def _overwatch_config(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Point Overwatch at the test account lists and skip ledger model dumps."""
    global _exchange_accounts
    _exchange_accounts = list(_EXCHANGE_HOT)
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_overwatch.InternalConfig",
        lambda: _InternalConfig(_exchange_accounts),
    )
    monkeypatch.setattr(LedgerEntry, "log_extra", property(lambda self: {}))
    return _exchange_accounts


@pytest.mark.asyncio
async def test_exchange_to_treasury_does_not_stall(
    _overwatch_config: list[str],
    caplog: pytest.LogCaptureFixture,
):
    """binance-hivehw → treasury with exc_to_t opens no customer flow and never stalls."""
    caplog.set_level(logging.INFO, logger="backend")
    _overwatch_config.append("binance-hivehw")
    ow = _register()

    await ow.ingest_op(
        _transfer(
            from_account="binance-hivehw",
            to_account="v4vapp.tre",
            short_id=_REBALANCE_SHORT_ID,
            group_id=_REBALANCE_GROUP_ID,
        )
    )
    assert ow.active_flows == []
    assert {flow.flow_definition.name for flow in ow.flow_instances}.isdisjoint(
        _CUSTOMER_FLOW_NAMES
    )

    await ow.ingest_ledger_entry(_rebalance_ledger())
    assert ow.active_flows == []
    assert ow.stalled_flows == []

    classified = _classified(caplog)
    assert len(classified) == 1
    message = classified[0].getMessage()
    assert classified[0].levelno == logging.INFO
    assert _REBALANCE_SHORT_ID in message
    assert "binance-hivehw" in message
    assert _AMOUNT in message

    caplog.clear()
    stalled = await ow.check_stalls(_TS + timedelta(hours=2))
    ow._log_report()
    assert stalled == []
    assert ow.stalled_flows == []
    assert not any("stalled" in record.getMessage().lower() for record in caplog.records)


@pytest.mark.asyncio
async def test_configured_exchange_hive_account_skips_customer_flows(
    _overwatch_config: list[str],
    caplog: pytest.LogCaptureFixture,
):
    """Sender membership comes from exchange_config, not a hardcoded name."""
    caplog.set_level(logging.INFO, logger="backend")
    assert "binance-hot2" in _overwatch_config
    ow = _register()

    await ow.ingest_op(
        _transfer(
            from_account="binance-hot2",
            to_account="v4vapp.tre",
            short_id="hot_1",
            group_id="gid_hot",
            amount="10.000 HIVE",
        )
    )
    assert ow.active_flows == []
    classified = _classified(caplog)
    assert len(classified) == 1
    assert "binance-hot2" in classified[0].getMessage()
    assert "10.000 HIVE" in classified[0].getMessage()
    assert "hot_1" in classified[0].getMessage()


@pytest.mark.asyncio
async def test_treasury_sender_is_classified_from_hive_config(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.INFO, logger="backend")
    treasury = _TREASURY
    ow = _register()

    await ow.ingest_op(
        _transfer(
            from_account=treasury,
            to_account="bdhivesteem",
            short_id="tre_1",
            group_id="gid_tre",
            amount="1.000 HIVE",
        )
    )
    assert ow.active_flows == []
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Classified exchange rebalance" in message and treasury in message
        for message in messages
    )
    assert not any(
        "Skipping flow creation for outbound transfer" in message for message in messages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ledger_type",
    [LedgerType.EXCHANGE_TO_TREASURY, LedgerType.TREASURY_TO_EXCHANGE],
)
async def test_rebalance_ledger_drops_candidates_opened_earlier(
    ledger_type: LedgerType,
    caplog: pytest.LogCaptureFixture,
):
    """Candidates opened before the ledger arrives are cancelled, not stalled.

    The sender is not in config here, so the transfer looks like a customer
    deposit until the exc_to_t / t_to_exc entry shows up. A different customer's
    flow for another group_id stays open.
    """
    caplog.set_level(logging.INFO, logger="backend")
    ow = _register()
    await ow.ingest_op(
        _transfer(
            from_account="binance-hivehw",
            to_account=_TREASURY,
            short_id=_REBALANCE_SHORT_ID,
            group_id=_REBALANCE_GROUP_ID,
        )
    )
    opened = {flow.flow_definition.name for flow in ow.active_flows}
    assert _CUSTOMER_FLOW_NAMES <= opened

    # A different customer's flow is already tracked. Appended directly so the
    # rebalance transfer is not swallowed as that flow's change transfer.
    customer = FlowInstance(
        flow_definition=HIVE_TO_KEEPSATS_FLOW,
        trigger_group_id="gid_alice",
        trigger_short_id="cust_alice_1",
        cust_id="alice",
        status=FlowStatus.IN_PROGRESS,
        started_at=_TS,
    )
    customer.add_event(
        FlowEvent(
            event_type="op",
            timestamp=_TS,
            group_id="gid_alice",
            short_id="cust_alice_1",
            op_type="transfer",
        )
    )
    ow.flow_instances.append(customer)

    await ow.ingest_ledger_entry(_rebalance_ledger(ledger_type, cust_id="binance_convert"))
    assert not any(flow.trigger_short_id == _REBALANCE_SHORT_ID for flow in ow.flow_instances)
    assert any(flow.trigger_short_id == "cust_alice_1" for flow in ow.active_flows)

    classified = _classified(caplog)
    assert len(classified) == 1
    assert _REBALANCE_SHORT_ID in classified[0].getMessage()
    assert "binance-hivehw" in classified[0].getMessage()
    assert "9999.990" in classified[0].getMessage()

    caplog.clear()
    stalled = await ow.check_stalls(_TS + timedelta(seconds=10))
    ow._log_report()
    assert not any(flow.trigger_short_id == _REBALANCE_SHORT_ID for flow in stalled)
    assert not any(
        _REBALANCE_SHORT_ID in record.getMessage() and "stall" in record.getMessage().lower()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_known_rebalance_candidate_is_not_reminded(
    _overwatch_config: list[str],
    caplog: pytest.LogCaptureFixture,
):
    """A candidate already sitting in memory is dropped instead of stall-reminded."""
    caplog.set_level(logging.INFO, logger="backend")
    _overwatch_config.append("binance-hivehw")
    ow = _register()
    flow = FlowInstance(
        flow_definition=HIVE_TO_KEEPSATS_FLOW,
        trigger_group_id=_REBALANCE_GROUP_ID,
        trigger_short_id=_REBALANCE_SHORT_ID,
        cust_id="binance-hivehw",
        status=FlowStatus.IN_PROGRESS,
        started_at=_TS,
    )
    flow.add_event(
        FlowEvent(
            event_type="op",
            timestamp=_TS,
            group_id=_REBALANCE_GROUP_ID,
            short_id=_REBALANCE_SHORT_ID,
            op_type="transfer",
            event_value=_AMOUNT,
        )
    )
    flow.add_event(
        FlowEvent(
            event_type="op",
            timestamp=_TS + timedelta(seconds=30),
            group_id="gid_other_transfer",
            short_id="other_transfer",
            op_type="transfer",
        )
    )
    assert flow.matched_stage_names == {"trigger_transfer", "change_transfer_op"}
    ow.flow_instances.append(flow)

    caplog.clear()
    stalled = await ow.check_stalls(_TS + timedelta(hours=2))
    ow._log_report()
    assert stalled == []
    assert ow.stalled_flows == []
    assert not any(item.trigger_short_id == _REBALANCE_SHORT_ID for item in ow.flow_instances)
    assert not any("stalled" in record.getMessage().lower() for record in caplog.records)
    classified = _classified(caplog)
    assert len(classified) == 1
    assert _REBALANCE_SHORT_ID in classified[0].getMessage()
    assert "binance-hivehw" in classified[0].getMessage()
    assert _AMOUNT in classified[0].getMessage()


@pytest.mark.asyncio
async def test_customer_hive_to_keepsats_still_opens_and_stalls(
    caplog: pytest.LogCaptureFixture,
):
    """A customer deposit still tracks hive_to_keepsats and stalls when stages are missing."""
    caplog.set_level(logging.INFO, logger="backend")
    ow = _register()
    await ow.ingest_op(
        _transfer(
            from_account="alice",
            to_account="someaccount",
            short_id="cust_5081",
            group_id="gid_customer",
            amount="10.000 HIVE",
        )
    )
    opened = {flow.flow_definition.name for flow in ow.active_flows}
    assert "hive_to_keepsats" in opened

    deposit = LedgerEntry(
        group_id="gid_customer",
        short_id="cust_5081",
        ledger_type=LedgerType.CUSTOMER_HIVE_IN,
        timestamp=_TS,
        cust_id="alice",
        debit_amount=Decimal("10.000"),
        credit_amount=Decimal("10.000"),
        debit_unit=Currency.HIVE,
        credit_unit=Currency.HIVE,
    )
    await ow.ingest_ledger_entry(deposit)

    flow = next(
        item
        for item in ow.active_flows
        if item.flow_definition.name == "hive_to_keepsats"
        and item.trigger_short_id == "cust_5081"
    )
    assert flow.status == FlowStatus.IN_PROGRESS
    assert "trigger_transfer" in flow.matched_stage_names
    assert "customer_hive_in" in flow.matched_stage_names
    assert any(stage.name == "conv_hive_to_keepsats" for stage in flow.missing_stages)
    assert not _classified(caplog)

    stalled = await ow.check_stalls(_TS + timedelta(minutes=6))
    assert any(
        item.flow_definition.name == "hive_to_keepsats" and item.trigger_short_id == "cust_5081"
        for item in stalled
    )
    assert flow.status == FlowStatus.STALLED


@pytest.mark.asyncio
async def test_server_outbound_is_not_classified_as_rebalance(
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(logging.INFO, logger="backend")
    ow = _register()
    await ow.ingest_op(
        _transfer(
            from_account=_SERVER,
            to_account="alice",
            short_id="srv_1",
            group_id="gid_srv",
            amount="1.000 HIVE",
        )
    )
    assert ow.active_flows == []
    messages = [record.getMessage() for record in caplog.records]
    assert any("Skipping flow creation for outbound transfer" in message for message in messages)
    assert not any("Classified exchange rebalance" in message for message in messages)
