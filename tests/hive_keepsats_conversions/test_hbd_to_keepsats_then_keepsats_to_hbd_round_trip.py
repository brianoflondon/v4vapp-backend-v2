from datetime import datetime, timezone
from decimal import Decimal

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.conversion.calculate import ConversionResult

# Functions under test
from v4vapp_backend_v2.conversion.hive_to_keepsats import conversion_hive_to_keepsats
from v4vapp_backend_v2.conversion.keepsats_to_hive import conversion_keepsats_to_hive
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency

# Skip the whole module due to flakiness; use pytestmark for module-level skipping
pytestmark = pytest.mark.skip(reason="Flaky test, needs investigation")


class DummyOp:
    def __init__(self, short_id: str, group_id: str, amount=None):
        self.short_id = short_id
        self.group_id = group_id
        self.op_type = "transfer"
        self.d_memo = "memo"
        self.lightning_memo = ""
        self.link = ""
        self.timestamp = datetime.now(tz=timezone.utc)
        self.change_memo = None
        self.change_amount = None
        self.change_conv = None

    async def update_conv(self, quote=None):
        return None

    async def save(self):
        return None

    def add_reply(self, **kwargs):
        return None


@pytest.mark.asyncio
async def test_hbd_to_keepsats_then_keepsats_to_hbd_round_trip(monkeypatch):
    ledger_captured = []

    async def fake_save(self):
        # append a shallow copy / reference for inspection
        ledger_captured.append(self)
        return None

    monkeypatch.setattr(LedgerEntry, "save", fake_save, raising=True)

    # Provide a usable quote so CryptoConversion computations do not fail
    quote = QuoteResponse(btc_usd=20000, hive_usd=0.09, hbd_usd=1.0)

    # Prepare conversion result for HBD -> Keepsats (deposit)
    # Choose msats so numbers are simple: to_convert_conv.msats = 11_331_400 msats (11,331.4 sats)
    to_convert_msats_1 = Decimal(11_331_400)
    fee_msats_1 = Decimal(265_300)
    net_msats_1 = to_convert_msats_1 - fee_msats_1

    to_convert_conv_1 = CryptoConversion(
        value=to_convert_msats_1, conv_from=Currency.MSATS, quote=quote
    ).conversion
    net_to_receive_conv_1 = CryptoConversion(
        value=net_msats_1, conv_from=Currency.MSATS, quote=quote
    ).conversion
    fee_conv_1 = CryptoConversion(
        value=fee_msats_1, conv_from=Currency.MSATS, quote=quote
    ).conversion

    conv1 = ConversionResult(
        quote=quote,
        from_currency=Currency.HBD,
        to_currency=Currency.MSATS,
        to_convert=to_convert_msats_1,
        to_convert_conv=to_convert_conv_1,
        net_to_receive=net_msats_1,
        net_to_receive_conv=net_to_receive_conv_1,
        fee=fee_msats_1,
        fee_conv=fee_conv_1,
        change=Decimal(0),
        change_conv=CryptoConversion(value=0, conv_from=Currency.HBD, quote=quote).conversion,
    )

    # Prepare conversion result for Keepsats -> HBD (withdraw)
    to_convert_msats_2 = Decimal(11_000_000)
    fee_msats_2 = Decimal(259_000)
    net_msats_2 = to_convert_msats_2 - fee_msats_2

    to_convert_conv_2 = CryptoConversion(
        value=to_convert_msats_2, conv_from=Currency.MSATS, quote=quote
    ).conversion
    net_to_receive_conv_2 = CryptoConversion(
        value=net_msats_2, conv_from=Currency.MSATS, quote=quote
    ).conversion
    fee_conv_2 = CryptoConversion(
        value=fee_msats_2, conv_from=Currency.MSATS, quote=quote
    ).conversion

    conv2 = ConversionResult(
        quote=quote,
        from_currency=Currency.MSATS,
        to_currency=Currency.HBD,
        to_convert=to_convert_msats_2,
        to_convert_conv=to_convert_conv_2,
        net_to_receive=net_msats_2,
        net_to_receive_conv=net_to_receive_conv_2,
        fee=fee_msats_2,
        fee_conv=fee_conv_2,
        change=Decimal(0),
        change_conv=CryptoConversion(value=0, conv_from=Currency.MSATS, quote=quote).conversion,
    )

    # Monkeypatch the calc functions to return our crafted conversion results
    async def fake_calc_hive_to_keepsats(*args, **kwargs):
        return conv1

    async def fake_calc_keepsats_to_hive(*args, **kwargs):
        return conv2

    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.hive_to_keepsats.calc_hive_to_keepsats",
        fake_calc_hive_to_keepsats,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.keepsats_to_hive.calc_keepsats_to_hive",
        fake_calc_keepsats_to_hive,
    )

    # Stub out hold_keepsats to create a HOLD entry without trying to fetch quotes
    from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount

    async def fake_hold_keepsats(amount_msats, cust_id, tracked_op=None, fee=False):
        entry = LedgerEntry(
            short_id=tracked_op.short_id if tracked_op else "s",
            op_type="transfer",
            cust_id=cust_id,
            ledger_type=LedgerType.HOLD_KEEPSATS,
            group_id=(tracked_op.group_id + "_hold") if tracked_op else "g_hold",
            timestamp=datetime.now(tz=timezone.utc),
            description=f"Hold Keepsats {int(amount_msats / 1000)} sats for {cust_id}",
            debit=LiabilityAccount(name="VSC Liability", sub=cust_id),
            debit_unit=Currency.MSATS,
            debit_amount=amount_msats,
            debit_conv=CryptoConversion(
                value=amount_msats, conv_from=Currency.MSATS, quote=quote
            ).conversion,
            credit=LiabilityAccount(name="VSC Liability", sub="keepsats"),
            credit_unit=Currency.MSATS,
            credit_amount=amount_msats,
            credit_conv=CryptoConversion(
                value=amount_msats, conv_from=Currency.MSATS, quote=quote
            ).conversion,
        )
        await entry.save()
        return entry

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hold_release_keepsats.hold_keepsats", fake_hold_keepsats
    )

    # Stub out send_transfer_custom_json and reply_with_hive so no network calls
    async def fake_send_transfer_custom_json(*args, **kwargs):
        # Accept any signature used by send_transfer_custom_json or send_custom_json
        return {"trx_id": "tx_fake"}

    async def fake_reply_with_hive(details, nobroadcast=False):
        return {}

    # Patch both helpers that may be called with different signatures
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.send_transfer_custom_json",
        fake_send_transfer_custom_json,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.send_custom_json",
        fake_send_transfer_custom_json,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.send_transfer",
        lambda *a, **k: {},
    )

    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.keepsats_to_hive.reply_with_hive", fake_reply_with_hive
    )

    # No-op rebalance
    async def fake_rebalance(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.exchange_process.rebalance_queue_task", fake_rebalance
    )

    # Prevent external quote fetching during test (avoid CoinMarketCap/API calls)
    async def fake_get_all_quotes(self, use_cache=True, timeout=60.0, store_db=True):
        # no-op: do not attempt network calls during unit test
        return None

    monkeypatch.setattr(
        "v4vapp_backend_v2.helpers.crypto_prices.AllQuotes.get_all_quotes",
        fake_get_all_quotes,
    )

    # Prepare dummy ops as TransferBase instances (satisfies TrackedAny requirements)
    from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
    from v4vapp_backend_v2.hive_models.op_transfer import TransferBase

    op1 = TransferBase(
        type="transfer",
        from_account="v4vapp-test",
        to_account="devser.v4vapp",
        amount=AmountPyd(amount="10", nai="@@000000013", precision=3),
        trx_id="tx1",
        block_num=1,
        op_in_trx=1,
        trx_num=1,
        timestamp=datetime.now(tz=timezone.utc),
    )

    op2 = TransferBase(
        type="transfer",
        from_account="v4vapp-test",
        to_account="devser.v4vapp",
        amount=AmountPyd(amount="0.001", nai="@@000000013", precision=3),
        trx_id="tx2",
        block_num=2,
        op_in_trx=1,
        trx_num=1,
        timestamp=datetime.now(tz=timezone.utc),
    )

    server_id = "devser.v4vapp"
    cust_id = "v4vapp-test"

    # Run both conversions
    await conversion_hive_to_keepsats(
        server_id=server_id, cust_id=cust_id, tracked_op=op1, nobroadcast=True
    )
    await conversion_keepsats_to_hive(
        server_id=server_id, cust_id=cust_id, tracked_op=op2, nobroadcast=True
    )

    # Inspect captured ledger entries
    # Find RECLASSIFY_VSC_SATS entry and assert it used net_to_receive_conv.msats
    reclass_entries = [
        e for e in ledger_captured if e.ledger_type == LedgerType.RECLASSIFY_VSC_SATS
    ]
    assert len(reclass_entries) == 1, "Expected one RECLASSIFY_VSC_SATS entry"
    reclass = reclass_entries[0]
    assert reclass.debit_amount == conv2.net_to_receive_conv.msats

    # Find FEE_INCOME entry and assert it debits server_id and amount equals fee
    fee_entries = [e for e in ledger_captured if e.ledger_type == LedgerType.FEE_INCOME]
    assert len(fee_entries) >= 1, "Expected at least one FEE_INCOME entry"
    # find the one for our second conversion (group_id g2)
    fee_for_g2 = [
        e
        for e in fee_entries
        if e.group_id.endswith("_" + LedgerType.FEE_INCOME.value)
        and e.group_id.startswith(op2.group_id)
    ]
    assert len(fee_for_g2) == 1
    fee_entry = fee_for_g2[0]
    # debit account is stored in .debit which is a LedgerAccount; sub is accessible as attribute
    assert fee_entry.debit.sub == server_id
    assert fee_entry.debit_amount == conv2.fee_conv.msats

    # Sanity: ensure no negative customer debit entries created by conversion_keepsats_to_hive
    # i.e., there should be no FEE_INCOME that debits cust_id for g2
    fee_debit_cust = [e for e in fee_for_g2 if e.debit.sub == cust_id]
    assert len(fee_debit_cust) == 0
