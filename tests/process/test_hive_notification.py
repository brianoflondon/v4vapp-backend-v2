import pytest
from decimal import Decimal
from pathlib import Path
from datetime import datetime

from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.process.hive_notification import reply_with_hive
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance



# helper to produce a minimal Transfer object for tracked_op

def make_transfer_op() -> Transfer:
    # build with construct and supply the few attributes we will read later
    tx = Transfer.construct(
        group_id="GID",
        short_id="SID",
        op_type="transfer",
        log_extra={},
    )
    return tx


@pytest.mark.asyncio
async def test_reply_with_hive_force_custom_json(monkeypatch):
    """reply_with_hive should honour the force_custom_json flag."""
    sent = {"transfer": False, "custom": False}

    async def fake_send_transfer(*args, **kwargs):
        sent["transfer"] = True
        return {"trx_id": "t"}

    async def fake_send_custom_json(*args, **kwargs):
        sent["custom"] = True
        return {"trx_id": "cj"}

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.send_transfer",
        fake_send_transfer,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.send_custom_json",
        fake_send_custom_json,
    )
    # Make sure LockStr thinks the ID is a Hive account regardless of input
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.LockStr.is_hive",
        True,
        raising=False,
    )
    # stub Hive client so we don't need real credentials
    async def fake_hive_client(nobroadcast=False):
        return (object(), "server")
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.get_verified_hive_client",
        fake_hive_client,
    )
    # override Transfer.log_extra property so it does not try to compute
    # fields that are missing on our constructed object
    monkeypatch.setattr(
        Transfer,
        "log_extra",
        property(lambda self: {}),
        raising=False,
    )
    # ensure group_id and short_id properties can be accessed without needing block_num etc
    monkeypatch.setattr(
        Transfer,
        "group_id",
        property(lambda self: "dummy_group"),
        raising=False,
    )
    monkeypatch.setattr(
        Transfer,
        "short_id",
        property(lambda self: "dummy_short"),
        raising=False,
    )
    # prevent any save call from touching a database
    async def fake_save(self, *args, **kwargs):
        return {}
    monkeypatch.setattr(Transfer, "save", fake_save, raising=False)
    # stub balance lookup to avoid mongo access
    async def fake_check(cust_id, amount):
        return amount
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.hive_notification.check_for_outstanding_hive_balance",
        fake_check,
    )

    details = HiveReturnDetails(
        tracked_op=make_transfer_op(),
        original_memo="memo",
        action=ReturnAction.CHANGE,
        pay_to_cust_id="someone",
        msats=Decimal(1000),
        force_custom_json=True,
    )

    await reply_with_hive(details)
    assert not sent["transfer"], "transfer should not be called when force flag is set"
    assert sent["custom"], "custom_json must be sent when force flag is set"

    # clearing and test reverse behaviour
    sent["transfer"] = False
    sent["custom"] = False
    details.force_custom_json = False
    await reply_with_hive(details)
    assert sent["transfer"], "transfer should be used when force flag is cleared"
    assert not sent["custom"], "custom_json should not be used when not forced"


@pytest.mark.asyncio
async def test_process_invoice_sets_force_flag(monkeypatch):
    """process_lightning_receipt_stage_2 should mark low‑value invoices."""

    from v4vapp_backend_v2.process.process_invoice import process_lightning_receipt_stage_2
    from v4vapp_backend_v2.models.invoice_models import Invoice

    def make_invoice(sats: Decimal):
        # memo includes #sats so that recv_currency property returns SATS rather
        # than the default HIVE (which would trigger the conversion path).
        inv = Invoice.construct(
            cust_id="bob",
            is_lndtohive=True,
            value=sats,
            value_msat=sats * 1000,
            memo="#sats",
            group_id="gid",
            short_id="sid",
            op_type="invoice",
        )
        return inv

    captured = {}

    async def fake_reply(details, nobroadcast=False):
        captured["details"] = details
        return {}

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_invoice.reply_with_hive",
        fake_reply,
    )

    threshold = V4VConfig().data.force_custom_json_payment_sats
    assert threshold > 0

    # below threshold should force
    invoice = make_invoice(Decimal(threshold) - Decimal(10))
    await process_lightning_receipt_stage_2(invoice)
    assert captured["details"].force_custom_json is True

    # above threshold should not force
    invoice = make_invoice(Decimal(threshold) + Decimal(10))
    captured.clear()
    await process_lightning_receipt_stage_2(invoice)
    assert captured["details"].force_custom_json is False
