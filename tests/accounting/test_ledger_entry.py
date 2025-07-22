from datetime import datetime, timedelta, timezone
from pathlib import Path
from pprint import pprint

import pytest

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import timestamp_inc
from v4vapp_backend_v2.models.payment_models import Payment


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
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


@pytest.mark.asyncio
async def test_ledger_entry_constructor():
    ledger_type = LedgerType.CONV_HIVE_TO_LIGHTNING
    timestamp = timestamp_inc(datetime.now(tz=timezone.utc), inc=timedelta(seconds=0.01))
    payment = Payment()
    await TrackedBaseModel.update_quote(store_db=False)
    quote = TrackedBaseModel.last_quote
    conversion_debit_amount = 3_318_000
    conversion_credit_debit_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=conversion_debit_amount,
        quote=quote,
    ).conversion
    node_name = "test_node"
    ledger_entry = LedgerEntry(
        cust_id="cust_id",
        ledger_type=ledger_type,
        group_id=f"group_id_{ledger_type}",
        timestamp=next(timestamp),
        description=f"Conv Hive to Lightning {ledger_type}",
        debit=AssetAccount(
            name="Treasury Lightning",
            sub=node_name,  # This is the SERVER Lightning
            contra=False,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=conversion_debit_amount,
        debit_conv=conversion_credit_debit_conv,
        credit=AssetAccount(
            name="Customer Deposits Hive",
            sub=node_name,  # This is the Server
            contra=False,
        ),
        credit_unit=Currency.HIVE,
        credit_amount=conversion_credit_debit_conv.hive,
        credit_conv=conversion_credit_debit_conv,
    )
    assert isinstance(ledger_entry, LedgerEntry)
    print(ledger_entry)
    print(ledger_entry.draw_t_diagram())

    print(ledger_entry.log_str)
    pprint(ledger_entry.log_extra)
    assert ledger_entry.is_completed
    assert float(ledger_entry.debit_amount_signed) == conversion_debit_amount
    assert float(ledger_entry.credit_amount_signed) == -conversion_credit_debit_conv.hive

    print(ledger_entry.conv_signed)


@pytest.mark.asyncio
async def test_ledger_entry_constructor_conv_account():
    ledger_type = LedgerType.CONV_HIVE_TO_LIGHTNING
    timestamp = timestamp_inc(datetime.now(tz=timezone.utc), inc=timedelta(seconds=0.01))
    payment = Payment()
    await TrackedBaseModel.update_quote(store_db=False)
    quote = TrackedBaseModel.last_quote
    conversion_debit_amount = 3_318_000
    conversion_credit_debit_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=conversion_debit_amount,
        quote=quote,
    ).conversion
    node_name = "test_node"
    ledger_entry = LedgerEntry(
        cust_id="cust_id",
        ledger_type=ledger_type,
        group_id=f"group_id_{ledger_type}",
        timestamp=next(timestamp),
        description=f"Conv Hive to Lightning {ledger_type}",
        debit=AssetAccount(
            name="Treasury Lightning",
            sub=node_name,  # This is the SERVER Lightning
            contra=False,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=conversion_debit_amount,
        debit_conv=conversion_credit_debit_conv,
        credit=AssetAccount(
            name="Converted Hive Offset",
            sub=node_name,  # This is the Server
            contra=True,
        ),
        credit_unit=Currency.HIVE,
        credit_amount=conversion_credit_debit_conv.hive,
        credit_conv=conversion_credit_debit_conv,
    )
    assert isinstance(ledger_entry, LedgerEntry)
    print(ledger_entry)
    print(ledger_entry.draw_t_diagram())

    print(ledger_entry.log_str)
    pprint(ledger_entry.log_extra)
    assert ledger_entry.is_completed
    assert float(ledger_entry.debit_amount_signed) == conversion_debit_amount
    assert float(ledger_entry.credit_amount_signed) == -conversion_credit_debit_conv.hive

    print(ledger_entry.conv_signed)
