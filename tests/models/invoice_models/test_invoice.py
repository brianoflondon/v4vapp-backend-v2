import json
from pathlib import Path

import pytest

from tests.get_last_quote import last_quote
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote
from v4vapp_backend_v2.models.invoice_models import Invoice


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    InternalConfig(config_filename=str(Path(test_config_path, "config.yaml")))
    TrackedBaseModel.last_quote = last_quote()
    yield
    InternalConfig().shutdown()


@pytest.fixture
def set_base_config_path_bad(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )

    monkeypatch.setattr(
        "v4vapp_backend_v2.lnd_grpc.lnd_connection.InternalConfig._instance",
        None,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


def test_invoice_model_validate():
    """
    Test the InvoiceModel.validate method.
    """
    TrackedBaseModel.last_quote = last_quote()
    with open("tests/data/lnd_to_pydantic_models/invoices.jsonl", "r") as f:
        for line in f:
            invoice = None
            if '"fullDocument"' in line:
                full_document = json.loads(line)["change"]["fullDocument"]
                invoice = Invoice.model_validate(full_document)
                print(invoice.cust_id)
                if invoice.custom_records:
                    print(invoice.custom_records.podcast)


@pytest.mark.asyncio
async def test_invoice_fixed_quote_magisats():
    """Test that fixed_quote returns the correct quote for a MAGISATS invoice."""
    net_sats = 50000
    fixed_hive_quote = await FixedHiveQuote.create_quote(
        magisats=True, sats=float(net_sats), store_db=False
    )

    # Memo format: account #MAGISATS #UUID <uuid> #v4vapp
    memo = f"@testuser #MAGISATS #UUID {fixed_hive_quote.unique_id} #v4vapp"
    invoice = Invoice(
        memo=memo,
        value_msat=fixed_hive_quote.sats_send * 1000,
    )

    assert invoice.is_magisats
    result = invoice.fixed_quote
    assert result is not None
    assert result.unique_id == fixed_hive_quote.unique_id
    assert result.sats_send == fixed_hive_quote.sats_send


@pytest.mark.asyncio
async def test_invoice_fixed_quote_magisats_wrong_amount():
    """Test that fixed_quote returns None when invoice value_msat doesn't match the quote."""
    net_sats = 50000
    fixed_hive_quote = await FixedHiveQuote.create_quote(
        magisats=True, sats=float(net_sats), store_db=False
    )

    memo = f"@testuser #MAGISATS #UUID {fixed_hive_quote.unique_id} #v4vapp"
    # Use net_sats (without fee) — mismatches stored sats_send
    invoice = Invoice(
        memo=memo,
        value_msat=net_sats * 1000,
    )

    assert invoice.is_magisats
    # fixed_quote should return None because value_msat // 1000 != sats_send
    assert invoice.fixed_quote is None


@pytest.mark.asyncio
async def test_invoice_fixed_quote_magisats_no_uuid():
    """Test that fixed_quote returns None for a MAGISATS invoice without a UUID in the memo."""
    invoice = Invoice(
        memo="@testuser #MAGISATS #v4vapp",
        value_msat=50000 * 1000,
    )

    assert invoice.is_magisats
    assert invoice.fixed_quote is None
