import json
from pathlib import Path

import pytest

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.models.invoice_models import Invoice
from tests.get_last_quote import last_quote

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
