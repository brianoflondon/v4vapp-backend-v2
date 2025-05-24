import json

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.models.invoice_models import Invoice


def test_invoice_model_validate():
    """
    Test the InvoiceModel.validate method.
    """
    TrackedBaseModel.update_quote_sync()
    with open("tests/data/lnd_to_pydantic_models/invoices.jsonl", "r") as f:
        for line in f:
            invoice = None
            if '"fullDocument"' in line:
                full_document = json.loads(line)["change"]["fullDocument"]
                invoice = Invoice.model_validate(full_document)
                print(invoice.hive_accname)
                if invoice.custom_records:
                    print(invoice.custom_records.podcast)
