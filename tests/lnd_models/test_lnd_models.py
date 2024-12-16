import base64
import hashlib
import json
from typing import Generator

import pytest
from pydantic import ValidationError

from v4vapp_backend_v2.models.htlc_event_models import HtlcTrackingList
from v4vapp_backend_v2.models.lnd_models import LNDInvoice


def validate_preimage(r_preimage_base64: str, r_hash_base64: str) -> bool:
    """
    Validate the r_preimage against the r_hash.

    Args:
        r_preimage_base64 (str): Base64-encoded preimage.
        r_hash_base64 (str): Base64-encoded hash.

    Returns:
        bool: True if the preimage is valid, False otherwise.
    """
    # Decode the base64-encoded preimage and hash
    r_preimage = base64.b64decode(r_preimage_base64)
    r_hash = base64.b64decode(r_hash_base64)

    # Compute the SHA-256 hash of the preimage
    computed_hash = hashlib.sha256(r_preimage).digest()

    # Validate the preimage against the hash
    return computed_hash == r_hash


def test_validate_preimage():
    # Test data
    r_preimage_base64 = "CuAat6H7E9z1rbnqhO83AQZf3taiIkMjVwLddL3AVSs="
    r_hash_base64 = "ZUYdrumI7CvOP7nLNE3981ClLwXA9hTY5wvtUO9G00Q="

    # Validate the preimage against the hash
    assert validate_preimage(r_preimage_base64, r_hash_base64) is True


def read_log_file_invoices(file_path: str) -> Generator[LNDInvoice, None, None]:
    with open(file_path, "r") as file:
        # Parse each line as JSON and yield the htlc_event data
        for line in file.readlines():
            try:
                log_entry = json.loads(line)
                if "invoice_data" in log_entry:
                    yield LNDInvoice.model_validate(log_entry["invoice_data"])

            except ValidationError as e:
                print(e)
                continue
            except Exception as e:
                print(e)
                continue


def test_log_file_invoices():
    tracking = HtlcTrackingList()
    try:
        for invoice in read_log_file_invoices("tests/data/invoices_test_data.log"):
            tracking.add_invoice(invoice)
            print(invoice.add_index)
            assert tracking.lookup_invoice(invoice.add_index) == invoice
            print(tracking.invoice_htlc_id(invoice.add_index))
            print("-" * 80)

        for invoice in tracking.invoices:
            if invoice.htlcs:
                assert invoice == tracking.lookup_invoice_by_htlc_id(
                    int(invoice.htlcs[0]["htlc_index"])
                )
            assert invoice == tracking.lookup_invoice(invoice.add_index)

            tracking.remove_invoice(invoice.add_index)

        assert len(tracking.invoices) == 0

    except FileNotFoundError as e:
        print(e)
        assert False
    except ValidationError as e:
        print(e)
        assert False
