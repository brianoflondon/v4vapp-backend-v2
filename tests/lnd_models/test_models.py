import base64
import hashlib
import json
from datetime import datetime
from typing import Generator

import pytest
from pydantic import ValidationError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.depreciated.htlc_event_models import HtlcTrackingList
from v4vapp_backend_v2.models.invoice_models import (
    Invoice,
    ListInvoiceResponse,
    protobuf_to_pydantic,
)
from v4vapp_backend_v2.models.payment_models import ListPaymentsResponse


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


def read_log_file_invoices(file_path: str) -> Generator[Invoice, None, None]:
    with open(file_path, "r") as file:
        # Parse each line as JSON and yield the htlc_event data
        for line in file.readlines():
            try:
                log_entry = json.loads(line)
                if "invoice_data" in log_entry:
                    yield Invoice.model_validate(log_entry["invoice_data"])

            except ValidationError as e:
                print(e)
                continue
            except Exception as e:
                print(e)
                continue


def test_log_file_invoices():
    tracking = HtlcTrackingList()
    try:
        for invoice in read_log_file_invoices("tests/data/invoices_test_data.safe_log"):
            tracking.add_invoice(invoice)
            print(invoice.add_index)
            assert tracking.lookup_invoice(invoice.add_index) == invoice
            print(tracking.invoice_htlc_id(invoice.add_index))
            print("-" * 80)

        for invoice in tracking.invoices:
            if invoice.htlcs:
                assert invoice == tracking.lookup_invoice_by_htlc_id(
                    int(invoice.htlcs[0].htlc_index)
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


def test_remove_expired_invoices():
    tracking = HtlcTrackingList()
    try:
        for invoice in read_log_file_invoices("tests/data/invoices_test_data.safe_log"):
            tracking.add_invoice(invoice)
            print(invoice.add_index)
            print("-" * 80)

        tracking.remove_expired_invoices()
        assert tracking.num_invoices == 0

    except FileNotFoundError as e:
        print(e)
        assert False
    except ValidationError as e:
        print(e)
        assert False


"""
This was the snippet of code use to create the test data:
            request = lnrpc.ListInvoiceRequest(
                pending_only=False,
                index_offset=index_offset,
                num_max_invoices=num_max_invoices,
                reversed=True,
            )
            invoices_raw: lnrpc.ListInvoiceResponse = await client.call(
                client.lightning_stub.ListInvoices,
                request,
            )
            with open("list_invoices_raw.bin", "wb") as f:
                f.write(invoices_raw.SerializeToString())
"""


def read_list_invoices_raw(file_path: str) -> lnrpc.ListInvoiceResponse:
    with open(file_path, "rb") as file:
        return lnrpc.ListInvoiceResponse.FromString(file.read())


def test_read_list_invoices_raw():
    """
    Test the `read_list_invoices_raw` function to ensure it correctly reads and processes
    raw invoice data from a binary file.

    This test performs the following checks:
    1. Verifies that `read_list_invoices_raw` returns a non-empty response.
    2. Ensures the response is an instance of `lnrpc.ListInvoiceResponse`.
    3. Converts the response to a Pydantic model using `protobuf_to_pydantic` and verifies the conversion.
    4. Converts the response to a `ListInvoiceResponse` model and verifies the conversion.
    5. Checks that the two converted responses are equal.
    6. Iterates through each invoice in the response and verifies that the `creation_date` attribute
       is an instance of `datetime`.

    Raises:
        AssertionError: If any of the assertions fail.
    """
    lnrpc_list_invoices = read_list_invoices_raw(
        "tests/data/lnd_lists/list_invoices_raw.bin"
    )
    assert lnrpc_list_invoices
    assert isinstance(lnrpc_list_invoices, lnrpc.ListInvoiceResponse)
    list_invoice_response = protobuf_to_pydantic(lnrpc_list_invoices)
    assert list_invoice_response
    list_invoice_response2 = ListInvoiceResponse(lnrpc_list_invoices)
    assert list_invoice_response2
    assert list_invoice_response == list_invoice_response2

    for lnrpc_invoice in lnrpc_list_invoices.invoices:
        invoice = Invoice(lnrpc_invoice)
        assert isinstance(invoice.creation_date, datetime)


def read_list_payments_raw(file_path: str) -> lnrpc.ListPaymentsResponse:
    with open(file_path, "rb") as file:
        return lnrpc.ListPaymentsResponse.FromString(file.read())


def test_read_list_payments_raw_destination_pub_keys():
    lnrpc_list_payments = read_list_payments_raw(
        "tests/data/lnd_lists/list_payments_raw.bin"
    )
    assert lnrpc_list_payments
    assert isinstance(lnrpc_list_payments, lnrpc.ListPaymentsResponse)
    list_payment_response = ListPaymentsResponse(lnrpc_list_payments)
    assert len(list_payment_response.payments) == 1000
    for payment in list_payment_response.payments:
        assert isinstance(payment.creation_date, datetime)
        try:
            payment.destination_pub_keys
        except Exception as e:
            print(e)
            assert False


def test_read_list_payments_pydantic_conversions():
    lnrpc_list_payments = read_list_payments_raw(
        "tests/data/lnd_lists/list_payments_raw.bin"
    )
    assert lnrpc_list_payments
    assert isinstance(lnrpc_list_payments, lnrpc.ListPaymentsResponse)
    list_payment_response = ListPaymentsResponse(lnrpc_list_payments)

    list_payment_response_dict = list_payment_response.model_dump()
    list_payment_response2 = ListPaymentsResponse.model_validate(
        list_payment_response_dict
    )
    assert list_payment_response == list_payment_response2


def test_route_in_payments():
    lnrpc_list_payments = read_list_payments_raw(
        "tests/data/lnd_lists/list_payments_raw.bin"
    )
    assert lnrpc_list_payments
    assert isinstance(lnrpc_list_payments, lnrpc.ListPaymentsResponse)
    list_payment_response = ListPaymentsResponse(lnrpc_list_payments)

    for payment in list_payment_response.payments:
        try:
            print(payment.destination_pub_keys)
            print(payment.destination)
        except Exception as e:
            print(e)
            assert False
