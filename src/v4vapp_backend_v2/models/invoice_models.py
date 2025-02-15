from typing import List
from pydantic import BaseModel, ConfigDict, Field, validator
from datetime import datetime, timezone
from bson import Int64
from google.protobuf.json_format import MessageToDict
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc


class BSONInt64(Int64):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value, field):
        if isinstance(value, str):
            try:
                value = Int64(int(value))
            except ValueError:
                raise ValueError(f"Value {value} is not a valid int64")
        elif isinstance(value, int):
            value = Int64(value)
        elif not isinstance(value, Int64):
            raise TypeError(f"Value {value} is not a valid int64")
        return value


class InvoiceHTLC(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    chan_id: BSONInt64
    htlc_index: BSONInt64 | None = None
    amt_msat: BSONInt64
    accept_height: int
    accept_time: datetime
    resolve_time: datetime
    expiry_height: int
    state: str
    custom_records: dict | None = None
    mpp_total_amt_msat: BSONInt64 | None = None
    amp: dict | None = None


class Invoice(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    memo: str = ""
    r_preimage: str | None = None
    r_hash: str | None = None
    value: BSONInt64 | None = None
    value_msat: BSONInt64 | None = None
    settled: bool = False
    creation_date: datetime
    settle_date: datetime | None = None
    payment_request: str | None = None
    description_hash: str | None = None
    expiry: int | None = None
    fallback_addr: str | None = None
    cltv_expiry: int
    route_hints: List[dict] | None = None
    private: bool | None = None
    add_index: BSONInt64 | None = None
    settle_index: BSONInt64 | None = None
    amt_paid: BSONInt64 | None = None
    amt_paid_sat: BSONInt64 | None = None
    amt_paid_msat: BSONInt64 | None = None
    state: str | None = None
    htlcs: List[InvoiceHTLC] | None = None
    features: dict
    is_keysend: bool = False
    payment_addr: str | None = None
    is_amp: bool = False
    amp_invoice_state: dict | None = None


class ListInvoiceResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    invoices: List[Invoice]
    last_index_offset: BSONInt64
    first_index_offset: BSONInt64


def convert_timestamp_to_datetime(timestamp):
    """
    Convert a Unix timestamp to a timezone-aware datetime object.

    Args:
        timestamp (float or int): The Unix timestamp to convert.

    Returns:
        datetime: A timezone-aware datetime object in UTC.

    Raises:
        ValueError: If the timestamp cannot be converted to a float.
    """
    return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)


def convert_datetime_fields(invoice: dict) -> dict:
    """
    Converts timestamp fields in an invoice dictionary to datetime objects.

    This function checks for the presence of specific timestamp fields in the
    provided invoice dictionary and converts them to datetime objects using
    the `convert_timestamp_to_datetime` function. The fields that are converted
    include:
    - "creation_date"
    - "settle_date"
    - "accept_time" (within each HTLC in the "htlcs" list)
    - "resolve_time" (within each HTLC in the "htlcs" list)

    Args:
        invoice (dict): The invoice dictionary containing timestamp fields.

    Returns:
        dict: The invoice dictionary with the specified timestamp fields
              converted to datetime objects.
    """
    if "creation_date" in invoice:
        invoice["creation_date"] = convert_timestamp_to_datetime(
            invoice["creation_date"]
        )
    if "settle_date" in invoice:
        invoice["settle_date"] = convert_timestamp_to_datetime(invoice["settle_date"])
    for htlc in invoice.get("htlcs", []):
        if "accept_time" in htlc:
            htlc["accept_time"] = convert_timestamp_to_datetime(htlc["accept_time"])
        if "resolve_time" in htlc:
            htlc["resolve_time"] = convert_timestamp_to_datetime(htlc["resolve_time"])
    return invoice


def protobuf_invoice_to_pydantic(invoice: lnrpc.Invoice) -> Invoice:
    """
    Converts a protobuf Invoice object to a Pydantic Invoice model.

    Args:
        invoice (lnrpc.Invoice): The protobuf Invoice object to be converted.

    Returns:
        Invoice: The converted Pydantic Invoice model. If an error occurs during validation,
                 an empty Invoice model is returned.
    """
    invoice_dict = MessageToDict(invoice, preserving_proto_field_name=True)
    invoice_dict = convert_datetime_fields(invoice_dict)
    try:
        invoice_model = Invoice.model_validate(invoice_dict)
    except Exception as e:
        print(e)
        return Invoice()
    return invoice_model


def protobuf_to_pydantic(message) -> ListInvoiceResponse:
    message_dict = MessageToDict(message, preserving_proto_field_name=True)
    for invoice in message_dict.get("invoices", []):
        invoice = convert_datetime_fields(invoice)
        try:
            invoice_model = Invoice.model_validate(invoice)
        except Exception as e:
            print(e)
        pass
    return ListInvoiceResponse.model_validate(message_dict)
