from datetime import datetime, timezone
from typing import Any

from bson import Int64
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.json_format import MessageToDict as OriginalMessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1


def custom_list_invoice_response_to_dict(
    list_invoice_resp: lnrpc.ListInvoiceResponse,
) -> dict:
    """
    Custom function to convert an LND ListInvoiceResponse message to a dictionary while preserving
    int64 and uint64 fields as integers.

    Args:
        invoices: The LND ListInvoiceResponse message instance to convert.

    Returns:
        dict: The dictionary representation of the LND ListInvoiceResponse message.
    """
    # Recursively convert int64 and uint64 fields from strings to integers in the invoices list
    invoices_list = []
    if list_invoice_resp.invoices:
        for invoice in list_invoice_resp.invoices:
            invoice_dict = custom_lnrpc_invoice_to_dict(invoice)
            invoices_list.append(invoice_dict)

    invoices_dict = {
        "last_index_offset": int(list_invoice_resp.last_index_offset),
        "first_index_offset": int(list_invoice_resp.first_index_offset),
        "invoices": invoices_list,
    }

    return invoices_dict


def custom_lnrpc_invoice_to_dict(invoice: lnrpc.Invoice, **kwargs):
    """
    Custom function to convert an LND Invoice message to a dictionary while preserving
    int64 and uint64 fields as integers.

    Args:
        invoice: The LND Invoice message instance to convert.
        **kwargs: Additional arguments to pass to the original MessageToDict function.

    Returns:
        dict: The dictionary representation of the LND Invoice message.
    """
    # Convert the message to a dictionary using the original MessageToDict function
    invoice_dict = CustomMessageToDict(invoice, **kwargs)
    return invoice_dict


def CustomMessageToDict(message: Any, **kwargs):
    """
    Custom function to convert a Protobuf message to a dictionary while preserving
    int64 and uint64 fields as integers.

    Args:
        message: The Protobuf message instance to convert.
        **kwargs: Additional arguments to pass to the original MessageToDict function.

    Returns:
        dict: The dictionary representation of the Protobuf message.
    """
    # Convert the message to a dictionary using the original MessageToDict function
    message_dict = OriginalMessageToDict(message, **kwargs)

    # Recursively convert int64 and uint64 fields from strings to integers
    def convert_int64_fields(d):
        for key, value in d.items():
            if isinstance(value, dict):
                convert_int64_fields(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        convert_int64_fields(item)
            elif (
                isinstance(value, str)
                and key in message.DESCRIPTOR.fields_by_camelcase_name
            ):
                field = message.DESCRIPTOR.fields_by_name[key]
                if field.cpp_type in (
                    FieldDescriptor.CPPTYPE_INT64,
                    FieldDescriptor.CPPTYPE_UINT64,
                ):
                    try:
                        if check_int32_range(value):
                            d[key] = int(value)
                        elif check_int64_range(value):
                            d[key] = Int64(value)
                        else:
                            raise ValueError(f"Value {value} out of range for int64")
                    except ValueError:
                        pass

    convert_int64_fields(message_dict)
    return message_dict


def check_int32_range(value: str) -> bool:
    if INT32_MIN <= Int64(value) <= INT32_MAX:
        return True
    return False


def check_int64_range(value: str) -> bool:
    if INT64_MIN <= Int64(value) <= INT64_MAX:
        return True
    return False


def convert_timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(
        timestamp.seconds + timestamp.nanos / 1e9, tz=timezone.UTC
    )
