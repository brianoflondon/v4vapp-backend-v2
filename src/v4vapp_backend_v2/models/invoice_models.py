import re
from datetime import datetime
from typing import Any, List

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, computed_field

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import LoggerFunction, logger
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.models.custom_records import KeysendCustomRecord, b64_decode
from v4vapp_backend_v2.models.pydantic_helpers import BSONInt64, convert_datetime_fields

# This is the regex for finding if a given message is an LND invoice to pay.
# This looks for #v4vapp v4vapp
LND_INVOICE_TAG = r"(.*)(#(v4vapp))"


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


# TODO: #92 this is where the custom_records in each invoice are stored this is where we will decode the custom records


class Invoice(BaseModel):
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

    is_lndtohive: bool = False
    hive_accname: AccNameType | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(__pydantic_self__, lnrpc_invoice: lnrpc.Invoice = None, **data: Any) -> None:
        if lnrpc_invoice and isinstance(lnrpc_invoice, lnrpc.Invoice):
            data_dict = MessageToDict(lnrpc_invoice, preserving_proto_field_name=True)
            invoice_dict = convert_datetime_fields(data_dict)
        else:
            invoice_dict = convert_datetime_fields(data)
        super().__init__(**invoice_dict)

        # perform my check to see if this invoice can be paid to Hive
        if __pydantic_self__.memo:
            match = re.match(LND_INVOICE_TAG, __pydantic_self__.memo.lower())
            if match:
                __pydantic_self__.is_lndtohive = True

        __pydantic_self__.hive_accname = __pydantic_self__.hive_account()

    def hive_account(self) -> AccNameType | None:
        if self.memo:
            match = re.match(LND_INVOICE_TAG, self.memo.lower())
            if match:
                extracted_value = match.group(1)
        if self.htlcs[0]:
            if self.htlcs[0].custom_records.get("818818"):
                value = self.htlcs[0].custom_records.get("818818")
                try:
                    extracted_value = b64_decode(value)
                except Exception as e:
                    logger.warning(f"Error decoding {value}: {e}", extra={"notification": False})

        if extracted_value:
            hive_accname = AccNameType(extracted_value)
            return hive_accname

        return None

    @computed_field
    def custom_record(self) -> KeysendCustomRecord | None:
        if self.htlcs[0].custom_records:
            for key, value in self.htlcs[0].custom_records.items():
                if key == 818818:
                    return b64_decode(value)

    def invoice_message(self) -> str:
        if self.settled:
            return (
                f"✅ Settled invoice {self.add_index} with memo {self.memo} {self.value:,.0f} sats"
            )
        else:
            return (
                f"✅ Valid   invoice {self.add_index} with memo {self.memo} {self.value:,.0f} sats"
            )

    def invoice_log(self, logger_func: LoggerFunction, send_notification: bool = False) -> None:
        logger_func(
            self.invoice_message(),
            extra={
                "notification": send_notification,
                "invoice": self.model_dump(exclude_none=True, exclude_unset=True),
            },
        )


class ListInvoiceResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    invoices: List[Invoice]
    last_index_offset: BSONInt64
    first_index_offset: BSONInt64

    def __init__(
        __pydantic_self__,
        lnrpc_list_invoice_response: lnrpc.ListInvoiceResponse = None,
        **data: Any,
    ) -> None:
        if lnrpc_list_invoice_response and isinstance(
            lnrpc_list_invoice_response, lnrpc.ListInvoiceResponse
        ):
            list_invoice_dict = MessageToDict(
                lnrpc_list_invoice_response, preserving_proto_field_name=True
            )
            list_invoice_dict["invoices"] = [
                Invoice.model_validate(invoice) for invoice in list_invoice_dict["invoices"]
            ]
            super().__init__(**list_invoice_dict)
        else:
            super().__init__(**data)
            if not __pydantic_self__.invoices:
                __pydantic_self__.invoices = []


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
        return invoice_model
    except Exception as e:
        print(e)
        return Invoice()


def protobuf_to_pydantic(message) -> ListInvoiceResponse:
    message_dict = MessageToDict(message, preserving_proto_field_name=True)
    for invoice in message_dict.get("invoices", []):
        invoice = convert_datetime_fields(invoice)
        # try:
        #     invoice_model = Invoice.model_validate(invoice)
        # except Exception as e:
        #     print(e)
    return ListInvoiceResponse.model_validate(message_dict)
    return ListInvoiceResponse.model_validate(message_dict)
