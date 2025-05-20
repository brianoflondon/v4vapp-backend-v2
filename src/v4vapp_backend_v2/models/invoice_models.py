import re
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Dict, List

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import LoggerFunction, logger
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.models.custom_records import (
    DecodedCustomRecord,
    b64_decode,
    decode_all_custom_records,
)
from v4vapp_backend_v2.models.pydantic_helpers import BSONInt64, convert_datetime_fields

# This is the regex for finding if a given message is an LND invoice to pay.
# This looks for #v4vapp v4vapp
# LND_INVOICE_TAG = r"(.*)(#(v4vapp))"
# Updated to separate the hive name at the start of the message
LND_INVOICE_TAG = r"^\s*(\S+).*#v4vapp"


class InvoiceState(StrEnum):
    """
    Enum representing the possible states of an invoice.

    Attributes:
        OPEN (str): The invoice is open and not yet settled.
        SETTLED (str): The invoice has been settled.
        CANCELED (str): The invoice has been canceled.
    """

    OPEN = "OPEN"
    SETTLED = "SETTLED"
    CANCELED = "CANCELED"
    ACCEPTED = "ACCEPTED"


class InvoiceHTLCState(StrEnum):
    """
    Enum representing the possible states of an HTLC (Hashed Time-Locked Contract).

    Attributes:
        ACCEPTED (str): The HTLC is accepted.
        SETTLED (str): The HTLC is settled.
        CANCELED (str): The HTLC has been canceled.
    """

    ACCEPTED = "ACCEPTED"
    SETTLED = "SETTLED"
    CANCELED = "CANCELED"


class InvoiceHTLC(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    chan_id: BSONInt64
    htlc_index: BSONInt64 | None = None
    amt_msat: BSONInt64
    accept_height: int
    accept_time: datetime
    resolve_time: datetime
    expiry_height: int
    state: InvoiceHTLCState
    custom_records: Dict[str, str] | None = None
    mpp_total_amt_msat: BSONInt64 | None = None
    amp: dict | None = None


class Feature(BaseModel):
    name: str = ""
    is_required: bool = False
    is_known: bool = False


# TODO: #92 this is where the custom_records in each invoice are stored this is where we will decode the custom records


class Invoice(TrackedBaseModel):
    """
    Invoice Model

    This class represents an invoice model with various attributes and methods to handle
    invoice-related data. It is designed to work with data from the Lightning Network Daemon (LND)
    and includes functionality for extracting Hive account information and custom records.

    Based on :
    https://github.com/lightningnetwork/lnd/blob/7e50b8438ef5f88841002c4a8c23510928cfe64b/lnrpc/lightning.proto#L3768

    Attributes:
        memo (str): A memo or description for the invoice. Defaults to an empty string.
        r_preimage (str | None): The preimage of the invoice, if available.
        r_hash (str | None): The hash of the invoice, if available.
        value (BSONInt64 | None): The value of the invoice in satoshis.
        value_msat (BSONInt64 | None): The value of the invoice in millisatoshis.
        settled (bool): Indicates whether the invoice is settled. Defaults to False.
        creation_date (datetime): The creation date of the invoice.
        settle_date (datetime | None): The settlement date of the invoice, if available.
        payment_request (str | None): The payment request string for the invoice.
        description_hash (str | None): The hash of the invoice description, if available.
        expiry (int | None): The expiry time of the invoice in seconds.
        fallback_addr (str | None): A fallback address for the invoice, if available.
        cltv_expiry (int): The CLTV expiry value for the invoice.
        route_hints (List[dict] | None): Route hints for the invoice, if available.
        private (bool | None): Indicates whether the invoice is private.
        add_index (BSONInt64 | None): The add index of the invoice, if available.
        settle_index (BSONInt64 | None): The settle index of the invoice, if available.
        amt_paid (BSONInt64 | None): The amount paid for the invoice in satoshis.
        amt_paid_sat (BSONInt64 | None): The amount paid for the invoice in satoshis.
        amt_paid_msat (BSONInt64 | None): The amount paid for the invoice in millisatoshis.
        state (str | None): The state of the invoice, if available.
        htlcs (List[InvoiceHTLC] | None): A list of HTLCs (Hashed Time-Locked Contracts) associated with the invoice.
        features (dict): Features associated with the invoice.
        is_keysend (bool): Indicates whether the invoice is a keysend invoice. Defaults to False.
        payment_addr (str | None): The payment address for the invoice, if available.
        is_amp (bool): Indicates whether the invoice is an AMP (Atomic Multi-Path) invoice. Defaults to False.
        amp_invoice_state (dict | None): The state of the AMP invoice, if available.
        is_lndtohive (bool): Indicates whether the invoice can be paid to Hive. Defaults to False.
        hive_accname (AccNameType | None): The Hive account name associated with the invoice, if available.
        custom_record (KeysendCustomRecord | None): A custom record associated with the invoice, if available.

    Methods:
        __init__(lnrpc_invoice: lnrpc.Invoice = None, **data: Any) -> None:
            Initializes the Invoice object. Converts datetime fields and determines if the invoice
            can be paid to Hive.

        hive_account() -> AccNameType | None:
            Attempts to extract the account name from the `memo` field or the `custom_records` field
            of the first HTLC.

        fill_custom_record() -> None:
            Extracts and validates a custom record from the first HTLC's custom records, if available.
    """

    memo: str = ""
    r_preimage: str = ""
    r_hash: str = ""
    value: BSONInt64 | None = None
    value_msat: BSONInt64 | None = None
    settled: bool = False
    creation_date: datetime | None = None
    settle_date: datetime | None = None
    payment_request: str = ""
    description_hash: str = ""
    expiry: int | None = None
    fallback_addr: str = ""
    cltv_expiry: int | None = None
    route_hints: List[dict] | None = None
    private: bool | None = None
    add_index: BSONInt64 | None = None
    settle_index: BSONInt64 | None = None
    amt_paid: BSONInt64 | None = None
    amt_paid_sat: BSONInt64 | None = None
    amt_paid_msat: BSONInt64 | None = None
    state: InvoiceState | None = None
    htlcs: List[InvoiceHTLC] | None = None
    features: dict[str, Feature] | None = None
    is_keysend: bool = False
    payment_addr: str = ""
    is_amp: bool = False
    amp_invoice_state: dict | None = None

    # Additional fields, not in the LND invoice (but calculated at ingestion time)
    is_lndtohive: bool = Field(
        default=False, description="True if the invoice is a LND to Hive invoice"
    )
    hive_accname: AccNameType | None = Field(
        default=None, description="Hive account name associated with the invoice"
    )

    custom_records: DecodedCustomRecord | None = Field(
        default=None, description="Other custom records associated with the invoice"
    )
    expiry_date: datetime | None = Field(
        default=None, description="Expiry date of the invoice (creation_date + expiry)"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, lnrpc_invoice: lnrpc.Invoice | None = None, **data: Any) -> None:
        if lnrpc_invoice and isinstance(lnrpc_invoice, lnrpc.Invoice):
            data_dict = MessageToDict(lnrpc_invoice, preserving_proto_field_name=True)
            invoice_dict = convert_datetime_fields(data_dict)
        else:
            invoice_dict = convert_datetime_fields(data)

        super().__init__(**invoice_dict)

        # set the expiry date to the creation date + expiry time
        if self.creation_date:
            self.expiry_date = (
                self.creation_date + timedelta(seconds=self.expiry) if self.expiry else None
            )
        # perform my check to see if this invoice can be paid to Hive
        if self.memo:
            match = re.match(LND_INVOICE_TAG, self.memo.lower())
            if match:
                self.is_lndtohive = True

        self.fill_hive_accname()
        self.fill_custom_records()

    @property
    def collection(self) -> str:
        """
        Returns the collection name for the invoice.

        Returns:
            str: The collection name for the invoice.
        """
        return "invoices"

    @property
    def group_id_query(self) -> dict:
        """
        Returns the query used to identify the group ID in the database.

        Returns:
            dict: The query used to identify the group ID.
        """
        return {"r_hash": self.r_hash}

    def group_id(self) -> str:
        """
        Returns the group ID for the invoice.

        Returns:
            str: The group ID for the invoice.
        """
        return self.r_hash

    @property
    def log_str(self) -> str:
        """
        Returns a string representation of the invoice.

        Returns:
            str: A string representation of the invoice.
        """
        return f"Invoice {self.r_hash[:6]} ({self.value} sats) - {self.memo}"

    def fill_hive_accname(self) -> None:
        """
        Extracts and returns the Hive account name associated with the invoice, if available.

        The method attempts to extract the Hive account name from the `memo` field or the
        `custom_records` field of the first HTLC (Hashed Time-Locked Contract) in the invoice.

        Returns:
            AccNameType | None: The extracted Hive account name as an `AccNameType` object if
            successfully decoded and valid, otherwise `None`.

        Notes:
            - If the `memo` field is present, it is matched against the `LND_INVOICE_TAG` regex
              pattern to extract the account name.
            - If the `memo` field is not present but the `htlcs` field contains custom records,
              the method attempts to decode the base64-encoded value associated with the key "818818".
            - If decoding fails, a warning is logged, and the method returns `None`.
        """
        extracted_value = None
        if self.memo:
            match = re.match(LND_INVOICE_TAG, self.memo.lower())
            if match:
                extracted_value = match.group(1)

        elif self.htlcs and self.htlcs[0] and self.htlcs[0].custom_records:
            if value := self.htlcs[0].custom_records.get("818818", None):
                try:
                    extracted_value = b64_decode(value)
                except Exception as e:
                    logger.warning(f"Error decoding {value}: {e}", extra={"notification": False})

        if extracted_value:
            hive_accname = AccNameType(extracted_value)
            self.hive_accname = hive_accname
            self.is_lndtohive = True

    def fill_custom_records(self) -> None:
        """
        Populates the `custom_record` attribute by decoding and validating a custom record
        from the first HTLC's custom records, if available.

        The method performs the following steps:
        1. Checks if `htlcs` exists and contains at least one entry with `custom_records`.
        2. Attempts to retrieve and decode the custom record with the key "7629169".
        3. Validates the decoded custom record using the `KeysendCustomRecord` model.
        4. Assigns the validated custom record to the `custom_record` attribute.

        If an error occurs during validation, a warning is logged without raising an exception.

        Raises:
            None: All exceptions during validation are caught and logged.

        Logs:
            A warning message if an error occurs during custom record validation.

        Attributes:
            custom_record (KeysendCustomRecord): The validated custom record, if successfully decoded and validated.
        """
        if self.htlcs and self.htlcs[0].custom_records:
            extracted_value = decode_all_custom_records(self.htlcs[0].custom_records)
            self.custom_records = extracted_value

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
        self,
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
            if not self.invoices:
                self.invoices = []


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

    return ListInvoiceResponse.model_validate(message_dict)
