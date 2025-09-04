import re
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, ClassVar, Dict, List, override

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field, computed_field
from pymongo.asynchronous.collection import AsyncCollection

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, LoggerFunction, logger
from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse, currency_to_receive
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta
from v4vapp_backend_v2.models.custom_records import (
    DecodedCustomRecord,
    b64_decode,
    decode_all_custom_records,
)
from v4vapp_backend_v2.models.pydantic_helpers import BSONInt64, convert_datetime_fields
from v4vapp_backend_v2.process.lock_str_class import CustIDType, LockStr

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
    htlc_index: BSONInt64 = BSONInt64(0)
    amt_msat: BSONInt64
    accept_height: int
    accept_time: datetime
    resolve_time: datetime
    expiry_height: int
    state: InvoiceHTLCState
    custom_records: Dict[str, str] | None = None
    mpp_total_amt_msat: BSONInt64 = BSONInt64(0)
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

    **Note**: in order to use a `conv` object you need to call `update_conv` method
    after initializing the object with a `QuoteResponse` or `None`.
    This model extends `TrackedBaseModel` and includes additional fields

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

    memo: str = Field(
        default="",
        description="An optional memo to attach along with the invoice.",
    )
    r_preimage: str = Field(
        default="",
        description=(
            "The hex-encoded preimage (32 byte) which will allow settling an "
            "incoming HTLC payable to this preimage. When using REST, this field "
            "must be encoded as base64."
        ),
    )
    r_hash: str = Field(
        default="",
        description=(
            "The hash of the preimage. When using REST, this field must be encoded as base64. "
            "Note: Output only, don't specify for creating an invoice."
        ),
    )
    value: BSONInt64 = Field(
        default=BSONInt64(0),
        description="The value of this invoice in satoshis The fields value and value_msat are mutually exclusive.",
    )
    value_msat: BSONInt64 = Field(
        default=BSONInt64(0),
        description="The value of this invoice in millisatoshis. The fields value and value_msat are mutually exclusive.",
    )
    settled: bool = Field(
        default=False,
        deprecated=True,
        description="Whether this invoice has been fulfilled. The field is deprecated. Use the state field instead (compare to SETTLED).",
    )
    creation_date: datetime = Field(
        datetime.now(tz=timezone.utc), description="The date this invoice was created."
    )
    settle_date: datetime | None = None
    payment_request: str = ""
    description_hash: str = ""
    expiry: int | None = None
    fallback_addr: str = ""
    cltv_expiry: int | None = None
    route_hints: List[dict] | None = None
    private: bool | None = None
    add_index: BSONInt64 = BSONInt64(0)
    settle_index: BSONInt64 = BSONInt64(0)
    amt_paid: BSONInt64 = Field(
        BSONInt64(0), deprecated=True, description="Deprecated, use amt_paid_sat or amt_paid_msat."
    )
    amt_paid_sat: BSONInt64 = Field(
        BSONInt64(0),
        description=(
            "The amount that was accepted for this invoice, in satoshis. "
            "This will ONLY be set if this invoice has been settled or accepted. "
            "We provide this field as if the invoice was created with a zero value, "
            "then we need to record what amount was ultimately accepted. Additionally, "
            "it's possible that the sender paid MORE that was specified in the original "
            "invoice. So we'll record that here as well. Note: Output only, don't specify "
            "for creating an invoice."
        ),
    )
    amt_paid_msat: BSONInt64 = Field(BSONInt64(0), description="The amount paid in millisatoshis.")
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
    cust_id: CustIDType | None = Field(
        default=None, description="Customer ID associated with the invoice"
    )
    custom_records: DecodedCustomRecord | None = Field(
        default=None, description="Other custom records associated with the invoice"
    )
    expiry_date: datetime | None = Field(
        default=None, description="Expiry date of the invoice (creation_date + expiry)"
    )

    # Dump field names (not aliases) to MongoDB
    dump_by_alias: ClassVar[bool] = False

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

        self.fill_cust_id()
        self.fill_custom_records()

    @override
    async def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion rate for the payment.

        This method retrieves the latest conversion rate and updates the
        `conv` attribute of the payment instance.
        """
        if not quote:
            quote = await TrackedBaseModel.nearest_quote(self.timestamp)
        amount_msat = max(self.amt_paid_msat, self.value_msat)

        self.conv = CryptoConversion(
            conv_from=Currency.MSATS,
            value=float(amount_msat),
            quote=quote,
        ).conversion

    @property
    def collection_name(self) -> str:
        """
        Returns the collection name for the invoice.

        Returns:
            str: The collection name for the invoice.
        """
        return "invoices"

    @classmethod
    def collection(cls) -> AsyncCollection:
        """
        Returns the collection associated with this model.

        Returns:
            AsyncCollection: The collection object for this model.
        """
        return InternalConfig.db["invoices"]

    @property
    def group_id_query(self) -> Dict[str, Any]:
        """
        Returns the query used to identify the group ID in the database.

        Returns:
            dict: The query used to identify the group ID.
        """
        return {"r_hash": self.r_hash}

    @computed_field
    def group_id(self) -> str:
        """
        Returns the group ID for the invoice.

        Returns:
            str: The group ID for the invoice.
        """
        return self.r_hash

    @property
    def group_id_p(self) -> str:
        """
        Returns the group ID for the payment.
        """
        return self.r_hash

    @property
    def short_id(self) -> str:
        """
        Returns a short identifier for the payment, which is the first 10 characters of the payment hash.
        """
        return self.group_id_p[:10]

    @property
    def log_str(self) -> str:
        """
        Returns a string representation of the invoice.

        Returns:
            str: A string representation of the invoice.
        """
        return f"Invoice {self.r_hash[:6]} ({self.value} sats) - {self.memo}"

    @property
    def log_extra(self) -> dict:
        """
        Returns a dictionary containing additional information for logging.

        Returns:
            dict: A dictionary with additional information for logging.
        """
        return {
            "invoice": self.model_dump(exclude_none=True, exclude_unset=True, by_alias=True),
            "group_id": self.r_hash,
            "log_str": self.log_str,
        }

    @property
    def recv_currency(self) -> Currency:
        """
        Returns the currency in which the invoice is received.

        Returns:
            Currency: The currency in which the invoice is received.
        """
        return currency_to_receive(self.memo)

    @property
    def fixed_quote(self) -> FixedHiveQuote | None:
        """
        Returns the fixed quote for the invoice, if available.

        Returns:
            float | None: The fixed quote for the invoice, or None if not set.
        """

        pattern = r"#UUID\s+([a-f0-9]{6})"
        match = re.search(pattern, self.memo)  # Use re.search instead of re.match

        if match:
            unique_id = match.group(1)  # Extract the captured group (the 6-char UUID)
            try:
                quote = FixedHiveQuote.check_quote(
                    unique_id, self.value_msat // 1000
                )  # Pass just the UUID string
                if quote:
                    return quote
            except ValueError as e:
                logger.info(f"Fixed quote expired or not found {unique_id}: {e}")
            except Exception as e:
                logger.warning(f"Error checking fixed quote for {unique_id}: {e}")

        return None

    def fill_cust_id(self) -> None:
        """
        Extracts and returns the customer ID associated with the invoice, if available.

        The method attempts to extract the customer ID from the `memo` field or the
        `custom_records` field of the first HTLC (Hashed Time-Locked Contract) in the invoice.

        Returns:
            AccNameType | None: The extracted customer ID as an `AccNameType` object if
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
            self.cust_id = LockStr(extracted_value)
            if self.cust_id.startswith("@"):
                self.cust_id = self.cust_id[1:]
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

    @property
    def timestamp(self) -> datetime:
        """
        Returns the timestamp of the invoice, which is the creation date.

        Returns:
            datetime: The creation date of the invoice.
        """
        timestamp = self.settle_date or self.creation_date
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp

    @property
    def op_type(self) -> str:
        """
        Returns the operation type for the invoice.

        Returns:
            str: The operation type for the invoice, which is always "invoice".
        """
        return "invoice"

    @property
    def age(self) -> float:
        """
        Returns the age of the invoice as a float representing the total seconds.

        Returns:
            float: The age of the invoice in seconds.
        """
        return (datetime.now(tz=timezone.utc) - self.timestamp).total_seconds()

    @property
    def age_str(self) -> str:
        """
        Returns the age of the invoice as a formatted string.

        Returns:
            str: The age of the invoice in a human-readable format.
        """
        age_text = f" {format_time_delta(self.age)}" if self.age > 120 else ""
        return age_text

    @property
    def d_memo(self) -> str:
        """
        Returns the memo associated with the invoice, or an empty string if no memo is set.
        Provided for consistency with decoded memos in Hive.

        Returns:
            str: The memo text, or an empty string if memo is None or empty.
        """
        return self.memo if self.memo else ""


class ListInvoiceResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    invoices: List[Invoice]
    last_index_offset: BSONInt64
    first_index_offset: BSONInt64

    def __init__(
        self,
        lnrpc_list_invoice_response: lnrpc.ListInvoiceResponse | None = None,
        **data: Any,
    ) -> None:
        if lnrpc_list_invoice_response and isinstance(
            lnrpc_list_invoice_response, lnrpc.ListInvoiceResponse
        ):
            # always_print_fields_with_no_presence=True: forces serialization of fields that lack
            # presence (repeated, maps, scalars) so missing lists become [] instead of absent
            # (solves missing "invoices").
            list_invoice_dict = MessageToDict(
                lnrpc_list_invoice_response,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
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
