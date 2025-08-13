from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Dict, List, Optional, override

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field, computed_field
from pymongo.asynchronous.collection import AsyncCollection

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.process.cust_id_class import CustIDType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta
from v4vapp_backend_v2.models.custom_records import DecodedCustomRecord, decode_all_custom_records
from v4vapp_backend_v2.models.pydantic_helpers import BSONInt64, convert_datetime_fields


class PaymentStatus(StrEnum):
    """
    Enum representing the status of a payment.
    """

    UNKNOWN = "UNKNOWN"
    IN_FLIGHT = "IN_FLIGHT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    INITIATED = "INITIATED"


class Hop(BaseModel):
    chan_id: str
    chan_capacity: BSONInt64 | None = None
    amt_to_forward: BSONInt64 | None = None
    fee: BSONInt64 | None = None
    expiry: int
    amt_to_forward_msat: BSONInt64
    fee_msat: BSONInt64 | None = None
    pub_key: str
    tlv_payload: bool | None = None
    metadata: Optional[bytes] = None
    blinding_point: Optional[bytes] = None
    encrypted_data: Optional[bytes] = None
    total_amt_msat: Optional[BSONInt64] = None
    custom_records: Dict[str, str] | None = None


class Route(BaseModel):
    total_time_lock: int
    total_fees: BSONInt64 | None = None
    total_amt: BSONInt64
    hops: List[Hop]
    total_fees_msat: BSONInt64 | None = None
    total_amt_msat: BSONInt64


class HTLCAttempt(BaseModel):
    attempt_id: BSONInt64
    status: str | None = None
    attempt_time_ns: datetime | None = None
    resolve_time_ns: datetime | None = None
    preimage: Optional[str] = None
    route: Optional[Route] = None
    failure: Optional[dict] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data: Any) -> None:
        htlc_attempt_data = convert_datetime_fields(data)
        super().__init__(**htlc_attempt_data)


class NodeAlias(BaseModel):
    pub_key: str
    alias: str


class FirstHopCustomRecords(BaseModel):
    key: BSONInt64
    value: str | None = None


class Payment(TrackedBaseModel):
    """
    Payment model representing a payment transaction.

    **Note**: in order to use a `conv` object you need to call `update_conv` method
    after initializing the object with a `QuoteResponse` or `None`.
    This model extends `TrackedBaseModel` and includes additional fields

    Attributes:
        payment_hash (str | None): The hash of the payment.
        value (Optional[BSONInt64]): The value of the payment.
        creation_date (datetime | None): The creation date of the payment.
        fee (Optional[BSONInt64]): The fee associated with the payment.
        payment_preimage (str): The preimage of the payment.
        value_sat (BSONInt64 | None): The value of the payment in satoshis.
        value_msat (BSONInt64 | None): The value of the payment in millisatoshis.
        payment_request (str | None): The payment request string.
        status (str): The status of the payment.
        fee_sat (BSONInt64 | None): The fee of the payment in satoshis.
        fee_msat (BSONInt64 | None): The fee of the payment in millisatoshis.
        creation_time_ns (datetime): The creation time of the payment in nanoseconds.
        payment_index (BSONInt64): The index of the payment.
        failure_reason (str | None): The reason for payment failure, if any.
        htlcs (List[HTLCAttempt] | None): The HTLC attempts associated with the payment.
        destination_alias (str | None): The alias of the payment destination
            (needs to be looked up not sent by LND)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Attributes from PaymentExtra
    route: list[NodeAlias] | None = []

    # Attributes from Payment
    payment_hash: str = ""
    value: BSONInt64 = BSONInt64(0)
    creation_date: datetime = datetime.now(tz=timezone.utc)
    fee: BSONInt64 = BSONInt64(0)
    payment_preimage: str = ""
    value_sat: BSONInt64 = BSONInt64(0)
    value_msat: BSONInt64 = BSONInt64(0)
    payment_request: str = ""
    status: PaymentStatus | None = None
    fee_sat: BSONInt64 = BSONInt64(0)
    fee_msat: BSONInt64 = BSONInt64(0)
    creation_time_ns: datetime | None = None  # This is a datetime object, not an int
    payment_index: BSONInt64 = BSONInt64(0)
    failure_reason: str = ""
    htlcs: List[HTLCAttempt] | None = None
    first_hop_custom_record: List[FirstHopCustomRecords] | None = None

    conv_fee: CryptoConv | None = Field(
        default=None, description="Conversion of the fee for this payment"
    )
    custom_records: DecodedCustomRecord | None = Field(
        default=None, description="Other custom records associated with the invoice"
    )

    # Additional fields, not in the LND invoice (but calculated at ingestion time)
    cust_id: CustIDType | None = Field(
        default=None, description="Customer ID associated with the invoice"
    )

    def __init__(self, lnrpc_payment: lnrpc.Payment | None = None, **data: Any) -> None:
        if lnrpc_payment and isinstance(lnrpc_payment, lnrpc.Payment):
            data_dict = MessageToDict(lnrpc_payment, preserving_proto_field_name=True)
            payment_dict = convert_datetime_fields(data_dict)
        else:
            payment_dict = convert_datetime_fields(data)
        super().__init__(**payment_dict)
        self.fill_custom_record()

    @override
    async def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion rate for the payment.
        Includes the fee in the value for conversion.
        Also sets fee_conv if fee_msat is present

        This method retrieves the latest conversion rate and updates the
        `conv` attribute of the payment instance.
        """
        if not quote:
            quote = await TrackedBaseModel.nearest_quote(self.timestamp)
        if self.fee_msat:
            self.fee_conv = CryptoConversion(
                conv_from=Currency.MSATS,
                value=float(self.fee_msat),
                quote=quote,
            ).conversion
        self.conv = CryptoConversion(
            conv_from=Currency.MSATS,
            value=float(self.value_msat + self.fee_msat),
            quote=quote,
        ).conversion

    def fill_custom_record(self) -> None:
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

        if self.htlcs and self.htlcs[0].route and self.htlcs[0].route.hops:
            for hop in self.htlcs[0].route.hops:
                if custom_records := hop.custom_records:
                    self.custom_records = decode_all_custom_records(custom_records=custom_records)
                    self.cust_id = getattr(self.custom_records, "cust_id", "")

    # Methods from PaymentExtra
    @computed_field
    def destination(self) -> str:
        """
        Determines the destination based on the route.
        """
        if not self.route:
            return "Unknown"
        if len(self.route) == 1:
            return self.route[0].alias
        if self.route[-1].alias == "Unknown":
            if self.route[-2].alias == "magnetron":
                return "Muun User"
            elif self.route[-2].alias == "ACINQ":
                return "Phoenix User"
        return self.route[-1].alias

    @property
    def succeeded(self) -> bool:
        """
        Checks if the payment has succeeded.
        """
        return self.status == PaymentStatus.SUCCEEDED

    @property
    def failed(self) -> bool:
        """
        Checks if the payment has failed.
        """
        return self.status == PaymentStatus.FAILED

    @property
    def in_flight(self) -> bool:
        """
        Checks if the payment is currently in flight.
        """
        return self.status == PaymentStatus.IN_FLIGHT

    @property
    def get_succeeded_htlc(self) -> HTLCAttempt | None:
        """
        Retrieves the HTLC attempt with status 'SUCCEEDED'.
        """
        if not self.htlcs:
            return None
        for htlc in self.htlcs:
            if htlc.status == "SUCCEEDED":
                return htlc
        return None

    @property
    def route_fees_ppm(self) -> dict[str, float]:
        """
        Calculates the fee in parts per million (ppm) for each hop in the route.
        """
        fee_ppm_dict: dict[str, float] = {}
        htlc = self.get_succeeded_htlc
        if htlc and htlc.route:
            for hop in htlc.route.hops:
                if hop.fee_msat and hop.amt_to_forward_msat:
                    fee_ppm = (hop.fee_msat / hop.amt_to_forward_msat) * 1_000_000
                    fee_ppm_dict[hop.pub_key] = fee_ppm
        return fee_ppm_dict

    @computed_field
    def route_str(self) -> str:
        """
        Returns a string representation of the route with fees in ppm.
        """
        if not self.route:
            return "Unknown"

        route_fees_ppm = self.route_fees_ppm
        ans = " -> ".join(
            [
                (
                    f"{hop.alias}"
                    if route_fees_ppm.get(hop.pub_key) is None
                    else f"{hop.alias} ({route_fees_ppm.get(hop.pub_key):.0f} ppm)"
                )
                for hop in self.route
            ]
        )
        return ans

    # Methods from Payment
    @property
    def collection_name(self) -> str:
        """
        Returns the name of the collection used for storing payment records.

        Returns:
            str: The name of the collection ("payments").
        """
        return "payments"

    @classmethod
    def collection(cls) -> AsyncCollection:
        """
        Returns the collection associated with this model.

        Returns:
            AsyncCollection: The collection object for this model.
        """
        return InternalConfig.db["payments"]

    @property
    def group_id_query(self) -> Dict[str, str]:
        return {"payment_hash": self.payment_hash}

    @computed_field
    def group_id(self) -> str:
        """
        Returns the group ID for the payment.
        """
        return self.payment_hash

    @property
    def group_id_p(self) -> str:
        """
        Returns the group ID for the payment.
        """
        return self.payment_hash

    @property
    def short_id(self) -> str:
        """
        Returns a short identifier for the payment, which is the first 10 characters of the payment hash.
        """
        return self.group_id_p[:10]

    @property
    def destination_pub_keys(self) -> List[str | None]:
        """
        Retrieves the public keys of the destination hops in the HTLC route.
        """
        ans = []
        htlc = self.get_succeeded_htlc
        if htlc and htlc.route:
            for pub_key in htlc.route.hops:
                ans.append(pub_key.pub_key)
        return ans

    @computed_field
    def fee_ppm(self) -> int:
        """
        Calculates the fee in parts per million (ppm) for the payment.
        """
        if self.fee_msat and self.value_msat:
            return int((self.fee_msat / self.value_msat) * 1_000_000)
        return 0

    @property
    def log_str(self) -> str:
        """
        Returns a string representation of the payment log.
        """
        return f"Payment {self.payment_hash[:6]} ({self.status}) - {self.value_sat} sat - {self.fee_sat} sat fee - {self.creation_date}"

    @property
    def log_extra(self) -> dict[str, Any]:
        """
        Returns a dictionary containing additional information for logging.

        Returns:
            dict: A dictionary with additional information for logging.
        """
        return {
            "payment": self.model_dump(exclude_none=True, exclude_unset=True, by_alias=True),
            "group_id": self.payment_hash,
            "log_str": self.log_str,
        }

    # Properties which are not
    @property
    def timestamp(self) -> datetime:
        """
        Returns the timestamp of the invoice, which is the creation date.

        Returns:
            datetime: The creation date of the invoice.
        """
        timestamp = self.creation_time_ns or self.creation_date
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp

    @property
    def op_type(self) -> str:
        """
        Returns the operation type for the payment.

        Returns:
            str: The operation type for the payment, which is always "payment".
        """
        return "payment"

    @property
    def age(self) -> float:
        """
        Returns the age of the payment as a float representing the total seconds.

        Returns:
            float: The age of the invoice in seconds.
        """
        return (datetime.now(tz=timezone.utc) - self.timestamp).total_seconds()

    @property
    def age_str(self) -> str:
        """
        Returns the age of the payment as a formatted string.

        Returns:
            str: The age of the payment in a human-readable format.
        """
        age_text = f" {format_time_delta(self.age)}" if self.age > 120 else ""
        return age_text


class ListPaymentsResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    payments: List[Payment]
    first_index_offset: BSONInt64
    last_index_offset: BSONInt64
    total_num_payments: BSONInt64 = BSONInt64(0)

    def __init__(
        self,
        lnrpc_list_payments_response: lnrpc.ListPaymentsResponse | None = None,
        **data: Any,
    ) -> None:
        if lnrpc_list_payments_response and isinstance(
            lnrpc_list_payments_response, lnrpc.ListPaymentsResponse
        ):
            # always_print_fields_with_no_presence=True: forces serialization of fields that lack
            # presence (repeated, maps, scalars) so missing lists become [] instead of absent
            # (solves missing "invoices").
            list_payments_dict = MessageToDict(
                lnrpc_list_payments_response, preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )
            list_payments_dict["payments"] = [
                Payment.model_validate(payment) for payment in list_payments_dict["payments"]
            ]
            super().__init__(**list_payments_dict)
        else:
            super().__init__(**data)
            if not self.payments:
                self.payments = []
