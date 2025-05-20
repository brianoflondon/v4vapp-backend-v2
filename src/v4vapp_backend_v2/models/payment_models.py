from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Optional, override

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field, computed_field

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
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
    attempt_time_ns: BSONInt64
    resolve_time_ns: Optional[BSONInt64] = None
    preimage: Optional[str] = None
    route: Optional[Route] = None
    failure: Optional[dict] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class NodeAlias(BaseModel):
    pub_key: str
    alias: str


class FirstHopCustomRecords(BaseModel):
    key: BSONInt64
    value: str | None = None


class Payment(TrackedBaseModel):
    """
    Payment model representing a payment transaction.

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
    creation_date: datetime | None = None
    fee: BSONInt64 = BSONInt64(0)
    payment_preimage: str = ""
    value_sat: BSONInt64 = BSONInt64(0)
    value_msat: BSONInt64 = BSONInt64(0)
    payment_request: str = ""
    status: PaymentStatus | None = None
    fee_sat: BSONInt64 = BSONInt64(0)
    fee_msat: BSONInt64 = BSONInt64(0)
    creation_time_ns: datetime | None = None
    payment_index: BSONInt64 = BSONInt64(0)
    failure_reason: str = ""
    htlcs: List[HTLCAttempt] | None = None
    first_hop_custom_record: List[FirstHopCustomRecords] | None = None

    custom_records: DecodedCustomRecord | None = Field(
        default=None, description="Other custom records associated with the invoice"
    )

    def __init__(self, lnrpc_payment: lnrpc.Payment | None = None, **data: Any) -> None:
        if lnrpc_payment and isinstance(lnrpc_payment, lnrpc.Payment):
            data_dict = MessageToDict(lnrpc_payment, preserving_proto_field_name=True)
            payment_dict = convert_datetime_fields(data_dict)
        else:
            payment_dict = convert_datetime_fields(data)
        super().__init__(**payment_dict)
        if not self.conv:
            self.update_conv()
        self.fill_custom_record()

    @override
    def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion rate for the payment.

        This method retrieves the latest conversion rate and updates the
        `conv` attribute of the payment instance.
        """
        quote = quote or self.last_quote
        self.conv = CryptoConversion(
            conv_from=Currency.MSATS,
            value=float(self.value_msat),
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
                    return

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
    def collection(self) -> str:
        return "payments"

    @property
    def group_id_query(self) -> dict:
        return {"payment_hash": self.payment_hash}

    @property
    def group_id(self) -> str:
        """
        Returns the group ID for the payment.
        """
        return self.payment_hash

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
            list_payments_dict = MessageToDict(
                lnrpc_list_payments_response, preserving_proto_field_name=True
            )
            list_payments_dict["invoices"] = [
                Payment.model_validate(payment) for payment in list_payments_dict["payments"]
            ]
            super().__init__(**list_payments_dict)
        else:
            super().__init__(**data)
            if not self.payments:
                self.payments = []
