from datetime import datetime
from enum import StrEnum
from typing import Any, List, Optional

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, computed_field

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
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


class Route(BaseModel):
    total_time_lock: int
    total_fees: BSONInt64 | None = None
    total_amt: BSONInt64
    hops: List[Hop]
    total_fees_msat: BSONInt64 | None = None
    total_amt_msat: BSONInt64


class HTLCAttempt(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    attempt_id: BSONInt64
    status: str | None = None
    attempt_time_ns: BSONInt64
    resolve_time_ns: Optional[BSONInt64] = None
    preimage: Optional[str] = None
    route: Optional[Route] = None
    failure: Optional[dict] = None


class NodeAlias(BaseModel):
    pub_key: str
    alias: str


class PaymentExtra(TrackedBaseModel):
    route: list[NodeAlias] | None = []

    @computed_field
    def destination(self) -> str:
        """
        Determines the destination based on the route.
        Returns:
            str: The alias of the destination. If the route is empty, returns "Unknown".
                If the route has only one element, returns the alias of that element.
                If the last element in the route has an alias of "Unknown",
                    checks the second to last element:
                    - If the alias is "magnetron", returns "Muun User".
                    - If the alias is "ACINQ", returns "Phoenix User".
                Otherwise, returns the alias of the last element in the route.
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

    @computed_field
    def route_str(self) -> str:
        """
        Returns a string representation of the route with fees in ppm

        Returns:
            str: A string representation of the route.
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


class Payment(PaymentExtra):
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

    Methods:
        __init__(lnrpc_payment: lnrpc.Payment = None, **data: Any) -> None:
            Initializes a Payment instance with data from an lnrpc.Payment
                object or provided data.

        destination_pub_keys() -> List[str]:
            Returns the public keys of the payment hops
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    payment_hash: str | None = None
    value: Optional[BSONInt64] = None
    creation_date: datetime | None = None
    fee: Optional[BSONInt64] = None
    payment_preimage: str | None = None
    value_sat: BSONInt64 | None = None
    value_msat: BSONInt64 | None = None
    payment_request: str | None = None
    status: PaymentStatus | None = None
    fee_sat: BSONInt64 | None = None
    fee_msat: BSONInt64 | None = None
    creation_time_ns: datetime | None = None
    payment_index: BSONInt64 | None = None
    failure_reason: str | None = None
    htlcs: List[HTLCAttempt] | None = None

    def __init__(self, lnrpc_payment: lnrpc.Payment | None = None, **data: Any) -> None:
        if lnrpc_payment and isinstance(lnrpc_payment, lnrpc.Payment):
            data_dict = MessageToDict(lnrpc_payment, preserving_proto_field_name=True)
            payment_dict = convert_datetime_fields(data_dict)
        else:
            payment_dict = convert_datetime_fields(data)
        super().__init__(**payment_dict)

    @property
    def collection(self) -> str:
        """
        Returns the collection name for the invoice.

        Returns:
            str: The collection name for the invoice.
        """
        return "payments"

    @property
    def group_id_query(self) -> dict:
        """
        Returns the query used to identify the group ID in the database.

        Returns:
            dict: The query used to identify the group ID.
        """
        return {"payment_hash": self.payment_hash}

    @property
    def get_succeeded_htlc(self) -> HTLCAttempt | None:
        """
        Retrieves the HTLC attempt with status 'SUCCEEDED'.

        Returns:
            Optional[HTLCAttempt]: The HTLC attempt with status 'SUCCEEDED', or None
            if not found.
        """
        if not self.htlcs:
            return None
        for htlc in self.htlcs:
            if htlc.status == "SUCCEEDED":
                return htlc
        return None

    @property
    def destination_pub_keys(self) -> List[str | None]:
        """
        Retrieves the public keys of the destination hops in the HTLC route.

        Returns:
            Tuple[str, str]: A tuple containing the public key of the last hop and
            the second to last hop in the route. If there is only one hop, the second
            element of the tuple will be an empty string. If there are no hops,
            an empty string is returned.
        """
        ans = []
        htlc = self.get_succeeded_htlc
        if htlc:
            for pub_key in htlc.route.hops:
                ans.append(pub_key.pub_key)
        return ans

    @property
    def route_fees_ppm(self) -> dict[str, float]:
        """
        Calculates the fee in parts per million (ppm) for each hop in the route.

        Returns:
            dict[str, float]: A dict of fee ppm values for each hop.
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
    def fee_ppm(self) -> int:
        """
        Calculates the fee in parts per million (ppm) for the payment.

        Returns:
            float: The fee in ppm.
        """
        if self.fee_msat and self.value_msat:
            return int((self.fee_msat / self.value_msat) * 1_000_000)
        return 0


class ListPaymentsResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    payments: List[Payment]
    first_index_offset: BSONInt64
    last_index_offset: BSONInt64
    total_num_payments: Optional[BSONInt64] = None

    def __init__(
        __pydantic_self__,
        lnrpc_list_payments_response: lnrpc.ListPaymentsResponse = None,
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
            if not __pydantic_self__.payments:
                __pydantic_self__.payments = []
