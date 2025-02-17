from typing import Any, List, Optional, Tuple
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from google.protobuf.json_format import MessageToDict
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import LoggerFunction
from v4vapp_backend_v2.models.pydantic_helpers import (
    BSONInt64,
    convert_datetime_fields,
)


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


class Payment(BaseModel):
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
        destination_alias (str | None): The alias of the payment destination (needs to be looked up not sent by LND)

    Methods:
        __init__(lnrpc_payment: lnrpc.Payment = None, **data: Any) -> None:
            Initializes a Payment instance with data from an lnrpc.Payment object or provided data.

        destination_pub_keys() -> List[str]:
            Returns the public keys of the payment hops
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    payment_hash: str | None = None
    value: Optional[BSONInt64] = None
    creation_date: datetime | None = None
    fee: Optional[BSONInt64] = None
    payment_preimage: str
    value_sat: BSONInt64 | None = None
    value_msat: BSONInt64 | None = None
    payment_request: str | None = None
    status: str
    fee_sat: BSONInt64 | None = None
    fee_msat: BSONInt64 | None = None
    creation_time_ns: datetime
    payment_index: BSONInt64
    failure_reason: str | None = None
    htlcs: List[HTLCAttempt] | None = None
    destination_alias: str | None = None

    def __init__(
        __pydantic_self__, lnrpc_payment: lnrpc.Payment = None, **data: Any
    ) -> None:
        if lnrpc_payment and isinstance(lnrpc_payment, lnrpc.Payment):
            data_dict = MessageToDict(lnrpc_payment, preserving_proto_field_name=True)
            payment_dict = convert_datetime_fields(data_dict)
        else:
            payment_dict = convert_datetime_fields(data)
        super().__init__(**payment_dict)

    @property
    def get_succeeded_htlc(self) -> HTLCAttempt | None:
        """
        Retrieves the HTLC attempt with status 'SUCCEEDED'.

        Returns:
            Optional[HTLCAttempt]: The HTLC attempt with status 'SUCCEEDED', or None if not found.
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
            Tuple[str, str]: A tuple containing the public key of the last hop and the second to last hop in the route.
                             If there is only one hop, the second element of the tuple will be an empty string.
                             If there are no hops, an empty string is returned.
        """
        ans = []
        htlc = self.get_succeeded_htlc
        if htlc:
            for pub_key in htlc.route.hops:
                ans.append(pub_key.pub_key)
        return ans


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
                Payment.model_validate(payment)
                for payment in list_payments_dict["payments"]
            ]
            super().__init__(**list_payments_dict)
        else:
            super().__init__(**data)
            if not __pydantic_self__.payments:
                __pydantic_self__.payments = []
