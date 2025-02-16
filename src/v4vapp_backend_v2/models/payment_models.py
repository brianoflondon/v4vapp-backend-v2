from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from google.protobuf.json_format import MessageToDict
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import LoggerFunction
from v4vapp_backend_v2.models.protobuf_pydantic_conversion_models import (
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

    def __init__(
        __pydantic_self__, lnrpc_payment: lnrpc.Payment = None, **data: Any
    ) -> None:
        if lnrpc_payment and isinstance(lnrpc_payment, lnrpc.Payment):
            data_dict = MessageToDict(lnrpc_payment, preserving_proto_field_name=True)
            payment_dict = convert_datetime_fields(data_dict)
        else:
            payment_dict = convert_datetime_fields(data)
        super().__init__(**payment_dict)


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
