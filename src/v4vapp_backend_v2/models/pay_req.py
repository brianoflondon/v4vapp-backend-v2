from datetime import datetime, timedelta
from typing import Any, Dict, List

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field, ValidationError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.models.pydantic_helpers import BSONInt64, convert_datetime_fields


# Reusing the Feature model from the Invoice model
class Feature(BaseModel):
    name: str = ""
    is_required: bool = False
    is_known: bool = False


# Model for HopHint (nested within RouteHint)
class HopHint(BaseModel):
    node_id: str
    chan_id: str
    fee_base_msat: int
    fee_proportional_millionths: int
    cltv_expiry_delta: int

    model_config = ConfigDict(arbitrary_types_allowed=True)


# Model for RouteHint
class RouteHint(BaseModel):
    hop_hints: List[HopHint] = []

    model_config = ConfigDict(arbitrary_types_allowed=True)


# Simplified model for BlindedHop (nested within BlindedPath)
class BlindedHop(BaseModel):
    blinded_node: str  # Base64-encoded bytes
    encrypted_data: str  # Base64-encoded bytes

    model_config = ConfigDict(arbitrary_types_allowed=True)


# Simplified model for BlindedPath (nested within BlindedPaymentPath)
class BlindedPath(BaseModel):
    introduction_node: str  # Base64-encoded bytes
    blinding_point: str  # Base64-encoded bytes
    blinded_hops: List[BlindedHop] = []

    model_config = ConfigDict(arbitrary_types_allowed=True)


# Model for BlindedPaymentPath
class BlindedPaymentPath(BaseModel):
    blinded_path: BlindedPath
    base_fee_msat: BSONInt64
    proportional_fee_rate: int
    total_cltv_delta: int
    htlc_min_msat: BSONInt64
    htlc_max_msat: BSONInt64
    features: List[int] = []  # List of FeatureBit enum values

    model_config = ConfigDict(arbitrary_types_allowed=True)


# Main PayReq model
class PayReq(BaseModel):
    """
    Pydantic model representing a decoded Lightning Network payment request (lnrpc.PayReq).

    This model is based on the lnrpc.PayReq message from lightning.proto and is designed to
    handle the decoded data from a payment request string. It follows the style of the Invoice
    model, reusing components like Feature, BSONInt64, and convert_datetime_fields.

    Attributes:
        destination (str): The public key of the destination node.
        payment_hash (str): The hash of the payment preimage.
        value (BSONInt64 | None): The value of the payment request in satoshis.
        value_msat (BSONInt64 | None): The value of the payment request in millisatoshis.
        timestamp (datetime | None): The creation timestamp of the payment request.
        expiry (int | None): The expiry time of the payment request in seconds.
        expiry_date (datetime | None): The calculated expiry date (timestamp + expiry).
        memo (str): A description or memo for the payment request.
        description_hash (str | None): The hash of the description, if available.
        fallback_addr (str | None): A fallback on-chain address, if available.
        cltv_expiry (int | None): The CLTV expiry value for the payment.
        route_hints (List[RouteHint] | None): Route hints for reaching the destination.
        payment_addr (str | None): The payment address (base64-encoded bytes).
        features (Dict[str, Feature] | None): Features supported or required by the payment request.
        blinded_paths (List[BlindedPaymentPath] | None): Blinded payment paths, if available.

    Methods:
        __init__(lnrpc_payreq: lnrpc.PayReq = None, **data: Any) -> None:
            Initializes the PayReq object, converting protobuf data and calculating derived fields.
    """

    destination: str = ""
    payment_hash: str = ""
    value: BSONInt64 = Field(BSONInt64(0), alias="num_satoshis")
    value_msat: BSONInt64 = Field(BSONInt64(0), alias="num_msat")
    timestamp: datetime | None = None
    expiry: int | None = None
    expiry_date: datetime | None = Field(
        default=None, description="Expiry date of the payment request (timestamp + expiry)"
    )
    memo: str = Field("", alias="description")
    description_hash: str | None = None
    fallback_addr: str | None = None
    cltv_expiry: int | None = None
    route_hints: List[RouteHint] | None = None
    payment_addr: str | None = None
    features: Dict[str, Feature] | None = None
    blinded_paths: List[BlindedPaymentPath] | None = None

    conv: CryptoConv = Field(
        default_factory=CryptoConv,
        description="Conversion data for the payment request",
    )
    pay_req_str: str = Field(
        default="",
        description="Original payment request string",
    )
    dest_alias: str = Field(
        default="",
        description="Alias of the destination node, set outside the class",
    )

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,  # Allow initialization with aliased field names
        extra="allow",  # Allow extra fields to handle potential mismatches
    )

    def __init__(self, lnrpc_payreq: lnrpc.PayReq | None = None, **data: Any) -> None:
        if lnrpc_payreq and isinstance(lnrpc_payreq, lnrpc.PayReq):
            data_dict = MessageToDict(lnrpc_payreq, preserving_proto_field_name=True)
            invoice_dict = convert_datetime_fields(data_dict)
        else:
            invoice_dict = convert_datetime_fields(data)

        # Convert features to Feature models
        if "features" in invoice_dict:
            invoice_dict["features"] = {
                str(k): Feature(**v) for k, v in invoice_dict["features"].items()
            }

        # Convert route_hints to RouteHint models
        if "route_hints" in invoice_dict:
            invoice_dict["route_hints"] = [
                RouteHint(hop_hints=[HopHint(**hint) for hint in route.get("hop_hints", [])])
                for route in invoice_dict["route_hints"]
            ]

        # Convert blinded_paths to BlindedPaymentPath models
        if "blinded_paths" in invoice_dict:
            invoice_dict["blinded_paths"] = [
                BlindedPaymentPath(
                    blinded_path=BlindedPath(
                        introduction_node=path.get("blinded_path", {}).get(
                            "introduction_node", ""
                        ),
                        blinding_point=path.get("blinded_path", {}).get("blinding_point", ""),
                        blinded_hops=[
                            BlindedHop(**hop)
                            for hop in path.get("blinded_path", {}).get("blinded_hops", [])
                        ],
                    ),
                    base_fee_msat=path.get("base_fee_msat", 0),
                    proportional_fee_rate=path.get("proportional_fee_rate", 0),
                    total_cltv_delta=path.get("total_cltv_delta", 0),
                    htlc_min_msat=path.get("htlc_min_msat", 0),
                    htlc_max_msat=path.get("htlc_max_msat", 0),
                    features=path.get("features", []),
                )
                for path in invoice_dict["blinded_paths"]
            ]

        try:
            super().__init__(**invoice_dict)
            if self.value == 0 and self.value_msat > 0:
                self.value = BSONInt64(self.value_msat // 1000)
            elif self.value_msat == 0 and self.value > 0:
                self.value_msat = BSONInt64(self.value * 1000)

        except ValidationError as e:
            logger.error(f"Validation error in PayReq: {e}", extra={"invoice_dict": invoice_dict})
            raise

        # Calculate expiry_date
        self.expiry_date = (
            self.timestamp + timedelta(seconds=self.expiry)
            if self.timestamp and self.expiry
            else None
        )

    @property
    def collection(self) -> str:
        """
        Returns the collection name for the payment request.

        Returns:
            str: The collection name.
        """
        return "pay_requests"

    @property
    def group_id_query(self) -> dict:
        """
        Returns the query used to identify the group ID in the database.

        Returns:
            dict: The query based on payment_hash.
        """
        return {"payment_hash": self.payment_hash}

    @property
    def is_zero_value(self) -> bool:
        """
        Checks if the payment request is a zero-value request.

        Returns:
            bool: True if the payment request is zero-value, False otherwise.
        """
        return self.value == 0 and self.value_msat == 0

    @property
    def is_expired(self) -> bool:
        """
        Checks if the payment request has expired.

        Returns:
            bool: True if the payment request is expired, False otherwise.
        """
        return self.expiry_date is not None and datetime.now() > self.expiry_date + timedelta(
            seconds=0
        )

    @property
    def amount_msat(self) -> int:
        """
        Returns the amount in millisatoshis.

        Returns:
            int: The amount in millisatoshis.
        """
        return self.value_msat if self.value_msat > 0 else self.value * 1000

    @property
    def log_str(self) -> str:
        """
        Returns a string representation of the payment request.

        Returns:
            str: The string representation of the payment request.
        """
        return f"PayReq(destination={self.dest_alias or self.destination}, payment_hash={self.payment_hash}, value={self.value}, value_msat={self.value_msat}, expiry_date={self.expiry_date}, memo={self.memo})"

    @property
    def name(self) -> str:
        return "pay_req"

    @property
    def log_extra(self) -> dict:
        """
        Returns a dictionary containing additional information for logging.

        Returns:
            dict: A dictionary with additional information for logging.
        """
        return {
            self.name: self.model_dump(exclude_none=True, exclude_unset=True, by_alias=True),
            "log_str": self.log_str,
        }


def protobuf_pay_req_to_pydantic(pay_req: lnrpc.PayReq, pay_req_str: str) -> PayReq:
    """
    Converts a protobuf PayReq object to a Pydantic PayReq model.
    Also passes along the original pay_req_str for later use.

    Args:
        pay_req (lnrpc.PayReq): The protobuf PayReq object to be converted.

    Returns:
        PayReq: The converted Pydantic PayReq model. If an error occurs during validation,
                 an empty PayReq model is returned.
    """
    pay_req_dict = MessageToDict(pay_req, preserving_proto_field_name=True)
    pay_req_dict = convert_datetime_fields(pay_req_dict)
    try:
        pay_req_model = PayReq.model_validate(pay_req_dict)
        pay_req_model.pay_req_str = pay_req_str
        return pay_req_model
    except Exception as e:
        logger.error(
            f"Error converting PayReq to Pydantic model: {e}",
            extra={
                "notification": False,
                "pay_req_dict": pay_req_dict,
                "pay_req_str": pay_req_str,
            },
        )
        raise e
