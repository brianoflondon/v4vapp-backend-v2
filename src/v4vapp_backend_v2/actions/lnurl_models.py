import math
from enum import Enum
from typing import Annotated, Any, List, Literal, Optional, Self, Set, Tuple, Union

from bech32 import bech32_decode, bech32_encode, convertbits
from pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    WrapValidator,
    field_validator,
    model_validator,
)

LNURL_WELL_KNOWN_REGEX = R"http.*:\/\/.*\/.well-known\/lnurlp/.*"

MilliSatoshi = Annotated[int, Field(gt=0)]
Max144Str = Annotated[str, Field(max_length=144)]
InitializationVector = Annotated[str, Field(min_length=24, max_length=24)]


class LnurlPaySuccessAction(BaseModel):
    pass


class MessageAction(LnurlPaySuccessAction):
    tag: Literal["message"] = "message"
    message: Max144Str


class UrlAction(LnurlPaySuccessAction):
    tag: Literal["url"] = "url"
    url: AnyUrl
    description: Max144Str


class AesAction(LnurlPaySuccessAction):
    tag: Literal["aes"] = "aes"
    description: Max144Str
    ciphertext: str  # TODO
    iv: InitializationVector


class LnurlPayRouteHop(BaseModel):
    node_id: str = Field(..., alias="nodeId")
    channel_update: str = Field(..., alias="channelUpdate")


def lnurl_bech32_decode(
    bech32: str, *, allowed_hrp: Optional[Set[str]] = None
) -> Tuple[str, List[int]]:
    """
    Decode a Bech32-encoded string into its Human Readable Part (HRP) and data.

    Args:
        bech32 (str): The Bech32-encoded string to decode.
        allowed_hrp (Optional[Set[str]]): A set of allowed Human Readable Prefixes (HRPs).
            If provided, the decoded HRP must be in this set. Defaults to None.

    Returns:
        Tuple[str, List[int]]: A tuple containing the decoded HRP and data.

    Raises:
        ValueError: If the data or HRP is invalid, or if the decoded HRP is not in the allowed set.
    """
    hrp, data = bech32_decode(bech32)

    if not hrp or not data or (allowed_hrp and hrp not in allowed_hrp):
        raise ValueError(f"Invalid data or Human Readable Prefix (HRP): {hrp}.")

    return hrp, data


def bech32_validate(v: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> str:
    # TODO this probably should be a After Validator not this more complex wrapper
    try:
        assert isinstance(v, str), f"Expected a string, got {type(v).__name__}"
        hrp, data = lnurl_bech32_decode(v)
    except ValueError:
        assert False, f"Invalid Bech32 string: {v}"
    return v


# region Lnurl types
Bech32 = Annotated[str, WrapValidator(bech32_validate)]
LightningInvoice = Annotated[str, WrapValidator(bech32_validate)]
Lnurl = Annotated[AnyUrl, Field(description="The decoded URL.")]


def strip_lightning(input: str) -> str:
    """Removes lightning: from the start of a string if it is there"""
    input = input.strip("⚡️").lower()
    return input[10:] if input.startswith("lightning:") else input


class InvalidLnurl(Exception):
    pass


class InvalidUrl(Exception):
    pass


def lnurl_decode(lnurl: str) -> str:
    """
    Decode a LNURL and return a url string without performing any validation on it.
    Use `lnurl.decode()` for validation and to get `Url` object.
    """
    _, data = lnurl_bech32_decode(strip_lightning(lnurl), allowed_hrp={"lnurl"})

    try:
        bech32_data = convertbits(data, 5, 8, False)
        assert bech32_data
        url = bytes(bech32_data).decode("utf-8")
        return url
    except UnicodeDecodeError:
        raise InvalidLnurl


def lnurl_encode(url: str) -> str:
    """
    Encode a URL without validating it first and return a bech32 LNURL string.
    Use `lnurl.encode()` for validation and to get a `Lnurl` object.
    """
    try:
        bech32_data = convertbits(url.encode("utf-8"), 8, 5, True)
        assert bech32_data
        lnurl = bech32_encode("lnurl", bech32_data)
    except UnicodeEncodeError:
        raise InvalidUrl

    return lnurl.upper()


# region Lnurl models
class LnurlProxyData(BaseModel):
    lightning_address: str | None = Field(
        "",
        description="Lightning address (looks like email address",
        alias="LightningAddress",
    )
    bech32_lnurl: str | None = Field(
        "", description="LNURL encoded as BECH32 string", alias="bech32Lnurl"
    )
    decoded_url: str | None = Field(
        default="",
        pattern=LNURL_WELL_KNOWN_REGEX,
        alias="decodedUrl",
        description="Decoded URL for a lnurlp lookup",
    )
    anything: Union[EmailStr, str] | None = Field("", description="Will try to figure it out")

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)


class LnurlResponseModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    def dict(self, **kwargs):
        kwargs.setdefault("by_alias", True)
        return super().model_validate(**kwargs)

    def json(self, **kwargs):
        kwargs.setdefault("by_alias", True)
        return super().model_validate(**kwargs)

    @property
    def ok(self) -> bool:
        return True


class LnurlPayActionResponse(LnurlResponseModel):
    pr: LightningInvoice
    success_action: Optional[Union[MessageAction, AnyUrl]] = Field(None, alias="successAction")
    routes: List[List[LnurlPayRouteHop]] = []
    verify: Optional[str] = None


class LnurlPayResponseComment(LnurlResponseModel):
    """
    ref LUD-12: Comments in payRequest.
    """

    tag: Literal["payRequest"] = "payRequest"
    callback: AnyUrl
    min_sendable: MilliSatoshi = Field(..., alias="minSendable")
    max_sendable: MilliSatoshi = Field(..., alias="maxSendable")
    metadata: str
    comment_allowed: int = Field(
        1000,
        description="Length of comment which can be sent",
        alias="commentAllowed",
    )

    @model_validator(mode="after")
    def max_less_than_min(self) -> Self:
        if self.max_sendable < self.min_sendable:
            raise ValidationError("`max_sendable` cannot be less than `min_sendable`.")
        return self

    # TODO: #240 Add a field_validator to check if the metadata is a valid JSON string


class LnurlCurrencyEnum(str, Enum):
    """
    Represents the available currencies for LNURL.

    Attributes:
        hive (str): The currency code for Hive.
        hbd (str): The currency code for HBD.
        sats (str): The currency code for sats.
    """

    hive = "hive"
    hbd = "hbd"
    sats = "sats"


LnurlCurrency = Annotated[
    LnurlCurrencyEnum, Field(description="The currency for the pay request.")
]


def currency_pretty(v: LnurlCurrency) -> str:
    return {
        "hive": "Hive",
        "hbd": "HBD",
        "sats": "sats",
    }.get(v, "")


def currency_hashtag(v: LnurlCurrency) -> str:
    return {
        "hive": "#HIVE",
        "hbd": "#HBD",
        "sats": "#SATS",
    }.get(v, "")


LightningNodeUri = Annotated[str, Field(description="The URI of the lightning node.")]
LnurlPayMetadata = Annotated[str, Field(description="The metadata for the pay request.")]


class LnurlErrorResponse(LnurlResponseModel):
    status: Literal["ERROR"] = "ERROR"
    reason: str

    @property
    def error_msg(self) -> str:
        return self.reason

    @property
    def ok(self) -> bool:
        return False


class LnurlSuccessResponse(LnurlResponseModel):
    status: Literal["OK"] = "OK"


class LnurlAuthResponse(LnurlResponseModel):
    tag: Literal["login"] = "login"
    callback: AnyUrl
    k1: str


class LnurlChannelResponse(LnurlResponseModel):
    tag: Literal["channelRequest"] = "channelRequest"
    uri: LightningNodeUri
    callback: AnyUrl
    k1: str


class LnurlHostedChannelResponse(LnurlResponseModel):
    tag: Literal["hostedChannelRequest"] = "hostedChannelRequest"
    uri: LightningNodeUri
    k1: str
    alias: Optional[str] = None


class LnurlPayResponse(LnurlResponseModel):
    tag: Literal["payRequest"] = "payRequest"
    callback: AnyUrl
    min_sendable: MilliSatoshi = Field(..., alias="minSendable", gt=0)
    max_sendable: MilliSatoshi = Field(..., alias="maxSendable", gt=0)
    metadata: LnurlPayMetadata

    @field_validator("max_sendable")
    def max_less_than_min(cls, value, values, **kwargs):  # noqa
        if "min_sendable" in values and value < values["min_sendable"]:
            raise ValueError("`max_sendable` cannot be less than `min_sendable`.")
        return value

    @property
    def min_sats(self) -> int:
        return int(math.ceil(self.min_sendable / 1000))

    @property
    def max_sats(self) -> int:
        return int(math.floor(self.max_sendable / 1000))


class LnurlWithdrawResponse(LnurlResponseModel):
    tag: Literal["withdrawRequest"] = "withdrawRequest"
    callback: AnyUrl
    k1: str
    min_withdrawable: MilliSatoshi = Field(..., alias="minWithdrawable", gt=0)
    max_withdrawable: MilliSatoshi = Field(..., alias="maxWithdrawable", gt=0)
    default_description: str = Field("", alias="defaultDescription")

    @model_validator(mode="after")
    def max_less_than_min(self) -> Self:
        if self.max_withdrawable < self.min_withdrawable:
            raise ValidationError("`max_withdrawable` cannot be less than `min_withdrawable`.")
        return self

    @property
    def min_sats(self) -> int:
        return int(math.ceil(self.min_withdrawable / 1000))

    @property
    def max_sats(self) -> int:
        return int(math.floor(self.max_withdrawable / 1000))


class LnurlPayOption(BaseModel):
    type: Literal["lnurlp"] = "lnurlp"
    callback: AnyUrl
    min_sendable: int = Field(..., alias="minSendable")
    max_sendable: int = Field(..., alias="maxSendable")
    metadata: str
    comment_allowed: Optional[int] = Field(None, alias="commentAllowed")


class KeysendCustomData(BaseModel):
    customKey: int = 818818
    customValue: str = Field(
        title="Hive account name",
        description="Valid Hive account name",
    )


class KeysendOption(BaseModel):
    type: Literal["keysend"]
    pubkey: str
    custom_data: List[KeysendCustomData] = Field(..., alias="customData")


class PodOptionsResponse(BaseModel):
    status: Literal["OK"] = "OK"
    options: List[LnurlPayOption | KeysendOption]
