import re
from decimal import Decimal
from typing import Dict, Tuple
from urllib.parse import parse_qs, urlparse

import httpx
from bech32 import convertbits
from pydantic import AnyUrl

from v4vapp_backend_v2.actions.lnurl_models import (
    LNURL_WELL_KNOWN_REGEX,
    LnurlPayResponseComment,
    LnurlProxyData,
    lnurl_bech32_decode,
    strip_lightning,
)
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    get_node_alias_from_pub_key,
    get_pay_req_from_pay_request,
)
from v4vapp_backend_v2.models.pay_req import PayReq, protobuf_pay_req_to_pydantic

# Get this from the config
LNURL_BASE_URL = "https://v4v.app/"


class LnurlException(Exception):
    """Custom exception for LNURL errors."""

    def __init__(self, message: str | None = None, failure: Dict[str, str] | None = None):
        if message is None and failure:
            message = failure.get("error", "Unknown error")
        super().__init__(message)


def check_lightning_address(lightning_address: str, get_url: bool = False) -> str:
    """
    Return the lightning_address URL if the lightning address is valid.

    Args:
        lightning_address (str): The lightning address to check.
        get_url (bool, optional): Whether to return the URL instead of the
        lightning address.
            Defaults to False.

    Returns:
        str: The lightning_address URL if the lightning address is valid,
            or the lightning address itself if `get_url` is False.

    """
    try:
        username, path = lightning_address.split("@")
        found_url = AnyUrl(f"https://{path}/.well-known/lnurlp/{username}")
        if get_url:
            return str(found_url)
        return lightning_address
    except Exception:
        return ""


def check_bech32_lnurl(bech32_lnurl: str) -> str:
    """
    Test for a bech32 string as a valid lnurl. Returns a decoded URL
    or empty string if not a bech32 url.

    Parameters:
    - bech32_lnurl (str): The bech32 string to be checked.

    Returns:
    - str: The decoded URL if the bech32 string is valid, otherwise an empty string.
    """
    try:
        # decoded_url = lnurl_decode(bech32_lnurl)
        bech32_lnurl = strip_lightning(bech32_lnurl)
        _, data = lnurl_bech32_decode(bech32_lnurl)
        try:
            bech32_data = convertbits(data, 5, 8, False)
            assert bech32_data
            url = bytes(bech32_data).decode("utf-8")
            return url
        except UnicodeDecodeError:
            pass

    except Exception:
        pass
    return ""


def check_bech32_or_lightning_address(anything: str) -> Tuple[str, str]:
    """
    Checks if the given string is a valid lightning address or bech32 address.

    Args:
        anything (str): The string to be checked.

    Returns:
        Tuple[str, str]: A tuple containing the result of checking the lightning address
        and the bech32 address, respectively.
    """
    return check_lightning_address(anything), check_bech32_lnurl(anything)


async def decode_any_lightning_string(
    input: str,
    zero_amount_invoice_send_msats: Decimal = Decimal(0),
    comment: str = "",
    lnd_client: LNDClient | None = None,
) -> PayReq:
    """
    Takes in a string and checks if it is a valid LNURL or a valid Lightning Address.

    Args:
        input (str): The input string to decode.
        zero_amount_invoice_send_msats (int, optional): The amount in millisatoshis. Defaults to 0.
                this amount is used for zero amount invoices and ignored if not a zero amount invoice.
        comment (str, optional): A comment to include. Defaults to "".

    Returns:
        PayReq: Returns a PayReq object if the input is a valid Lightning invoice,
                  otherwise raises an exception.

        the value which will be paid is pay_req.value_msat, which is the amount in millisatoshis.

    Raises:
        LNDInvoiceError: If the amount is out of the allowed range or the comment is too long
        some other error with the LNDInvoice.
    """
    input = strip_lightning(input)
    extras = input.split(" ", 1)
    if len(extras) > 1:
        comment = extras[1] if not comment else comment
        input = extras[0]

    if not lnd_client:
        lnd_config = InternalConfig().config.lnd_config
        lnd_client = LNDClient(connection_name=lnd_config.default)

    if input.startswith("lnbc"):
        lnrpc_pay_req = await get_pay_req_from_pay_request(
            pay_request=input, lnd_client=lnd_client
        )
        # # Dealing with a zero sat invoice record the amount to be sent.
        # if ln_invoice.zero_sat:
        #     ln_invoice.force_send_sats = sats
        pay_req = protobuf_pay_req_to_pydantic(lnrpc_pay_req, pay_req_str=input)
        pay_req.dest_alias = await get_node_alias_from_pub_key(
            pay_req.destination, lnd_client=lnd_client
        )
        pay_req.send_everything = pay_req.is_zero_value or False
        return pay_req

    data = LnurlProxyData(
        anything=input,
    )
    try:
        response = await decode_any_lnurp_or_lightning_address(data)
        if response.tag != "payRequest":
            raise LnurlException("Not a valid LNURLp or Lightning Address")
        if not (response.min_sendable <= zero_amount_invoice_send_msats <= response.max_sendable):
            message = f"Amount {zero_amount_invoice_send_msats // 1_000:,} out of range: {response.min_sendable // 1_000:,} -> {response.max_sendable // 1_000:,}"
            logger.warning(message, extra={"notification": False})
            raise LnurlException(
                message=message,
                failure={"error": "amount out of range"},
            )
        if not response.comment_allowed:
            params = {"amount": int(zero_amount_invoice_send_msats)}
        else:
            if len(comment) > response.comment_allowed:
                comment = comment[: response.comment_allowed]
            params = {"amount": int(zero_amount_invoice_send_msats), "comment": comment}

    except LnurlException as ex:
        logger.debug(
            f"LnurlException: {ex}", extra={"notification": False, "lnurl_failure": str(ex)}
        )
        # Bare raise to avoid chaining exceptions
        raise

    # Parse the query string from response.callback
    parsed_url = urlparse(str(response.callback))
    query_params = parse_qs(parsed_url.query)  # Convert query string to a dictionary

    # Flatten the query_params dictionary (parse_qs returns lists for each key)
    query_params = {key: value[0] for key, value in query_params.items()}

    # Merge query_params with the existing params
    merged_params = {**query_params, **params}

    # Make the HTTP request with the merged parameters
    with httpx.Client() as httpx_client:
        try:
            # raise httpx.RequestError(
            #     "Simulated timeout for testing"
            # )  # Simulate a timeout error for testing
            response = httpx_client.get(
                parsed_url._replace(query="").geturl(),  # Use the base URL without the query
                params=merged_params,  # Pass the merged parameters
                follow_redirects=True,
            )
            response.raise_for_status()
            response_data = response.json()
            if response_data.get("pr"):
                lnrpc_pay_req = await get_pay_req_from_pay_request(
                    pay_request=response_data["pr"], lnd_client=lnd_client
                )
                if lnrpc_pay_req:
                    return protobuf_pay_req_to_pydantic(
                        lnrpc_pay_req, pay_req_str=response_data["pr"]
                    )
            raise LnurlException("No payment request found in response")
        except (httpx.RequestError, httpx.HTTPStatusError) as ex:
            logger.error(f"HTTP error: {str(ex)}", extra={"notification": False})
            raise LnurlException(failure={"error": str(ex)})
        except Exception as ex:
            logger.exception(
                f"Unexpected error validating response: {str(ex)}", extra={"notification": False}
            )
            raise LnurlException(failure={"error": str(ex)})


async def decode_any_lnurp_or_lightning_address(
    data: Dict[str, str] | LnurlProxyData,
) -> LnurlPayResponseComment:
    """
    Decode any of LNURLp or Lightning Address. Raises HTTPException
    if the data is not valid.

    Args:
        data (LnurlProxyData): The data to be decoded.

    Returns:
        LnurlPayResponseComment: The decoded LNURLp response.

    Raises:
        HTTPException: If the data is not valid.

    """
    found = False
    valid_bech32 = False
    failure: Dict[str, str] = {}
    if not isinstance(data, LnurlProxyData):
        try:
            data = LnurlProxyData(**data)
        except Exception as ex:
            failure["error"] = str(ex)
            raise LnurlException(failure=failure)

    if data.anything:
        try:
            data.anything = strip_lightning(data.anything)
        except Exception as ex:
            logger.warning(f"Error decoding URL from lightning address: {ex}")
            failure["error"] = "processing anything failed"

        data.lightning_address = data.anything  # type: ignore

    if data.lightning_address:
        la = strip_lightning(data.lightning_address)
        data.decoded_url = check_lightning_address(la, get_url=True)
        if data.decoded_url:
            found = True
        else:
            failure["error"] = "not lightning address"

    if not found and data.anything:
        data.bech32_lnurl = data.anything

    if not data.decoded_url and data.bech32_lnurl:
        data.decoded_url = check_bech32_lnurl(data.bech32_lnurl)
        if data.decoded_url:
            valid_bech32 = True
            found = True
            failure["message"] = "processing as bech32 lnurl"
        else:
            failure["error"] = "not lnurl"

    if not found and data.anything:
        data.decoded_url = data.anything

    if data.decoded_url:
        if not valid_bech32 and not re.search(LNURL_WELL_KNOWN_REGEX, str(data.decoded_url)):
            failure["error"] = "not valid lnurl"
            failure["message"] = "Can't process"
            raise LnurlException(failure=failure)
        logger.info(f"Proxying: {data.decoded_url}")
        try:
            failure["message"] = "processing as lightning address"
            lpr = await perform_lnaddress_proxy(url=data.decoded_url, failure=failure)
        except Exception as ex:
            logger.error(f"Error in proxy {ex}")
            raise LnurlException(failure=failure)
        return lpr
    failure["message"] = "nothing to process"
    raise LnurlException(failure=failure)


async def perform_lnaddress_proxy(url: str, failure: Dict[str, str]) -> LnurlPayResponseComment:
    """
    Performs the actual .well-known lookup for a lightning address.
    Filters out calls to this API to prevent stressing its own API.

    Args:
        url (str): The URL to perform the lookup on.
        failure (Dict[str, str]): A dictionary to store any failure messages or errors encountered
        during the lookup.

    Returns:
        LnurlPayResponseComment: The response object containing the result of the lookup.

    Raises:
        LnurlException: If any error occurs during the lookup process.
    """
    try:
        # Perform the HTTP GET request
        response = httpx.get(url, follow_redirects=True)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        response_data = response.json()
    except (httpx.RequestError, httpx.HTTPStatusError) as ex:
        # Handle HTTP and connection-related errors
        failure["error"] = f"HTTP error: {str(ex)}"
        raise LnurlException(failure=failure)
    except ValueError as ex:
        # Handle JSON decoding errors
        failure["error"] = f"Invalid JSON response: {str(ex)}"
        raise LnurlException(failure=failure)

    try:
        # Validate and return the response as LnurlPayResponseComment
        return LnurlPayResponseComment(**response_data)
    except Exception as ex:
        # Handle validation or unexpected errors
        failure["error"] = f"Error validating response: {str(ex)}"
        logger.error(f"Error in proxy: {ex}")
        raise LnurlException(failure=failure)


def lightning_address_url(hive_accname: str, prefix: str = "") -> str:
    """Return the lightning address URL."""
    blob = urlparse(LNURL_BASE_URL)
    if prefix:
        lightning_address = f"{hive_accname}@{prefix}.{blob.netloc}"
    else:
        lightning_address = f"{hive_accname}{blob.netloc}"
    return lightning_address
