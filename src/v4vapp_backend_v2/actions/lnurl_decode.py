import re
from typing import Dict, Tuple
from urllib.parse import urlparse

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
from v4vapp_backend_v2.config.setup import logger

# Get this from the config
LNURL_BASE_URL = "https://v4v.app/"


class LnurlException(Exception):
    """Custom exception for LNURL errors."""

    def __init__(self, message: str | None = None, failure: Dict[str, str] | None = None):
        if message is None and failure:
            message = failure.get("error", "Unknown error")
        logger.error(
            f"LnurlException: {message}", extra={"notification": False, "lnurl_failure": failure}
        )
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
    Performs the actual .well-known lookup for a lightning address
    Filters out calls to this API to prevent stressing own api

    Args:
        url (str): The URL to perform the lookup on.
        failure (Dict[str, str]): A dictionary to store any failure messages or errors encountered
        during the lookup.

    Returns:
        LnurlPayResponseCommentNostr: The response object containing the result of
        the lookup.

    Raises:
        HTTPException: If any error occurs during the lookup process.

    """
    # Needs to decode the sats.v4v.app or hbd.v4v.app
    # Check if this is a URL for this API/website
    # parsed_url = urlparse(url)
    # if parsed_url.netloc.endswith(urlparse(LNURL_BASE_URL).netloc):
    #     failure.append({"message": "Processing own urls"})
    #     if parsed_url.path.startswith("/.well-known/lnurlp/"):
    #         try:
    #             if len(parsed_url.netloc.split(".")) == 2:
    #                 currency = LnurlCurrencyEnum.hive
    #             else:
    #                 currency = TypeAdapter(LnurlCurrency).validate_python(
    #                     parsed_url.netloc.split(".")[0]
    #                 )
    #             hive_accname = parsed_url.path.split("/")[-1]
    #             lpr = await lnd_lnurlp(hive_accname, no_image=False, currency=currency)
    #             return lpr
    #         except ValueError as ex:
    #             failure.append({"error": f"{ex}"})
    #             raise LnurlException(failure=failure)
    #         except Exception as ex:
    #             failure.append({"error": f"{ex}"})
    #             raise LnurlException(failure=failure)

    try:
        res = httpx.get(url, follow_redirects=True)
        res_json = res.json()
    except (httpx.ReadTimeout, httpx.ConnectError) as ex:
        failure["error"] = f"{ex}"
        raise LnurlException(failure=failure)
    except Exception as ex:
        try:
            failure["error"] = str(res.status_code)
        except NameError:
            pass  # Ignore the error if res is undefined
        failure["error"] = ex.args[0]
        raise LnurlException(failure=failure)
    if not res.is_success:
        try:
            failure["error"] = str(res.status_code)
        except NameError:
            pass
        raise LnurlException(failure=failure)
    try:
        lpr = LnurlPayResponseComment(**res_json)
        return lpr
    except Exception as ex:
        failure["error"] = str(res_json)
        logger.error(f"Error in proxy {ex}")
        raise LnurlException(failure=failure)


def lightning_address_url(hive_accname: str, prefix: str = "") -> str:
    """Return the lightning address URL."""
    blob = urlparse(LNURL_BASE_URL)
    if prefix:
        lightning_address = f"{hive_accname}@{prefix}.{blob.netloc}"
    else:
        lightning_address = f"{hive_accname}{blob.netloc}"
    return lightning_address
