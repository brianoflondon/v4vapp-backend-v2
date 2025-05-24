import base64
import json
import re
from base64 import b64decode
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from v4vapp_backend_v2.config.setup import logger


def is_json(check_json: str) -> bool:
    """
    Checks if the given input is a valid JSON string.

    Args:
        check_json (str): The string to be checked.

    Returns:
        bool: True if the input is a valid JSON string, False otherwise.
    """
    try:
        json.loads(check_json)
    except ValueError:
        return False
    return True


def b64_decode(base64_message) -> str | dict:
    """
    Decodes a base64-encoded string and returns the decoded value.

    If the decoded value is a valid JSON string, it is parsed and returned as a dictionary.
    Otherwise, the decoded value is returned as a string.

    Args:
        base64_message (str): The base64-encoded string to decode. If None, an empty string is returned.

    Returns:
        str | dict: The decoded value, either as a string or a dictionary if the decoded value is valid JSON.
    """
    if base64_message is None:
        return ""

    base64_bytes = base64_message.encode("utf-8")
    message_bytes = b64decode(base64_bytes)
    message = message_bytes.decode("utf-8")
    if is_json(message):
        return json.loads(message)
    else:
        return message


class KeysendCustomRecord(BaseModel):
    podcast: str | None = Field(None, description="Title of the podcast")
    feedID: int | None = Field(None, description="ID of podcast in PodcastIndex.org directory")
    url: str | None = Field(None, description="RSS feed URL of podcast")
    guid: str | None = Field(
        None,
        description="The `<podcast:guid>` tag. See https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md#guid",
    )
    #
    episode: str | None = Field(None, description="Title of the podcast episode")
    itemID: int | None = Field(None, description="ID of episode in PodcastIndex.org directory")
    episode_guid: str | None = Field(None, description="The GUID of the episode")
    #
    time: str | None = Field(
        None,
        description="Timestamp of when the payment was sent, in HH:MM:SS notation, as an offset from 00:00:00 (i.e., playback position)",
    )
    ts: int | None = Field(
        None,
        description="Timestamp of when the payment was sent, in seconds, as an offset from zero (i.e., playback position)",
    )
    action: str | None = Field(
        "stream",
        description='Action type for the payment: "boost", "stream", or "auto". See Appendix B of the value spec for details.',
    )
    app_name: str | None = Field(None, description="Name of the sending app")
    app_version: str | None = Field(None, description="Version of the sending app")
    boost_link: str | None = Field(
        None,
        description="App-specific URL containing a route to the podcast, episode, and/or timestamp at the time of the action",
    )
    message: str | None = Field(
        None,
        description="Text message to add to the payment. When present, the payment is known as a 'boostagram'.",
    )
    name: str | None = Field(None, description="Name for this split in the value tag")
    pubkey: str | None = Field(None, description="Public key of the sender")
    sender_key: str | None = Field(None, description="Public key of the sender")
    sender_name: str | None = Field(
        None, description="Name of the sender (free text, not validated in any way)"
    )
    sender_id: str | None = Field(
        None,
        description="Static random identifier for users, not displayed by apps to prevent abuse. Can be a GUID-like random identifier or a Nostr hex-encoded pubkey.",
    )
    sig_fields: str | None = Field(None, description="Fields used for signature generation")
    signature: str | None = Field(
        None,
        description="Signature for the payment, used to verify the sender's identity. If `sender_id` contains a Nostr public key, this should contain a Nostr signature.",
    )
    speed: str | None = Field(
        None,
        description="Speed at which the podcast was playing, in decimal notation (e.g., 0.5 for half speed, 2 for double speed).",
    )
    boost_uuid: str | None = Field(None, description="UUID for the boost/stream/auto payment")
    stream_uuid: str | None = Field(None, description="UUID for the stream payment")
    uuid: str | None = Field(None, description="UUID of a payment sent to a single recipient")
    value_msat: int | None = Field(
        None, description="Number of millisats for the payment after fees are subtracted"
    )
    cr_value_msat: int | None = Field(None, description="Custom record value in millisats")
    value_msat_total: int | None = Field(
        None,
        description="TOTAL number of millisats for the payment before any fees are subtracted. Important for preserving numerology reasons.",
    )
    reply_address: str | None = Field(
        None,
        description="The pubkey of the lightning node that can receive payments for the sender. Must be capable of receiving keysend payments.",
    )
    reply_custom_key: int | None = Field(
        None,
        description="Custom key for routing a reply payment to the sender. Should not be present if not required for payment routing.",
    )
    reply_custom_value: str | None = Field(
        None,
        description="Custom value for routing a reply payment to the sender. Should not be present if not required for payment routing.",
    )
    remote_feed_guid: str | None = Field(
        None,
        description="Feed GUID from the `<podcast:remoteItem>` tag when a payment is sent to a feed's value block via a `<podcast:valueTimeSplit>` tag.",
    )
    remote_item_guid: str | None = Field(
        None,
        description="Item GUID from the `<podcast:remoteItem>` tag when a payment is sent to a feed's value block via a `<podcast:valueTimeSplit>` tag.",
    )

    @field_validator(
        "speed",
        "app_version",
        "name",
        "guid",
        "reply_address",
        "reply_custom_key",
        "reply_custom_value",
        "guid",
        mode="before",
    )
    def coerce_to_str(cls, value):
        if isinstance(value, (float, int)):
            return str(value)
        if isinstance(value, dict):
            if value == {}:
                return ""
            try:
                return json.dumps(value, default=str)
            except TypeError:
                return None
        if isinstance(value, bytes):
            return value.decode("utf-8")
        if isinstance(value, str):
            if value.startswith("b'") and value.endswith("'"):
                return value[2:-1]
            if value.startswith('"') and value.endswith('"'):
                return value[1:-1]
            if value.startswith('{"cache'):
                #     Validates the `reply_address` field. If it contains JSON-like data, it is replaced with an empty string.
                #     This fixes Errheads bad tests 2023-06-12.
                return ""
            if value == {}:
                return ""
            if value.startswith("{") and value.endswith("}"):
                return json.loads(value)
        return value

    # @field_validator(
    #     "reply_address",
    #     mode="before",
    # )
    # def validate_reply_address(cls, value):
    #     """
    #     Validates the `reply_address` field. If it contains JSON-like data, it is replaced with an empty string.
    #     This fixes Errheads bad tests 2023-06-12.
    #     """
    #     if value.startswith('{"cache'):
    #         return ""
    #     return value

    @field_validator("ts", "value_msat", "itemID", mode="before")
    def coerce_to_int(cls, value):
        if isinstance(value, (float, int, str)):
            try:
                return int(value)
            except ValueError:
                return 0
        return value

    # @property
    # def trx_reason(self) -> "TrxReason":
    #     """
    #     Returns the trx reason for keysend invoice with this action
    #     This is where we will switch to KEEPSATS_BOOST and KEEPSATS_STREAMING
    #     """
    #     if self.action == "boost":
    #         return TrxReason.KEEPSATS_BOOST
    #     elif self.action == "auto":
    #         return TrxReason.KEEPSATS_AUTO
    #     else:
    #         return TrxReason.KEEPSATS_STREAMING

    @property
    def action_type(self) -> str:
        """
        Returns the action type for keysend invoice with this action
        Returns "stream" or "boost" for anything that isn't a stream
        """
        if self.action == "stream":
            return "stream"
        return "boost"

    @property
    def unique_listen_event_id(self) -> str:
        if self.action == "boost":
            if self.boost_uuid:
                return self.boost_uuid
        if self.action == "stream":
            if self.stream_uuid:
                return self.stream_uuid

        podcast = next(
            item for item in [str(self.guid), self.podcast, self.feedID, self.url] if item
        )
        episode = next(item for item in [self.episode, self.itemID, self.episode_guid] if item)
        player = f"{self.app_name}-{self.app_version}"
        action = f"{self.action}"
        unique_id = f"{podcast}-{episode}-{player}-{action}"
        unique_id = re.sub(r"[ /:]", "_", unique_id)
        return unique_id

    def __init__(self, **data: Any) -> None:
        """
        Cludge for Fountain problem 2023-05-15
        This fixes a problem where Fountain sent a string instead of an int
        for the itemID in the keysend invoice TLV records. Without this the
        code will throw multiple input errors every time it re-scans the whole
        database.
        """
        # if data.get("itemID") == "cB5cg0whBP9RuEOyH08j":
        #     data["itemID"] = 15465533793

        # # Cludge for Errheads bad tests 2023-06-12 added
        # if data.get("app_name") == "PeerTube" and (
        #     data.get("app_version") == "4.2.8" or data.get("app_version") == "4.2.6"
        # ):
        #     data.pop("guid", None)
        #     data.pop("reply_address", None)

        # if data.get("guid") == "":
        #     # remove guid item from data if it is empty
        #     # Podverse problem 2023-10-27
        #     data.pop("guid", None)
        #     logging.warning("Empty guid removed from keysend TLV import data")
        #     try:
        #         logging.warning(json.dumps(data))
        #     except Exception:
        #         pass
        super().__init__(**data)


class DecodedCustomRecord(BaseModel):
    """
    Represents a decoded set of custom records, typically extracted from keysend payments or similar sources.
    Attributes:
        podcast (KeysendCustomRecord | None): Decoded KeysendCustomRecord object from the field with alias "7629169".
        keysend_message (str | None): Decoded message from the field with alias "34349334", if available.
        v4vapp_group_id (str | None): Decoded group ID from the field with alias "1818181818", if available.
        hive_accname (str | None): Decoded Hive account name from the field with alias "818818", if available.
        other (Dict[str, Any]): Dictionary containing any additional custom records not mapped to the above fields.
    Methods:
        __init__(**data: Any):
            Initializes the DecodedCustomRecord instance with provided data.
            Any key-value pairs in `data` whose keys are not among ["7629169", "34349334", "1818181818", "818818"]
            are added to the `other` dictionary.
    """

    podcast: KeysendCustomRecord | None = Field(
        None,
        description="Decoded KeysendCustomRecord object from the 7629169 field",
        alias="7629169",
    )
    keysend_message: str | None = Field(
        None,
        description="Decoded message from the 34349334 field, if available",
        alias="34349334",
    )
    v4vapp_group_id: str | None = Field(
        None,
        description="Decoded group ID from the 1818181818 field, if available",
        alias="1818181818",
    )
    hive_accname: str | None = Field(
        None,
        description="Decoded Hive account name from the 818818 field, if available",
        alias="818818",
    )
    other: Dict[str, Any] = Field(
        default_factory=dict,
        description="Other custom records as a dictionary",
    )

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **data: Any) -> None:
        """
        Initializes the object with provided keyword arguments.

        Parameters:
            **data (Any): Arbitrary keyword arguments representing the data to initialize the object with.

        Behavior:
            - Calls the superclass initializer with the provided data.
            - For each key-value pair in data, if the key is not one of ["7629169", "34349334", "1818181818", "818818"],
              the pair is added to the `self.other` dictionary.
        """
        super().__init__(**data)
        for key, value in data.items():
            if key not in ["7629169", "34349334", "1818181818", "818818"]:
                self.other[key] = value


def decode_all_custom_records(
    custom_records: Dict[str, str],
) -> DecodedCustomRecord:
    """
    Decodes a dictionary of custom records, handling specific and generic cases.
    Args:
        custom_records (Dict[str, str]): A dictionary where keys are custom record identifiers (as strings)
            and values are Base64-encoded strings representing the record data.
    Returns:
        DecodedCustomRecord: An object containing the decoded custom records, including a parsed
            podcast custom record (if present) and a dictionary of other decoded records.
    Behavior:
        - For the key "7629169", decodes the value from Base64, validates it as a KeysendCustomRecord,
          and assigns it to the `podcast_custom_record` attribute of the result. Handles validation errors
          and logs warnings if decoding or validation fails.
        - For all other keys, decodes the value from Base64, attempts to parse it as JSON, and stores the
          result (either as a parsed object or string) in the `other_custom_records` dictionary of the result.
          If decoding fails, stores an error message instead.
    Raises:
        Does not raise exceptions; errors are logged and captured in the result object.
    """
    result: Dict[str, Any] = {}
    for key, value in custom_records.items():
        if key == "7629169":
            extracted_value = b64_decode(value)
            try:
                custom_record = KeysendCustomRecord.model_validate(extracted_value)
                result["7629169"] = custom_record
            except ValidationError:
                pass
            except Exception as e:
                logger.warning(
                    f"Error in custom record: {e}",
                    extra={"notification": False},
                )
                logger.warning(
                    f"Error validating custom record: {e} ",
                    extra={
                        "notification": False,
                        "custom_records": custom_records,
                    },
                )
        else:
            try:
                # Decode the Base64 value
                decoded_bytes = base64.b64decode(value)
                decoded_str = decoded_bytes.decode("utf-8")

                # Attempt to parse as JSON
                try:
                    decoded_value = json.loads(decoded_str)
                except json.JSONDecodeError:
                    decoded_value = decoded_str  # If not JSON, keep as string

                # Add to the result dictionary with integer key
                result[str(key)] = decoded_value
            except Exception as _:
                # Log or handle decoding errors
                result[str(key)] = "Error decoding value"

    answer = DecodedCustomRecord(**result)
    return answer
