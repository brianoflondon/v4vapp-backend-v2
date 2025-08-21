from typing import Dict

from pydantic import BaseModel, Field


class KeepsatsTransferExternal(BaseModel):
    """
    Model for transferring sats from one Hive account to another.
    This is the External facing version to match legacy API.

    Attributes:
        hive_accname_from (str): Hive name SENDING sats.
        hive_accname_to (str): Hive name RECEIVING sats.
        sats (int): Amount of sats to transfer.
        memo (str, optional): Memo to include with the transfer.
    """

    hive_accname_from: str = Field(
        ...,  # This field is required
        description="Hive name SENDING sats",
    )
    hive_accname_to: str = Field(
        ...,  # This field is required
        description="Hive name RECEIVING sats",
    )
    sats: int = Field(
        ...,  # This field is required
        description="Amount of sats to transfer",
    )
    memo: str = Field(
        "",  # This field is optional
        description="Memo to include with the transfer",
    )


class KeepsatsTransferResponse(BaseModel):
    """
    Model for returning the response from a transfer of sats from one Hive account to another.

    Attributes:
        success (bool): True if the transfer was successful.
        message (str): Message about the result of the transfer.
        trx_id (str): Transaction ID of the transfer.
        error (Optional[Dict]): Optional error information.
    """

    success: bool = Field(
        ...,
        description="True if the transfer was successful",
    )
    message: str = Field(
        ...,
        description="Message about the result of the transfer",
    )

    trx_id: str = Field(
        ...,
        description="Transaction ID of the transfer",
    )
    error: Dict | None = Field(
        default=None,
        description="Error information if the transfer failed",
    )
