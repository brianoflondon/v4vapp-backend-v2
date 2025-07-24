# Custom string class for Hive account names
from enum import StrEnum, auto
from typing import Annotated

from pydantic import AfterValidator


class AccName(str):
    @property
    def link(self) -> str:
        # Replace this with your specific URL pattern (the "mussel")
        return f"https://hivehub.dev/@{self}"

    @property
    def markdown_link(self) -> str:
        # Replace this with your specific URL pattern (the "mussel")
        return f"[{self}](https://hivehub.dev/@{self})"

    @property
    def is_hive(self) -> bool:
        # Implement your validation logic here
        return check_hive_name(self) == AccountNameDetail.VALID_HIVE


# Annotated type with validator to cast to HiveAccName
AccNameType = Annotated[str, AfterValidator(lambda x: AccName(x))]
# Annotated type with validator to cast to HiveAccName


class AccountNameDetail(StrEnum):
    VALID_HIVE = auto()
    VALID_EVM = auto()
    # VALID_NOSTR = auto()
    TOO_SHORT = auto()
    TOO_LONG = auto()
    WRONG_LENGTH = auto()
    INVALID_SEQUENCE = auto()
    INVALID_CHARACTERS = auto()


def check_hive_name(name: str) -> AccountNameDetail:
    # Constants for min and max length
    HIVE_MIN_ACCOUNT_NAME_LENGTH = 3
    HIVE_MAX_ACCOUNT_NAME_LENGTH = 16

    # Check length
    if len(name) < HIVE_MIN_ACCOUNT_NAME_LENGTH:
        return AccountNameDetail.TOO_SHORT
    if len(name) > HIVE_MAX_ACCOUNT_NAME_LENGTH:
        return AccountNameDetail.TOO_LONG

    # Split by '.'
    parts = name.split(".")

    for part in parts:
        if len(part) < 3:
            return AccountNameDetail.INVALID_SEQUENCE

        # Check first character
        if not part[0].isalpha():
            return AccountNameDetail.INVALID_SEQUENCE

        # Check last character
        if not (part[-1].isalnum()):
            return AccountNameDetail.INVALID_SEQUENCE

        # Check middle characters
        for char in part[1:-1]:
            if char not in "abcdefghijklmnopqrstuvwxyz0123456789-":
                return AccountNameDetail.INVALID_SEQUENCE

    return AccountNameDetail.VALID_HIVE
