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

    @property
    def is_evm(self) -> bool:
        # Implement your validation logic here
        if self.startswith("0x") and len(self) == 42:
            try:
                int(self[2:], 16)
                return True
            except ValueError:
                return False
        return False

    @property
    def is_contract(self) -> bool:
        if self.startswith("contract:"):
            return True
        return False

    @property
    def magi_prefix(self) -> str:
        if isinstance(self, AccName):
            raw_account = str(self)
        else:
            raw_account = self

        if not raw_account:
            raise ValueError("account is required")

        if raw_account.startswith("hive:"):
            return raw_account

        if raw_account.startswith("did:pkh:eip155:1:"):
            return raw_account

        if AccName(raw_account).is_hive:
            return f"hive:{raw_account.lower()}"

        if AccName(raw_account).is_evm:
            return f"did:pkh:eip155:1:{raw_account.lower()}"

        if AccName(raw_account).is_contract:
            return f"contract:{raw_account[9:].lower()}"

        raise ValueError("Invalid account format, expected a Hive account name or EVM address")


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
