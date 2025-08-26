from enum import StrEnum


class Currency(StrEnum):
    HIVE = "hive"
    HBD = "hbd"
    USD = "usd"
    SATS = "sats"
    MSATS = "msats"
    BTC = "btc"

    @property
    def symbol(self) -> str:
        """
        Returns the symbol of the cryptocurrency in uppercase for Nectar Amount.

        Returns:
            str: The uppercase symbol of the cryptocurrency.
        """
        if self in [Currency.HIVE, Currency.HBD]:
            return self.value.upper()
        raise ValueError("Invalid currency")
