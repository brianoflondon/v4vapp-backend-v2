from typing import Any

from nectar.amount import Amount
from pydantic import BaseModel

from v4vapp_backend_v2.helpers.crypto_prices import Currency


class AmountPyd(BaseModel):
    """
    AmountPyd is a Pydantic model that represents an amount with its associated
    NAI (Network Asset Identifier) and precision.

    Attributes:
        amount (str): The amount as a string.
        nai (str): The Network Asset Identifier.
        precision (int): The precision of the amount.

    Properties:
        beam (Amount): Returns an Hive Beam Library Amount object initialized
        with the amount and nai.
    """

    amount: str
    nai: str
    precision: int

    def __init__(self, **data: Any) -> None:
        if "asset" in data and data.get("asset", None):
            beam_amount = Amount(**data)
            new_data = beam_amount.json()
            super().__init__(**new_data)
        elif "amount" in data and isinstance(data["amount"], Amount):
            # If data contains an Amount object, convert it to a dictionary
            beam_amount = data["amount"]
            new_data = beam_amount.json()
            super().__init__(**new_data)
        else:
            super().__init__(**data)

    @property
    def beam(self) -> Amount:
        return Amount({"amount": self.amount, "nai": self.nai, "precision": self.precision})

    def __str__(self) -> str:
        return self.beam.__str__()

    def fixed_width_str(self, width: int) -> str:
        """
        Returns a fixed-width string representation of the amount and currency symbol.
        Args:
            width (int): The total width of the resulting string.
        Returns:
            str: A string containing the amount formatted to three decimal places,
                 right-justified to the specified width,
                 followed by the currency symbol right-justified to 4 characters.
        """

        number_str = f"{self.amount_decimal:,.3f}".rjust(width - 5)
        currency_str = f"{self.symbol:>4}"
        return f"{number_str} {currency_str}"

    @property
    def amount_decimal(self) -> float:
        """Convert string amount to decimal with proper precision"""
        # return self.beam.amount_decimal
        return float(self.amount) / (10**self.precision)

    @property
    def symbol(self) -> str:
        return self.beam.symbol

    @property
    def unit(self) -> Currency:
        return Currency(self.beam.symbol.lower())
