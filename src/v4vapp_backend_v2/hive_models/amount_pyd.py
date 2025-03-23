from beem.amount import Amount  # type: ignore
from pydantic import BaseModel


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

    def __init__(self, **data) -> None:
        super().__init__(**data)

    @property
    def beam(self) -> Amount:
        return Amount(self.amount, self.nai)

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
        # return float(self.amount) / (10**self.precision)
        return self.beam.amount_decimal

    @property
    def symbol(self) -> str:
        return self.beam.symbol
