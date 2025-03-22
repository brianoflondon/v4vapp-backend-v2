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

    # @property
    # def decimal_amount(self) -> float:
    #     """Convert string amount to decimal with proper precision"""
    #     return float(self.amount) / (10**self.precision)

    @property
    def beam(self) -> Amount:
        return Amount(self.amount, self.nai)
