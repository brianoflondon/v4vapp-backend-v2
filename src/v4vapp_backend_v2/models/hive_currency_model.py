from nectar.amount import Amount
from pydantic import BaseModel, Field


class HiveCurrency(BaseModel):
    nai: str = Field(description="Network Asset Identifier")
    amount: str = Field(description="Amount as a string representation")
    precision: int = Field(description="Decimal precision for the amount")

    @property
    def decimal_amount(self) -> float:
        """Convert string amount to decimal with proper precision"""
        return float(self.amount) / (10**self.precision)

    @property
    def beem_amount(self) -> Amount:
        """Convert HiveCurrency to a beem Amount object"""
        return Amount(self.amount, self.nai)

    @property
    def symbol(self) -> str:
        """Get the symbol for the currency"""
        return self.beem_amount.symbol

    @property
    def value(self) -> float:
        """Convert the amount to a value in the quote currency"""
        return self.beem_amount.amount
