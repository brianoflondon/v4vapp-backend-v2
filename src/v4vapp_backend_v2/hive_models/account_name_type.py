# Custom string class for Hive account names
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
        return f"**[{self}](https://hivehub.dev/@{self})**"


# Annotated type with validator to cast to HiveAccName
AccNameType = Annotated[str, AfterValidator(lambda x: AccName(x))]
# Annotated type with validator to cast to HiveAccName
