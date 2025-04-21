from typing import Annotated

from pydantic import AfterValidator


class DIDName(str):
    def __new__(cls, value: str):
        # Remove the optional "did:" prefix if present
        if value.startswith("did:"):
            value = value[4:]  # Strip the "did:" prefix

        # Validate the format "method:identifier"
        if ":" not in value:
            raise ValueError("DIDName must be in the format 'method:identifier'")
        method, identifier = value.split(":", 1)
        if not method or not identifier:
            raise ValueError("Both method and identifier must be non-empty in 'method:identifier'")
        return super().__new__(cls, value)

    @property
    def did(self) -> str:
        """Returns the DIDName with the 'did:' prefix."""
        return f"did:{self}"

    @property
    def method(self) -> str:
        """Returns the method part of the DIDName (before the colon)."""
        return self.split(":", 1)[0]

    @property
    def identifier(self) -> str:
        """Returns the identifier part of the DIDName (after the colon)."""
        return self.split(":", 1)[1]

    @property
    def link(self) -> str:
        """Generates a link based on the identifier."""
        # Replace this with your specific URL pattern
        if self.method == "hive":
            return f"https://hivehub.dev/@{self.identifier}"
        else:
            return ""

    @property
    def markdown_link(self) -> str:
        """Generates a Markdown link based on the identifier."""
        # Replace this with your specific URL pattern
        return f"[{self.identifier}](https://hivehub.dev/@{self.identifier})"


# Annotated type with validator to cast to DIDName
DIDNameType = Annotated[str, AfterValidator(lambda x: DIDName(x))]
