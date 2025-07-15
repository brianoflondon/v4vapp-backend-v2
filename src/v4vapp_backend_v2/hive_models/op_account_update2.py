import json
from typing import Any, Dict, List

from pydantic import Field

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.op_base import OpBase


class AccountUpdate2(OpBase):
    account: AccNameType = Field("", description="Account being updated")
    json_metadata: Dict[str, Any] | List[Any] | None = Field(
        {},
        description="JSON metadata associated with the account update",
    )
    posting_json_metadata: Dict[str, Any] | List[Any] | None = Field(
        {},
        description="JSON metadata associated with the posting key of the account update",
    )

    def __init__(self, **hive_event: Any) -> None:
        # Ensure json_metadata is a dictionary
        hive_event["json_metadata"] = self.ensure_dict(hive_event.get("json_metadata"))

        # Ensure posting_json_metadata is a dictionary
        hive_event["posting_json_metadata"] = self.ensure_dict(
            hive_event.get("posting_json_metadata")
        )

        try:
            super().__init__(**hive_event)
        except Exception as e:
            logger.error(f"Error in AccountUpdate2: {e}", extra={"notification": False})
            raise e

    @property
    def is_watched(self) -> bool:
        """
        Checks if the account is watched based on the JSON metadata.

        Returns:
            bool: True if the account is watched, False otherwise.
        """
        if self.account in self.watch_users:
            return True
        return False

    @property
    def log_str(self) -> str:
        """
        Returns a string representation of the account update operation.

        Returns:
            str: A formatted string representing the account update operation.
        """
        return f"Account Update: {self.account} updated metadata {self.link}"

    @property
    def notification_str(self) -> str:
        """
        Generates a notification string summarizing the account update operation.

        Returns:
            str: A formatted string containing details about the account update.
        """
        return f"Account Update: {self.account} updated metadata {self.markdown_link}"

    @property
    def log_extra(self) -> dict[str, Any]:
        """
        Returns additional information for logging purposes.

        Returns:
            dict: A dictionary containing extra information about the account update.
        """
        return {
            "account": self.account,
            "json_metadata": self.json_metadata,
            "posting_json_metadata": self.posting_json_metadata,
            "extensions": self.extensions,
        }

    @staticmethod
    def ensure_dict(value: Any) -> dict:
        """
        Ensures that the given value is a dictionary. If the value is a string,
        it attempts to parse it as JSON. If the value is None or an empty string,
        it returns an empty dictionary.

        Args:
            value (Any): The value to ensure is a dictionary.

        Returns:
            dict: The resulting dictionary.
        """
        if isinstance(value, str) and value == "":
            return {}
        elif isinstance(value, str):
            return json.loads(value)
        elif isinstance(value, dict):
            return value
        return {}
