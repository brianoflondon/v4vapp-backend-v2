from typing import ClassVar

from pydantic import BaseModel, Field
from pymongo.results import UpdateResult

from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case


class TrackedBaseModel(BaseModel):
    locked: bool = Field(
        default=False,
        description="Flag to indicate if the operation is locked or being processed",
        exclude=False,
    )

    db_client: ClassVar[MongoDBClient | None] = None

    def __init__(self, **data):
        """
        Initialize the TrackedBaseModel with the provided data.

        :param data: The data to initialize the model with.
        """
        super().__init__(**data)
        self.locked = data.get("locked", False)

    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the class in snake_case format.

        This method converts the class name to a snake_case string
        representation, which is typically used for naming operations
        or identifiers in a consistent and readable format.

        Returns:
            str: The snake_case representation of the class name.
        """
        return snake_case(cls.__name__)

    @property
    def collection(self) -> str:
        """
        Returns the name of the collection associated with this model.

        This method should be overridden in subclasses to provide the
        specific collection name.

        Returns:
            str: The name of the collection.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @property
    def group_id_query(self) -> dict:
        """
        Returns the query used to identify the group ID in the database.

        This method should be overridden in subclasses to provide the
        specific query for the group ID.

        Returns:
            dict: The query used to identify the group ID.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    async def lock_op(self) -> UpdateResult | None:
        """
        Locks the operation to prevent concurrent processing.

        This method sets the `locked` attribute to True, indicating that
        the operation is currently being processed and should not be
        modified or accessed by other threads or processes.

        Returns:
            None
        """
        self.locked = True
        if TrackedBaseModel.db_client:
            ans = await TrackedBaseModel.db_client.update_one(
                collection_name=self.collection,
                query=self.group_id_query,
                update={"locked": self.locked},
            )
            return ans
        return None

    async def unlock_op(self) -> UpdateResult | None:
        """
        Unlocks the operation to allow concurrent processing.

        This method sets the `locked` attribute to False, indicating that
        the operation is no longer being processed and can be modified
        or accessed by other threads or processes.

        Returns:
            None
        """
        self.locked = False
        if self.db_client:
            # Remember my update_one already has the $set
            ans = await self.db_client.update_one(
                collection_name=self.collection,
                query=self.group_id_query,
                update={"$unset": {"locked": ""}},
            )
            return ans
        return None
