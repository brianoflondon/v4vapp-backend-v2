from datetime import datetime, timezone
from typing import Any, Dict, List

from v4vapp_backend_v2.accounting.account_type import Account


def list_all_accounts_pipeline() -> List[object]:
    """
    Returns a MongoDB aggregation pipeline to list all accounts with their details.
    The pipeline performs the following operations:
    1. Projects the `debit` and `credit` fields into an `accounts` array.
    2. Unwinds the `accounts` array to create separate documents for each account.
    3. Groups the documents by `account_type`, `name`, and `sub` to remove duplicates.
    4. Projects the final output to include only the relevant fields.
    5. Sorts the results by `account_type`, `name`, and `sub`.
    """
    pipeline = [
        {
            "$project": {
                "accounts": [
                    {
                        "account_type": "$debit.account_type",
                        "name": "$debit.name",
                        "sub": "$debit.sub",
                    },
                    {
                        "account_type": "$credit.account_type",
                        "name": "$credit.name",
                        "sub": "$credit.sub",
                    },
                ]
            }
        },
        {"$unwind": "$accounts"},
        {
            "$group": {
                "_id": {
                    "account_type": "$accounts.account_type",
                    "name": "$accounts.name",
                    "sub": "$accounts.sub",
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "account_type": "$_id.account_type",
                "name": "$_id.name",
                "sub": "$_id.sub",
            }
        },
        {"$sort": {"account_type": 1, "name": 1, "sub": 1}},
    ]
    return pipeline


def filter_by_account_as_of_date_query(
    account: Account | None = None, as_of_date: datetime = datetime.now(tz=timezone.utc)
) -> Dict[str, Any]:
    """
    Generates a MongoDB query to filter documents by a specific account and date.

    This function creates a query that filters documents based on the provided
    account details (`name` and optionally `sub`) and ensures that the `timestamp`
    field is less than or equal to the specified `as_of_date`.

    Args:
        account (Account): The account object containing `name` and optionally `sub`
            to filter the documents.
        as_of_date (datetime, optional): The cutoff date for filtering documents.
            Defaults to the current datetime in UTC.

    Returns:
        Dict[str, Any]: A dictionary representing the MongoDB query.
    """
    if account:
        query = {
            "timestamp": {"$lte": as_of_date},
            "$or": [
                {
                    "debit.name": account.name,
                    "debit.sub": account.sub if account.sub else "",
                },
                {
                    "credit.name": account.name,
                    "credit.sub": account.sub if account.sub else "",
                },
            ],
        }
    else:
        query = {
            "timestamp": {"$lte": as_of_date},
        }
    return query
