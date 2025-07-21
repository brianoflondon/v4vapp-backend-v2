from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount


def list_all_accounts_pipeline() -> Sequence[Mapping[str, Any]]:
    """
    Returns a MongoDB aggregation pipeline to list all accounts with their details.
    The pipeline performs the following operations:
    1. Projects the `debit` and `credit` fields into an `accounts` array.
    2. Unwinds the `accounts` array to create separate documents for each account.
    3. Groups the documents by `account_type`, `name`, and `sub` to remove duplicates.
    4. Projects the final output to include only the relevant fields.
    5. Sorts the results by `account_type`, `name`, and `sub`.
    """
    pipeline: Sequence[Mapping[str, Any]] = [
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


def account_balance_details_pipeline(
    account: LedgerAccount,
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    age: timedelta | None = None,
) -> Sequence[Mapping[str, Any]]:
    """
    Generates a MongoDB aggregation pipeline to retrieve detailed account balance information for a specified ledger account.

    Args:
        account (LedgerAccount): The ledger account for which to retrieve balance details.
        as_of_date (datetime, optional): The end date for the balance calculation. Defaults to the current UTC datetime.
        age (timedelta | None, optional): If provided, limits the results to transactions within the specified age (time window) ending at `as_of_date`.

    Returns:
        Sequence[Mapping[str, Any]]: A MongoDB aggregation pipeline that:
            - Filters transactions by date and existence of signed conversion values.
            - Separates debit and credit transactions for the specified account.
            - Projects relevant fields for each transaction, including amounts and conversion values.
            - Combines debit and credit transactions into a unified list.
            - Sorts transactions chronologically.
            - Groups transactions by unit (currency).
            - Calculates running totals for amounts and conversions in multiple currencies.
            - Structures the output as a mapping from unit to a list of transaction details with running totals.

    Note:
        The pipeline is intended for use with MongoDB aggregation queries and assumes the presence of specific fields in the transaction documents.

    """
    if age:
        start_date = as_of_date - age
        date_range_query = {"$gte": start_date, "$lte": as_of_date}
    else:
        date_range_query = {"$lte": as_of_date}

    pipeline: Sequence[Mapping[str, Any]] = [
        {"$match": {"timestamp": date_range_query, "conv_signed": {"$exists": True}}},
        {
            "$facet": {
                "debits": [
                    {"$match": {"debit.name": account.name, "debit.sub": account.sub}},
                    {
                        "$project": {
                            "_id": 0,
                            "group_id": 1,
                            "short_id": 1,
                            "ledger_type": 1,
                            "timestamp": 1,
                            "description": 1,
                            "cust_id": 1,
                            "debit_amount": "$debit_amount",
                            "credit_amount": {"$literal": 0},
                            "amount_signed": "$debit_amount_signed",
                            "unit": "$debit_unit",
                            "conv": "$debit_conv",
                            "debit": 1,
                            "credit": 1,
                            "op_type": 1,
                            "conv_signed": "$conv_signed.debit",
                        }
                    },
                ],
                "credits": [
                    {"$match": {"credit.name": account.name, "credit.sub": account.sub}},
                    {
                        "$project": {
                            "_id": 0,
                            "group_id": 1,
                            "short_id": 1,
                            "ledger_type": 1,
                            "timestamp": 1,
                            "description": 1,
                            "cust_id": 1,
                            "debit_amount": {"$literal": 0},
                            "credit_amount": "$credit_amount",
                            "amount_signed": "$credit_amount_signed",
                            "unit": "$credit_unit",
                            "conv": "$credit_conv",
                            "debit": 1,
                            "credit": 1,
                            "op_type": 1,
                            "conv_signed": "$conv_signed.credit",
                        }
                    },
                ],
            }
        },
        {"$project": {"combined": {"$concatArrays": ["$debits", "$credits"]}}},
        {"$unwind": "$combined"},
        {"$replaceRoot": {"newRoot": "$combined"}},
        {"$sort": {"timestamp": 1}},
        {"$group": {"_id": "$unit", "items": {"$push": "$$ROOT"}}},
        {
            "$addFields": {
                "items": {
                    "$map": {
                        "input": "$items",
                        "as": "item",
                        "in": {
                            "$mergeObjects": [
                                "$$item",
                                {
                                    "amount_running_total": {
                                        "$sum": {
                                            "$map": {
                                                "input": {
                                                    "$slice": [
                                                        "$items",
                                                        {
                                                            "$add": [
                                                                {
                                                                    "$indexOfArray": [
                                                                        "$items",
                                                                        "$$item",
                                                                    ]
                                                                },
                                                                1,
                                                            ]
                                                        },
                                                    ]
                                                },
                                                "as": "subitem",
                                                "in": "$$subitem.amount_signed",
                                            }
                                        }
                                    },
                                    "conv_running_total": {
                                        "hive": {
                                            "$sum": {
                                                "$map": {
                                                    "input": {
                                                        "$slice": [
                                                            "$items",
                                                            {
                                                                "$add": [
                                                                    {
                                                                        "$indexOfArray": [
                                                                            "$items",
                                                                            "$$item",
                                                                        ]
                                                                    },
                                                                    1,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    "as": "subitem",
                                                    "in": "$$subitem.conv_signed.hive",
                                                }
                                            }
                                        },
                                        "hbd": {
                                            "$sum": {
                                                "$map": {
                                                    "input": {
                                                        "$slice": [
                                                            "$items",
                                                            {
                                                                "$add": [
                                                                    {
                                                                        "$indexOfArray": [
                                                                            "$items",
                                                                            "$$item",
                                                                        ]
                                                                    },
                                                                    1,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    "as": "subitem",
                                                    "in": "$$subitem.conv_signed.hbd",
                                                }
                                            }
                                        },
                                        "usd": {
                                            "$sum": {
                                                "$map": {
                                                    "input": {
                                                        "$slice": [
                                                            "$items",
                                                            {
                                                                "$add": [
                                                                    {
                                                                        "$indexOfArray": [
                                                                            "$items",
                                                                            "$$item",
                                                                        ]
                                                                    },
                                                                    1,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    "as": "subitem",
                                                    "in": "$$subitem.conv_signed.usd",
                                                }
                                            }
                                        },
                                        "sats": {
                                            "$sum": {
                                                "$map": {
                                                    "input": {
                                                        "$slice": [
                                                            "$items",
                                                            {
                                                                "$add": [
                                                                    {
                                                                        "$indexOfArray": [
                                                                            "$items",
                                                                            "$$item",
                                                                        ]
                                                                    },
                                                                    1,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    "as": "subitem",
                                                    "in": "$$subitem.conv_signed.sats",
                                                }
                                            }
                                        },
                                        "msats": {
                                            "$sum": {
                                                "$map": {
                                                    "input": {
                                                        "$slice": [
                                                            "$items",
                                                            {
                                                                "$add": [
                                                                    {
                                                                        "$indexOfArray": [
                                                                            "$items",
                                                                            "$$item",
                                                                        ]
                                                                    },
                                                                    1,
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    "as": "subitem",
                                                    "in": "$$subitem.conv_signed.msats",
                                                }
                                            }
                                        },
                                    },
                                },
                            ]
                        },
                    }
                }
            }
        },
        {"$replaceRoot": {"newRoot": {"$arrayToObject": [[{"k": "$_id", "v": "$items"}]]}}},
    ]
    return pipeline
