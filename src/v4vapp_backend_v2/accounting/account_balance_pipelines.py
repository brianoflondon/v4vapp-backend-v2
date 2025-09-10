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



def all_account_balances_pipeline(
    account: LedgerAccount | None = None,
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    age: timedelta | None = None,
) -> Sequence[Mapping[str, Any]]:
    """
    Generates a MongoDB aggregation pipeline to retrieve the balances of all accounts in the ledger.

    Args:
        as_of_date (datetime, optional): The end date for the balance calculation. Defaults to the current UTC datetime.
        age (timedelta | None, optional): If provided, limits the results to transactions within the specified age (time window) ending at `as_of_date`.

    Returns:
        Sequence[Mapping[str, Any]]: A MongoDB aggregation pipeline that:
            - Filters transactions by date and existence of signed conversion values.
            - Separates debit and credit transactions for all accounts.
            - Projects relevant fields for each transaction, including amounts and conversion values.
            - Combines debit and credit transactions into a unified list.
            - Sorts transactions chronologically.
            - Groups transactions by unit (currency).
            - Calculates running totals for amounts and conversions in multiple currencies.
            - Structures the output as a mapping from unit to a list of transaction details with running totals.

    Note:
        The pipeline is intended for use with MongoDB aggregation queries and assumes the presence of specific fields in the transaction documents.

    """
    if account:
        facet_debit_match = {
            "$match": {
                "debit.name": account.name,
                "debit.sub": account.sub,
                "debit.account_type": account.account_type,
            }
        }
        facet_credit_match = {
            "$match": {
                "credit.name": account.name,
                "credit.sub": account.sub,
                "credit.account_type": account.account_type,
            }
        }
    else:
        facet_debit_match = {"$match": {}}
        facet_credit_match = {"$match": {}}

    if age:
        start_date = as_of_date - age
        date_range_query = {"$gte": start_date, "$lte": as_of_date}
    else:
        date_range_query = {"$lte": as_of_date}

    pipeline: Sequence[Mapping[str, Any]] = [
        {"$match": {"timestamp": date_range_query, "conv_signed": {"$exists": True}}},
        {
            "$facet": {
                "debits_view": [
                    facet_debit_match,
                    {
                        "$project": {
                            "_id": 0,
                            "account_type": "$debit.account_type",
                            "name": "$debit.name",
                            "sub": "$debit.sub",
                            "contra": "$debit.contra",
                            "group_id": 1,
                            "short_id": 1,
                            "ledger_type": 1,
                            "timestamp": 1,
                            "description": 1,
                            "user_memo": 1,
                            "cust_id": 1,
                            "amount": "$debit_amount",
                            "amount_signed": "$debit_amount_signed",
                            "unit": "$debit_unit",
                            "conv": "$debit_conv",
                            "conv_signed": "$conv_signed.debit",
                            "op_type": 1,
                            "side": "debit",
                        }
                    },
                ],
                "credits_view": [
                    facet_credit_match,
                    {
                        "$project": {
                            "_id": 0,
                            "account_type": "$credit.account_type",
                            "name": "$credit.name",
                            "sub": "$credit.sub",
                            "contra": "$credit.contra",
                            "group_id": 1,
                            "short_id": 1,
                            "ledger_type": 1,
                            "timestamp": 1,
                            "description": 1,
                            "user_memo": 1,
                            "cust_id": 1,
                            "amount": "$credit_amount",
                            "amount_signed": "$credit_amount_signed",
                            "unit": "$credit_unit",
                            "conv": "$credit_conv",
                            "conv_signed": "$conv_signed.credit",
                            "op_type": 1,
                            "side": "credit",
                        }
                    },
                ],
            }
        },
        {"$project": {"combined": {"$concatArrays": ["$debits_view", "$credits_view"]}}},
        {"$unwind": "$combined"},
        {"$replaceRoot": {"newRoot": "$combined"}},
        {
            "$group": {
                "_id": {
                    "account_type": "$account_type",
                    "name": "$name",
                    "sub": "$sub",
                    "contra": "$contra",
                },
                "items": {"$push": "$$ROOT"},
            }
        },
        {"$project": {"items": {"$sortArray": {"input": "$items", "sortBy": {"timestamp": 1}}}}},
        {
            "$project": {
                "unit_groups": {
                    "$map": {
                        "input": {"$setUnion": ["$items.unit"]},
                        "as": "unit",
                        "in": {
                            "k": "$$unit",
                            "v": {
                                "$map": {
                                    "input": {
                                        "$filter": {
                                            "input": "$items",
                                            "as": "item",
                                            "cond": {"$eq": ["$$item.unit", "$$unit"]},
                                        }
                                    },
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
                                                                    {
                                                                        "$filter": {
                                                                            "input": "$items",
                                                                            "as": "subitem",
                                                                            "cond": {
                                                                                "$eq": [
                                                                                    "$$subitem.unit",
                                                                                    "$$unit",
                                                                                ]
                                                                            },
                                                                        }
                                                                    },
                                                                    {
                                                                        "$add": [
                                                                            {
                                                                                "$indexOfArray": [
                                                                                    {
                                                                                        "$filter": {
                                                                                            "input": "$items",
                                                                                            "as": "subitem",
                                                                                            "cond": {
                                                                                                "$eq": [
                                                                                                    "$$subitem.unit",
                                                                                                    "$$unit",
                                                                                                ]
                                                                                            },
                                                                                        }
                                                                                    },
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
                                                                        {
                                                                            "$filter": {
                                                                                "input": "$items",
                                                                                "as": "subitem",
                                                                                "cond": {
                                                                                    "$eq": [
                                                                                        "$$subitem.unit",
                                                                                        "$$unit",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                        {
                                                                            "$add": [
                                                                                {
                                                                                    "$indexOfArray": [
                                                                                        {
                                                                                            "$filter": {
                                                                                                "input": "$items",
                                                                                                "as": "subitem",
                                                                                                "cond": {
                                                                                                    "$eq": [
                                                                                                        "$$subitem.unit",
                                                                                                        "$$unit",
                                                                                                    ]
                                                                                                },
                                                                                            }
                                                                                        },
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
                                                                        {
                                                                            "$filter": {
                                                                                "input": "$items",
                                                                                "as": "subitem",
                                                                                "cond": {
                                                                                    "$eq": [
                                                                                        "$$subitem.unit",
                                                                                        "$$unit",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                        {
                                                                            "$add": [
                                                                                {
                                                                                    "$indexOfArray": [
                                                                                        {
                                                                                            "$filter": {
                                                                                                "input": "$items",
                                                                                                "as": "subitem",
                                                                                                "cond": {
                                                                                                    "$eq": [
                                                                                                        "$$subitem.unit",
                                                                                                        "$$unit",
                                                                                                    ]
                                                                                                },
                                                                                            }
                                                                                        },
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
                                                                        {
                                                                            "$filter": {
                                                                                "input": "$items",
                                                                                "as": "subitem",
                                                                                "cond": {
                                                                                    "$eq": [
                                                                                        "$$subitem.unit",
                                                                                        "$$unit",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                        {
                                                                            "$add": [
                                                                                {
                                                                                    "$indexOfArray": [
                                                                                        {
                                                                                            "$filter": {
                                                                                                "input": "$items",
                                                                                                "as": "subitem",
                                                                                                "cond": {
                                                                                                    "$eq": [
                                                                                                        "$$subitem.unit",
                                                                                                        "$$unit",
                                                                                                    ]
                                                                                                },
                                                                                            }
                                                                                        },
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
                                                                        {
                                                                            "$filter": {
                                                                                "input": "$items",
                                                                                "as": "subitem",
                                                                                "cond": {
                                                                                    "$eq": [
                                                                                        "$$subitem.unit",
                                                                                        "$$unit",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                        {
                                                                            "$add": [
                                                                                {
                                                                                    "$indexOfArray": [
                                                                                        {
                                                                                            "$filter": {
                                                                                                "input": "$items",
                                                                                                "as": "subitem",
                                                                                                "cond": {
                                                                                                    "$eq": [
                                                                                                        "$$subitem.unit",
                                                                                                        "$$unit",
                                                                                                    ]
                                                                                                },
                                                                                            }
                                                                                        },
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
                                                                        {
                                                                            "$filter": {
                                                                                "input": "$items",
                                                                                "as": "subitem",
                                                                                "cond": {
                                                                                    "$eq": [
                                                                                        "$$subitem.unit",
                                                                                        "$$unit",
                                                                                    ]
                                                                                },
                                                                            }
                                                                        },
                                                                        {
                                                                            "$add": [
                                                                                {
                                                                                    "$indexOfArray": [
                                                                                        {
                                                                                            "$filter": {
                                                                                                "input": "$items",
                                                                                                "as": "subitem",
                                                                                                "cond": {
                                                                                                    "$eq": [
                                                                                                        "$$subitem.unit",
                                                                                                        "$$unit",
                                                                                                    ]
                                                                                                },
                                                                                            }
                                                                                        },
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
                            },
                        },
                    }
                },
            }
        },
        {"$project": {"unit_groups": {"$arrayToObject": "$unit_groups"}}},
        {
            "$project": {
                "_id": 0,
                "account_type": "$_id.account_type",
                "name": "$_id.name",
                "sub": "$_id.sub",
                "contra": "$_id.contra",
                "balances": "$unit_groups",
            }
        },
        {"$sort": {"account_type": 1, "name": 1, "sub": 1}},
    ]

    return pipeline
