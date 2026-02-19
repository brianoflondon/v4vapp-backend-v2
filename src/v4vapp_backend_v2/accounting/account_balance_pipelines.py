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


def list_all_ledger_types_pipeline() -> Sequence[Mapping[str, Any]]:
    """
    Returns a MongoDB aggregation pipeline to list all unique ledger types in the ledger.
    The pipeline performs the following operations:
    1. Groups the documents by `ledger_type` to find unique types.
    2. Projects the final output to include only the `ledger_type` field.
    3. Sorts the results by `ledger_type`.
    """
    pipeline: Sequence[Mapping[str, Any]] = [
        {
            "$group": {
                "_id": "$ledger_type",
            }
        },
        {
            "$project": {
                "_id": 0,
                "ledger_type": "$_id",
            }
        },
        {"$sort": {"ledger_type": 1}},
    ]
    return pipeline


def all_account_balances_pipeline(
    account: LedgerAccount | None = None,
    account_name: str | None = None,
    sub: str | None = None,
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    age: timedelta | None = None,
    filter: Mapping[str, Any] | None = None,
    cust_ids: Sequence[str] | None = None,
) -> Sequence[Mapping[str, Any]]:
    """
    Generates a MongoDB aggregation pipeline to retrieve the balances of all accounts in the ledger.
    Notes:
    - The pipeline can be filtered by specific account details (account object, account name, or sub).
    - The pipeline considers transactions up to a specified date (`as_of_date`) and can be limited to a certain age (time window) if `age` is provided.
    - The resulting documents include running totals for amounts and conversions in various currencies, grouped by account and unit.
    The order of precedence for filtering is: `account` > `account_name` > `sub`. If none are provided, the pipeline will include all accounts.

    Args:
        account (LedgerAccount, optional): An instance of LedgerAccount to filter the transactions. If provided, the pipeline will match transactions for this specific account.
        account_name (str, optional): The name of the account to filter transactions. Used if `account` is not provided.
        sub (str, optional): The sub identifier to filter transactions
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
    filter = filter or {}
    if account:
        debit_match_query: dict[str, Any] = {
            "debit.name": account.name,
            "debit.sub": account.sub,
            "debit.account_type": account.account_type,
        }
        credit_match_query: dict[str, Any] = {
            "credit.name": account.name,
            "credit.sub": account.sub,
            "credit.account_type": account.account_type,
        }
    elif account_name:
        debit_match_query = {"debit.name": account_name}
        credit_match_query = {"credit.name": account_name}
    elif sub:
        debit_match_query = {"debit.sub": sub}
        credit_match_query = {"credit.sub": sub}
    else:
        debit_match_query = {}
        credit_match_query = {}

    # When cust_ids is provided, restrict the facet matches to only those subs.
    # This is the key optimisation: it ensures the expensive per-account
    # aggregation inside the facet only processes the requested accounts.
    if cust_ids is not None:
        debit_match_query["debit.sub"] = {"$in": list(cust_ids)}
        credit_match_query["credit.sub"] = {"$in": list(cust_ids)}

    facet_debit_match = {"$match": debit_match_query}
    facet_credit_match = {"$match": credit_match_query}

    if age:
        start_date = as_of_date - age
        date_range_query = {"$gte": start_date, "$lte": as_of_date}
    else:
        date_range_query = {"$lte": as_of_date}

    pipeline: Sequence[Mapping[str, Any]] = [
        {"$match": {"cust_id": {"$in": cust_ids}}} if cust_ids is not None else {"$match": {}},
        {"$match": {"timestamp": date_range_query, "conv_signed": {"$exists": True}}},
        {"$match": filter},
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
                            "link": 1,
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
                            "link": 1,
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


def all_held_msats_balance_pipeline(cust_id: str = "") -> Sequence[Mapping[str, Any]]:
    """
    Generates a MongoDB aggregation pipeline to calculate the net held balances for all customers.

    The net held balance for each customer is computed as the sum of debit_amount for 'hold_k' ledger types
    minus the sum of debit_amount for 'release_k' ledger types. This represents each customer's
    net held amount (e.g., positive means more held, negative means over-released).

    This is an all customers pipeline and quite expensive.

    Returns:
        List[Mapping[str, Any]]: The aggregation pipeline as a list of stages.
    """
    if cust_id:
        match_stage = {
            "$match": {"cust_id": cust_id, "ledger_type": {"$in": ["hold_k", "release_k"]}}
        }
    else:
        match_stage = {"$match": {"ledger_type": {"$in": ["hold_k", "release_k"]}}}

    pipeline: Sequence[Mapping[str, Any]] = [
        match_stage,
        {
            "$group": {
                "_id": "$cust_id",
                "hold_total": {
                    "$sum": {"$cond": [{"$eq": ["$ledger_type", "hold_k"]}, "$debit_amount", 0]}
                },
                "release_total": {
                    "$sum": {"$cond": [{"$eq": ["$ledger_type", "release_k"]}, "$debit_amount", 0]}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "cust_id": "$_id",
                "hold_total": 1,
                "release_total": 1,
                "net_held": {"$subtract": ["$hold_total", "$release_total"]},
            }
        },
        {"$sort": {"net_held": -1}},
    ]
    return pipeline


def active_account_subs_pipeline(
    account_name: str,
    min_transactions: int = 2,
) -> Sequence[Mapping[str, Any]]:
    """
    Returns a lightweight MongoDB aggregation pipeline that identifies account subs
    with at least `min_transactions` ledger entries for the given account name.

    This is much cheaper than running the full balance aggregation and can be used
    to pre-filter which accounts need the expensive balance calculation.

    Args:
        account_name: The account name to filter by (e.g. "VSC Liability").
        min_transactions: Minimum number of transactions to consider an account active.
            Defaults to 2 (accounts with only 1 entry are typically setup-only).

    Returns:
        Sequence[Mapping[str, Any]]: A MongoDB aggregation pipeline that returns
            documents with 'sub' and 'transaction_count' fields.
    """
    pipeline: Sequence[Mapping[str, Any]] = [
        # Project both debit and credit accounts into an array
        {
            "$project": {
                "accounts": [
                    {"name": "$debit.name", "sub": "$debit.sub"},
                    {"name": "$credit.name", "sub": "$credit.sub"},
                ]
            }
        },
        {"$unwind": "$accounts"},
        # Filter for the specific account name
        {"$match": {"accounts.name": account_name}},
        # Group by sub and count transactions
        {
            "$group": {
                "_id": "$accounts.sub",
                "transaction_count": {"$sum": 1},
            }
        },
        # Filter for minimum transactions
        {"$match": {"transaction_count": {"$gte": min_transactions}}},
        # Project just the sub and count
        {
            "$project": {
                "_id": 0,
                "sub": "$_id",
                "transaction_count": 1,
            }
        },
        {"$sort": {"sub": 1}},
    ]
    return pipeline
