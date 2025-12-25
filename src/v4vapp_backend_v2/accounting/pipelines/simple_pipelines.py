from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Sequence

from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.hive.v4v_config import V4VConfig, V4VConfigRateLimits


def filter_by_account_as_of_date_query(
    account: LedgerAccount | None = None,
    cust_id: str | None = None,
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    ledger_types: list[LedgerType] | None = None,
    group_id: str | None = None,
    short_id: str | None = None,
    sub_account: str | None = None,
    age: timedelta | None = None,
) -> Mapping[str, Any]:
    """
    Generates a MongoDB query to filter documents by a specific account and date.

    This function creates a query that filters documents based on the provided
    account details (`name` and optionally `sub`) and ensures that the `timestamp`
    field is less than or equal to the specified `as_of_date`.

    If `account` is not given, returns all accounts.
    If `age` is provided, it filters documents within the specified age range
    ending at `as_of_date`.
    if `cust_id` is provided, it adds a condition to filter by the given customer ID.
    if `ledger_types` is provided, it adds a condition to filter by the given ledger
    types.
    if `group_id` is provided, it adds a condition to filter by a regex matching the given group ID.
    if `short_id` is provided, it adds a condition to filter by a regex matching the given short ID.

    Args:
        account (Account): The account object containing `name` and optionally `sub`
            to filter the documents.
        as_of_date (datetime, optional): The cutoff date for filtering documents.
            Defaults to the current datetime in UTC.

    Returns:
        Dict[str, Any]: A dictionary representing the MongoDB query.
    """
    if age:
        start_date = as_of_date - age
        date_range_query = {"$gte": start_date, "$lte": as_of_date}
    else:
        date_range_query = {"$lte": as_of_date}

    query: Dict[str, Any] = {"timestamp": date_range_query}

    if account:
        query["$or"] = [
            {
                "debit.name": account.name,
                "debit.sub": account.sub if account.sub else "",
            },
            {
                "credit.name": account.name,
                "credit.sub": account.sub if account.sub else "",
            },
        ]
    elif sub_account:
        # Allow filtering by sub-account alone (do not require full account selection)
        query["$or"] = [
            {"debit.sub": sub_account},
            {"credit.sub": sub_account},
        ]

    # Add cust_id condition if provided
    if cust_id:
        query["cust_id"] = cust_id

    # Add ledger_types condition if provided and not empty
    if ledger_types:
        # If there's only one type, use simple equality
        if len(ledger_types) == 1:
            query["ledger_type"] = ledger_types[0]
        # If multiple types, use $in operator
        else:
            query["ledger_type"] = {"$in": ledger_types}

    if group_id:
        query["group_id"] = {"$regex": group_id}

    if short_id:
        query["short_id"] = {"$regex": short_id}

    return query


# Modify the limit_check_pipeline function to include cust_id in the output
def limit_check_pipeline(
    cust_id: str,
    extra_spend_sats: Decimal = Decimal(0),
    lightning_rate_limits: List[V4VConfigRateLimits] | None = None,
    details: bool = False,
) -> List[Mapping[str, Any]]:
    if lightning_rate_limits is None:
        lightning_rate_limits = V4VConfig().data.lightning_rate_limits

    # Ensure the list is sorted by hours ascending
    V4VConfig().data.check_and_sort_rate_limits()

    max_hours = V4VConfig().data.max_rate_limit_hours
    start_date = datetime.now(tz=timezone.utc) - timedelta(hours=max_hours)

    top_level_match = {
        "ledger_type": {"$in": ["h_conv_k", "k_conv_h"]},
        "cust_id": cust_id,
        "timestamp": {"$gte": start_date},
    }

    # Dynamically generate facet sections
    facet_dict = {}
    for i, limit in enumerate(lightning_rate_limits):
        facet_name = f"{limit.hours}"
        group_stage = {
            "_id": None,
            "totalCreditConvSumMSATS": {"$sum": "$credit_conv.msats"},
            "totalCreditConvSumSATS": {"$sum": "$credit_conv.sats"},
            "totalCreditConvSumUSD": {"$sum": "$credit_conv.usd"},
            "totalCreditConvSumHIVE": {"$sum": "$credit_conv.hive"},
            "totalCreditConvSumHBD": {"$sum": "$credit_conv.hbd"},
        }
        if details:
            group_stage["details"] = {"$push": "$$ROOT"}

        if i < len(lightning_rate_limits) - 1:
            # Add $match for time range
            facet_dict[facet_name] = [
                {
                    "$match": {
                        "$expr": {
                            "$gte": [
                                "$timestamp",
                                {
                                    "$dateSubtract": {
                                        "startDate": "$$NOW",
                                        "unit": "hour",
                                        "amount": limit.hours,
                                    }
                                },
                            ]
                        }
                    }
                },
                {"$group": group_stage},
            ]
        else:
            # No $match for the last (longest) period
            facet_dict[facet_name] = [{"$group": group_stage}]

    extra_spend_sats_str = str(extra_spend_sats)

    # Generate the pipeline dynamically
    pipeline: List[Mapping[str, Any]] = [
        {"$match": top_level_match},
        {"$facet": facet_dict},
        {"$project": {name: {"$first": f"${name}"} for name in facet_dict}},
        {
            "$project": {
                "cust_id": cust_id,
                "periods": {
                    f"{limit.hours}": {
                        "msats": {"$ifNull": [f"${f'{limit.hours}'}.totalCreditConvSumMSATS", 0]},
                        "sats": {"$ifNull": [f"${f'{limit.hours}'}.totalCreditConvSumSATS", 0]},
                        "usd": {"$ifNull": [f"${f'{limit.hours}'}.totalCreditConvSumUSD", 0]},
                        "hive": {"$ifNull": [f"${f'{limit.hours}'}.totalCreditConvSumHIVE", 0]},
                        "hbd": {"$ifNull": [f"${f'{limit.hours}'}.totalCreditConvSumHBD", 0]},
                        "limit_hours": f"{limit.hours}",
                        "limit_sats": f"{limit.sats}",
                        "limit_ok": {
                            "$lt": [{"$add": ["$sats", extra_spend_sats_str]}, str(limit.sats)]
                        },
                        **(
                            {"details": {"$ifNull": [f"${f'{limit.hours}'}.details", []]}}
                            if details
                            else {}
                        ),
                    }
                    for limit in lightning_rate_limits
                },
            }
        },
    ]
    return pipeline


# Fields that, when updated (and ONLY these), should cause the change stream event to be ignored
IGNORED_UPDATE_FIELDS = [
    "replies",
    "change_conv",
    "change_memo",
    "change_amount",
    "process_time",
    "locked",
    "extensions",
    "fee_conv",
    "json",
    # Add newly observed benign fields:
    "conv",  # main conversion recalculation
    "description_hash",  # memo hash / dedupe
    "fallback_addr",  # LN invoice fallback
    "features",  # LN invoice feature bits
    "is_amp",
    "is_keysend",
]


def db_monitor_pipelines() -> Dict[str, Sequence[Mapping[str, Any]]]:
    """
    Generates MongoDB aggregation pipelines for monitoring database changes in payments, invoices, and hive operations collections.

    Returns:
        Dict[str, Sequence[Mapping[str, Any]]]:
            A dictionary containing named pipelines:
                - "payments": Pipeline to monitor payment documents, excluding deletes and filtering by group ID and status.
                - "invoices": Pipeline to monitor invoice documents, excluding deletes and filtering for settled state.
                - "hive_ops": Pipeline to monitor hive operation documents, excluding deletes and block marker types.
            Each pipeline also ignores certain update operations that only affect specified fields ("replies", "change_conv", "process_time").
    """

    ignore_updates_match: Mapping[str, Any] = {
        "$match": {
            "$or": [
                {"operationType": {"$ne": "update"}},
                {
                    "operationType": "update",
                    "$expr": {
                        # Keep ONLY updates that changed at least one field outside the ignore list
                        "$gt": [
                            {
                                "$size": {
                                    "$setDifference": [
                                        {
                                            "$map": {
                                                "input": {
                                                    "$objectToArray": "$updateDescription.updatedFields"
                                                },
                                                "as": "field",
                                                "in": "$$field.k",
                                            }
                                        },
                                        IGNORED_UPDATE_FIELDS,
                                    ]
                                }
                            },
                            0,
                        ]
                    },
                },
            ]
        }
    }

    payments_pipeline: Sequence[Mapping[str, Any]] = [
        {
            "$match": {
                "operationType": {"$ne": "delete"},
                "fullDocument.custom_records.v4vapp_group_id": {"$ne": None},
                "fullDocument.status": {"$in": ["FAILED", "SUCCEEDED"]},
                # Only process completion updates
                "$or": [
                    {"operationType": {"$ne": "update"}},
                    {
                        "operationType": "update",
                        "updateDescription.updatedFields.status": {"$exists": True},
                    },
                ],
            }
        },
        ignore_updates_match,
    ]
    invoices_pipeline: Sequence[Mapping[str, Any]] = [
        {
            "$match": {
                "operationType": {"$ne": "delete"},
                "fullDocument.state": "SETTLED",
            }
        },
        ignore_updates_match,
    ]
    hive_ops_pipeline: Sequence[Mapping[str, Any]] = [
        {
            "$match": {
                "operationType": {"$ne": "delete"},
                "fullDocument.type": {"$nin": ["block_marker"]},
            }
        },
        ignore_updates_match,
    ]
    htlc_events_pipeline: Sequence[Mapping[str, Any]] = [
        {
            "$match": {
                "operationType": {"$ne": "delete"},
                "fullDocument.group_id": {"$ne": None},
            }
        },
        ignore_updates_match,
    ]

    return {
        "payments": payments_pipeline,
        "invoices": invoices_pipeline,
        "hive_ops": hive_ops_pipeline,
        "htlc_events": htlc_events_pipeline,
    }
