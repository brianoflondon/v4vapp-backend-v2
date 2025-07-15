from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Sequence

from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerType


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


def filter_by_account_as_of_date_query(
    account: LedgerAccount | None = None,
    cust_id: str | None = None,
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
    ledger_types: list[LedgerType] | None = None,
    age: timedelta | None = None,
) -> Dict[str, Any]:
    """
    Generates a MongoDB query to filter documents by a specific account and date.

    This function creates a query that filters documents based on the provided
    account details (`name` and optionally `sub`) and ensures that the `timestamp`
    field is less than or equal to the specified `as_of_date`.

    If `account` is not given, returns all accounts.
    If `age` is provided, it filters documents within the specified age range

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

    query: Mapping[str, Any] = {"timestamp": date_range_query}

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

    return query


def filter_sum_credit_debit_pipeline(
    account: LedgerAccount | None = None,
    cust_id: str | None = None,
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
    ledger_types: list[LedgerType] | None = None,
    age: timedelta | None = None,
    line_items: bool = False,
) -> List[Mapping[str, Any]]:
    """
    Creates a MongoDB aggregation pipeline to filter ledger entries by account, date, ledger types, and age,
    then computes the sum of credit and debit amounts in various currencies.

    The pipeline uses MongoDB's $facet operator to run two parallel aggregation pipelines:

    by_ledger_type: Groups documents by their ledger_type field
    overall: Groups all matching documents together
    Each group calculates the same set of totals for credits and debits this is somewhat redundant as credis and
    debits should always be equal, but it allows for more granular analysis if needed.

    Args:
        account (LedgerAccount | None): The ledger account to filter by. If None, no account filtering is applied.
        as_of_date (datetime): The date up to which entries are considered. Defaults to the current UTC datetime.
        ledger_types (list[LedgerType] | None): List of ledger types to filter by. If None, all types are included.
        age (timedelta | None): Optional age filter for entries. If None, no age filtering is applied.
        line_items (bool): If True, includes line items in the output. Defaults to False.

    Returns:
        List[Mapping[str, Any]]: A MongoDB aggregation pipeline that filters and groups ledger entries,
        returning the total sums for credit and debit in HIVE, HBD, SATS, MSATS, and USD.
    """
    if isinstance(account, str):
        # If account is a string, convert it to a LedgerAccount object
        account = LedgerAccount.from_string(account)
    if not isinstance(age, timedelta):
        if isinstance(age, int | float):
            # If age is a number, convert it to a timedelta
            age = timedelta(seconds=age)

    query = filter_by_account_as_of_date_query(
        account=account,
        cust_id=cust_id,
        as_of_date=as_of_date,
        ledger_types=ledger_types,
        age=age,
    )

    facet_sections = {
        # Group by ledger_type
        "by_ledger_type": [
            {
                "$group": {
                    "_id": "$ledger_type",  # Group by ledger_type
                    # Sum fields from credit_conv
                    "credit_total_hive": {"$sum": "$credit_conv.hive"},
                    "credit_total_hbd": {"$sum": "$credit_conv.hbd"},
                    "credit_total_sats": {"$sum": "$credit_conv.sats"},
                    "credit_total_msats": {"$sum": "$credit_conv.msats"},
                    "credit_total_usd": {"$sum": "$credit_conv.usd"},
                    # Sum fields from debit_conv
                    "debit_total_hive": {"$sum": "$debit_conv.hive"},
                    "debit_total_hbd": {"$sum": "$debit_conv.hbd"},
                    "debit_total_sats": {"$sum": "$debit_conv.sats"},
                    "debit_total_msats": {"$sum": "$debit_conv.msats"},
                    "debit_total_usd": {"$sum": "$debit_conv.usd"},
                }
            }
        ],
        # Calculate overall totals across all matching documents
        "total": [
            {
                "$group": {
                    "_id": "total",  # Use "total" as identifier for overall group
                    # Sum fields from credit_conv
                    "credit_total_hive": {"$sum": "$credit_conv.hive"},
                    "credit_total_hbd": {"$sum": "$credit_conv.hbd"},
                    "credit_total_sats": {"$sum": "$credit_conv.sats"},
                    "credit_total_msats": {"$sum": "$credit_conv.msats"},
                    "credit_total_usd": {"$sum": "$credit_conv.usd"},
                    # Sum fields from debit_conv
                    "debit_total_hive": {"$sum": "$debit_conv.hive"},
                    "debit_total_hbd": {"$sum": "$debit_conv.hbd"},
                    "debit_total_sats": {"$sum": "$debit_conv.sats"},
                    "debit_total_msats": {"$sum": "$debit_conv.msats"},
                    "debit_total_usd": {"$sum": "$debit_conv.usd"},
                }
            }
        ],
    }
    if line_items:
        # If line_items is True, add a section to include detailed line items
        # This section will project the relevant fields for each ledger entry
        # and sort them by timestamp.
        facet_sections["line_items"] = [
            {"$project": {"_id": 0}},
            {"$sort": {"timestamp": 1}},
        ]

    pipeline: List[Mapping[str, Any]] = [
        {"$match": query},  # Apply the filtering criteria
        {"$facet": facet_sections},
        {"$sort": {"timestamp": 1}},  # Sort by timestamp
    ]

    return pipeline


def db_monitor_pipelines() -> Dict[str, Sequence[Mapping[str, Any]]]:
    # Can't find any way to filter this in the pipeline, will do it in code.
    pipeline_exclude_locked_changes: list[Mapping[str, Any]] = []

    payments_pipeline: Sequence[Mapping[str, Any]] = pipeline_exclude_locked_changes + [
        {"$match": {"operationType": {"$ne": "delete"}}},
        {
            "$match": {
                "fullDocument.custom_records.v4vapp_group_id": {"$ne": None},
                "fullDocument.status": {
                    "$in": ["FAILED", "SUCCEEDED"]
                },  # status must be FAILED or SUCCEEDED
            }
        },
        # {
        #     "$project": {
        #         "fullDocument.creation_date": 1,
        #         "fullDocument.payment_hash": 1,
        #         "fullDocument.status": 1,
        #         "fullDocument.value_msat": 1,
        #     }
        # },
    ]
    invoices_pipeline: Sequence[Mapping[str, Any]] = pipeline_exclude_locked_changes + [
        {"$match": {"operationType": {"$ne": "delete"}}},
        {"$match": {"fullDocument.state": "SETTLED"}},  # state must exist and be SETTLED
        # {
        #     "$project": {
        #         "fullDocument.creation_date": 1,
        #         "fullDocument.r_hash": 1,
        #         "fullDocument.state": 1,
        #         "fullDocument.amt_paid_msat": 1,
        #         "fullDocument.value_msat": 1,
        #         "fullDocument.memo": 1,
        #     }
        # },
    ]
    hive_ops_pipeline: Sequence[Mapping[str, Any]] = pipeline_exclude_locked_changes + [
        {"$match": {"operationType": {"$ne": "delete"}}},
        {
            "$match": {
                "fullDocument.type": {"$ne": "block_marker"},
            }
        },
    ]

    return {
        "payments": payments_pipeline,
        "invoices": invoices_pipeline,
        "hive_ops": hive_ops_pipeline,
    }
