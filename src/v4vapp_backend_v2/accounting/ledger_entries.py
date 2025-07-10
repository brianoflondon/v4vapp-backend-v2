from datetime import datetime, timezone
from typing import Any, Dict, Mapping

import pandas as pd

from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import (
    filter_by_account_as_of_date_query,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger


async def get_ledger_entries(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "",
    filter_by_account: LedgerAccount | None = None,
    cust_id: str | None = None,
    filter_by_ledger_types: list[LedgerType] | None = None,
) -> list[LedgerEntry]:
    """
    Retrieves ledger entries from the database up to a specified date, optionally filtered by account.

    Args:
        as_of_date (datetime, optional): The cutoff date for retrieving ledger entries.
            Defaults to the current UTC datetime.
        collection_name (str, optional): The name of the database collection to query.
            Defaults to "ledger".
        filter_by_account (Account | None, optional): An Account object to filter entries by.
            If provided, only entries where the account matches either the debit or credit side
            (considering both name and sub-account) are returned. Defaults to None.

    Returns:
        list[LedgerEntry]: A list of LedgerEntry objects sorted by timestamp (ascending).

    Notes:
        - Queries the database for entries with a timestamp less than or equal to `as_of_date`.
        - Sorts results by timestamp in ascending order (earliest to latest).
        - If filter_by_account is provided, matches entries where either the debit or credit side
          corresponds to the specified account name and sub-account.
    """
    collection_name = LedgerEntry.collection() if not collection_name else collection_name
    query  = filter_by_account_as_of_date_query(
        account=filter_by_account,
        cust_id=cust_id,
        as_of_date=as_of_date,
        ledger_types=filter_by_ledger_types,
    )
    ledger_entries = []
    if not TrackedBaseModel.db_client:
        logger.error(
            "Database client is not initialized. Cannot fetch ledger entries.",
            extra={
                "notification": False,
                "as_of_date": as_of_date,
                "collection_name": collection_name,
            },
        )
        return ledger_entries
    db_client = TrackedBaseModel.db_client
    cursor = await db_client.find(
        collection_name=collection_name,
        query=query,
        sort=[("timestamp", 1)],
    )
    async for entry in cursor:
        try:
            ledger_entry = LedgerEntry.model_validate(entry)
            ledger_entries.append(ledger_entry)
        except Exception as e:
            logger.error(
                f"Error validating ledger entry: {entry}. Error: {e}",
                extra={"notification": False, "entry": entry, "error": str(e)},
            )
            continue
    return ledger_entries


async def get_ledger_dataframe(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "",
    filter_by_account: LedgerAccount | None = None,
) -> pd.DataFrame:
    """
    Fetches ledger entries from the database as of a specified date and returns them as a pandas DataFrame.

    Args:
        as_of_date (datetime, optional): The cutoff date for fetching ledger entries. Defaults to the current UTC datetime.
        collection_name (str, optional): The name of the database collection to query. Defaults to "ledger".
        filter_by_account (Account | None, optional): The account to filter by. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing ledger entry data with the following columns:
            - timestamp: The timestamp of the ledger entry.
            - group_id: The group ID associated with the ledger entry.
            - short_id: A short identifier for the ledger entry.
            - description: A description of the ledger entry.
            - debit_amount: The amount of the debit transaction.
            - debit_unit: The unit of the debit amount.
            - credit_amount: The amount of the credit transaction.
            - credit_unit: The unit of the credit amount.
            - debit_conv_sats, debit_conv_msats, debit_conv_hive, debit_conv_hbd, debit_conv_usd: Converted values for debit.
            - credit_conv_sats, credit_conv_msats, credit_conv_hive, credit_conv_hbd, credit_conv_usd: Converted values for credit.
            - debit_name: The name of the debit account.
            - debit_account_type: The type of the debit account.
            - debit_sub: The sub-account of the debit account.
            - credit_name: The name of the credit account.
            - credit_account_type: The type of the credit account.
            - credit_sub: The sub-account of the credit account.
    """
    collection_name = LedgerEntry.collection() if not collection_name else collection_name
    ledger_entries = await get_ledger_entries(
        as_of_date=as_of_date, collection_name=collection_name, filter_by_account=filter_by_account
    )
    data = []
    for entry in ledger_entries:
        if entry.debit and entry.credit:
            debit_modifier = -1 if entry.debit.contra else 1
            credit_modifier = -1 if entry.credit.contra else 1
            debit_modifier = 1
            credit_modifier = 1

            debit_amount = debit_modifier * entry.debit_amount
            debit_unit = entry.debit_unit.value if entry.debit_unit else None
            debit_conv = debit_modifier * entry.debit_conv
            credit_amount = credit_modifier * entry.credit_amount
            credit_unit = entry.credit_unit.value if entry.credit_unit else None
            credit_conv = credit_modifier * entry.credit_conv

            data.append(
                {
                    "timestamp": entry.timestamp,
                    "group_id": entry.group_id,
                    "short_id": entry.short_id,
                    "description": entry.description,
                    "ledger_type": entry.ledger_type,
                    "debit_amount": debit_amount,
                    "debit_unit": debit_unit,
                    "debit_conv_sats": debit_conv.sats,
                    "debit_conv_msats": debit_conv.msats,
                    "debit_conv_hive": debit_conv.hive,
                    "debit_conv_hbd": debit_conv.hbd,
                    "debit_conv_usd": debit_conv.usd,
                    "credit_amount": credit_amount,
                    "credit_unit": credit_unit,
                    "credit_conv_sats": credit_conv.sats,
                    "credit_conv_msats": credit_conv.msats,
                    "credit_conv_hive": credit_conv.hive,
                    "credit_conv_hbd": credit_conv.hbd,
                    "credit_conv_usd": credit_conv.usd,
                    "debit_name": entry.debit.name,
                    "debit_account_type": entry.debit.account_type,
                    "debit_sub": entry.debit.sub,
                    "debit_contra": entry.debit.contra,
                    "credit_name": entry.credit.name,
                    "credit_account_type": entry.credit.account_type,
                    "credit_sub": entry.credit.sub,
                    "credit_contra": entry.credit.contra,
                }
            )

    df = pd.DataFrame(data)
    return df
