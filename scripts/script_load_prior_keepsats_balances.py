from asyncio import TaskGroup
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import List

import aiofiles
from bson import json_util
from mongomock import DuplicateKeyError
from pydantic import BaseModel, Field, validator

from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.bad_actors_list import get_bad_hive_accounts
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency


class KeepsatsBalance(BaseModel):
    """
    Pydantic model for a single Keepsats balance record.

    This model validates and holds the data for each balance entry from the MongoDB export JSON.
    Fields are automatically converted to appropriate Python types (e.g., datetime for timestamps,
    int for large numbers).
    """

    id: str = Field(alias="_id", description="Unique identifier for the account")
    last_timestamp: datetime = Field(..., description="Last update timestamp")
    net_msats: int = Field(..., description="Net millisatoshis balance")
    net_hive: float = Field(..., description="Net Hive balance")
    net_usd: float = Field(..., description="Net USD balance")
    net_sats: int = Field(..., description="Net satoshis balance")
    rate_usd_btc: float = Field(..., description="USD to BTC exchange rate")

    @validator("last_timestamp", pre=True, always=True)
    def ensure_timestamp_tz_aware(cls, v):
        """
        Ensure the timestamp is timezone-aware and normalized to UTC.

        Accepts either a datetime or an ISO-format string. If the datetime is naive,
        it will be set to UTC. If it has a timezone, it will be converted to UTC.
        """
        if isinstance(v, str):
            try:
                dt = datetime.fromisoformat(v)
            except Exception:
                raise ValueError("Invalid datetime string for last_timestamp")
        elif isinstance(v, datetime):
            dt = v
        else:
            raise ValueError("last_timestamp must be a datetime or ISO-format string")

        # If naive, assume UTC. Otherwise, convert to UTC.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt


def generate_hive_trx_id(balance: KeepsatsBalance) -> str:
    """
    Generates a Hive-like transaction ID by hashing the KeepsatsBalance data.

    This function creates a SHA-256 hash of key fields from the balance record,
    producing a 64-character hexadecimal string that resembles a Hive transaction ID.

    Args:
        balance (KeepsatsBalance): The balance record to hash.

    Returns:
        str: A 64-character hexadecimal string representing the transaction ID.
    """
    # Serialize key fields into a string for hashing
    data_to_hash = f"{balance.id}{balance.last_timestamp.isoformat()}{balance.net_msats}{balance.net_hive}{balance.net_usd}{balance.net_sats}{balance.rate_usd_btc}"
    hash_object = sha256(data_to_hash.encode("utf-8"))
    return hash_object.hexdigest()


async def load_keepsats_balances(file_path: Path) -> List[KeepsatsBalance]:
    """
    Asynchronously loads and parses the Keepsats balances data from a MongoDB export JSON file.

    This function reads the JSON file, which contains BSON-specific types like $date and $numberLong,
    and uses bson.json_util.loads to properly decode them into Python types (e.g., datetime objects
    for dates and int for large numbers). It then validates and converts each record into a
    KeepsatsBalance Pydantic model.

    Args:
        file_path (Path): The path to the JSON file containing the balances data.

    Returns:
        List[KeepsatsBalance]: A list of KeepsatsBalance models representing the loaded balances data.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        json.JSONDecodeError: If the file content is not valid JSON.
        ValueError: If the parsed data is not a list.
        pydantic.ValidationError: If any record fails validation.
    """
    async with aiofiles.open(file_path, "r") as file:
        raw_content = await file.read()

    # Parse the JSON using bson.json_util.loads to handle MongoDB-specific types
    data = json_util.loads(raw_content)

    if not isinstance(data, list):
        raise ValueError("Expected the JSON file to contain a list of balances.")

    # Validate and convert each record to KeepsatsBalance model
    balances = [KeepsatsBalance(**record) for record in data]
    return balances


async def create_ledger_entry(balance: KeepsatsBalance, from_account: str = "") -> LedgerEntry:
    """
    Placeholder for future implementation of ledger entry creation.

    This function is intended to create a ledger entry based on the loaded Keepsats balances.
    The implementation details will depend on the specific requirements and data structure
    needed for the ledger entries.
    """
    if not from_account:
        from_account = "OpeningBalance"

    to_account = balance.id

    # This doesn't seem to make a material difference for opening balances
    # quote = await TrackedBaseModel.nearest_quote(balance.last_timestamp)
    quote = TrackedBaseModel.last_quote

    conv = CryptoConversion(
        conv_from=Currency.MSATS, value=balance.net_msats, quote=quote
    ).conversion

    memo = f"Opening Balance for {to_account} last transaction {balance.last_timestamp} "
    group_id = generate_hive_trx_id(balance)
    short_id = f"0000-{group_id[:6]}"

    ledger_type = LedgerType.OPENING_BALANCE
    transfer_ledger_entry = LedgerEntry(
        cust_id=to_account,
        short_id=short_id,
        ledger_type=ledger_type,
        group_id=f"{group_id}_{ledger_type.value}",
        user_memo=memo,
        timestamp=balance.last_timestamp,
        description=memo,
        op_type="custom_json",
        debit=LiabilityAccount(name="VSC Liability", sub=from_account),
        debit_conv=conv,
        debit_amount=balance.net_msats,
        debit_unit=Currency.MSATS,
        credit=LiabilityAccount(name="VSC Liability", sub=to_account),
        credit_conv=conv,
        credit_unit=Currency.MSATS,
        credit_amount=balance.net_msats,
    )
    try:
        await transfer_ledger_entry.save(ignore_duplicates=True, upsert=True)
    except DuplicateKeyError:
        print("Duplicate entry, likely already exists.")

    except Exception as e:
        print(f"Error saving ledger entry: {e}")
    return transfer_ledger_entry


async def owners_loan_account(amount_sats: Decimal) -> LedgerEntry:
    """
    Returns the LiabilityAccount instance for the Owner's Loan account.

    This function provides a standardized way to reference the Owner's Loan account
    in ledger entries and other accounting operations.

    Returns:
        LiabilityAccount: The LiabilityAccount instance for the Owner's Loan account.
    """
    ledger_type = LedgerType.OPENING_BALANCE
    quote = TrackedBaseModel.last_quote
    conv = CryptoConversion(conv_from=Currency.SATS, value=amount_sats, quote=quote).conversion
    owner_loan_entry = LedgerEntry(
        cust_id="OpeningBalance",
        short_id="0000-OWNLOAN",
        ledger_type=ledger_type,
        group_id=f"OWNLOAN_{ledger_type.value}",
        user_memo="Owner's Loan Account",
        timestamp=datetime.now(tz=timezone.utc),
        description="Owner's Loan Account Entry",
        op_type="custom_json",
        debit=LiabilityAccount(name="VSC Liability", sub="OpeningBalance"),
        debit_conv=conv,
        debit_amount=conv.msats,
        debit_unit=Currency.MSATS,
        credit=LiabilityAccount(name="Owner Loan Payable", sub="OpeningBalance"),
        credit_conv=conv,
        credit_unit=Currency.MSATS,
        credit_amount=conv.msats,
    )
    await owner_loan_entry.save()
    return owner_loan_entry


def ignore_user(cust_id: str) -> bool:
    IGNORE_USERS = ["v4vapp.dhf", "v4vapp.tre", "brianoflondon", "v4vapp-test"]
    if cust_id in IGNORE_USERS:
        return True
    if "v4vapp" in cust_id:
        return True
    return False


async def wipe_opening_balances():
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry

    collection = LedgerEntry.collection()
    result = await collection.delete_many({"ledger_type": LedgerType.OPENING_BALANCE.value})
    print(f"Deleted {result.deleted_count} opening balance ledger entries.")


async def main():
    db_conn = DBConn()
    await db_conn.setup_database()

    await wipe_opening_balances()
    # Make the file path relative to this script's location so it works
    # regardless of the current working directory when the script is run.
    script_dir = Path(__file__).resolve().parent
    file_path = (
        script_dir / "keepsats_data" / "v4vapp_voltage.keepsats_with_in_progress.json"
    ).resolve()
    bad_accounts = await get_bad_hive_accounts()
    try:
        balances = await load_keepsats_balances(file_path)
        print(f"Loaded {len(balances)} balance records.")
        await TrackedBaseModel.update_quote()
        total_sats = Decimal(0)
        tasks = []  # store (Task, balance) tuples so we can report after TaskGroup completes

        async with TaskGroup() as tg:
            for balance in balances:
                bad_account = balance.id in bad_accounts
                if balance.net_sats >= 2 and not bad_account and not ignore_user(balance.id):
                    task = tg.create_task(create_ledger_entry(balance))
                    tasks.append((task, balance))
                    total_sats += Decimal(balance.net_sats)
                else:
                    print(
                        f"Skipping account {balance.id[:16]:<18} with low balance: {balance.net_sats:>14,.0f} sats"
                    )

        # All tasks have completed successfully here (or an exception was raised)
        processed_count = 0
        for task, balance in tasks:
            try:
                ledger_entry = task.result()
                print(
                    f"Created ledger entry for account {balance.id[:16]:<18}: {balance.net_sats:>14,.0f} {ledger_entry.short_id}"
                )
                processed_count += 1
            except Exception as e:
                print(f"Task failed for account {balance.id}: {e}")

        print(f"Total sats processed: {total_sats:,} across {processed_count} accounts")
        await owners_loan_account(total_sats)

    except Exception as e:
        print(f"Error loading balances: {e}")


if __name__ == "__main__":
    import argparse
    import asyncio

    from v4vapp_backend_v2.config.setup import InternalConfig

    parser = argparse.ArgumentParser(
        description="Load prior Keepsats balances as opening balance ledger entries"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="production.fromhome.config.yaml",
        help="Config filename (relative to config/ folder). Default: production.fromhome.config.yaml",
    )
    args = parser.parse_args()

    # InternalConfig is a singleton — the FIRST call with a config_filename wins.
    # None of the top-level imports trigger InternalConfig(), so this is guaranteed
    # to be the first call.
    InternalConfig(config_filename=args.config)

    asyncio.run(main())
