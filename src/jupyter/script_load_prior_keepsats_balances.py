from datetime import datetime
from hashlib import sha256
from typing import List

import aiofiles
from bson import json_util
from mongomock import DuplicateKeyError
from pydantic import BaseModel, Field

from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
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


async def load_keepsats_balances(file_path: str) -> List[KeepsatsBalance]:
    """
    Asynchronously loads and parses the Keepsats balances data from a MongoDB export JSON file.

    This function reads the JSON file, which contains BSON-specific types like $date and $numberLong,
    and uses bson.json_util.loads to properly decode them into Python types (e.g., datetime objects
    for dates and int for large numbers). It then validates and converts each record into a
    KeepsatsBalance Pydantic model.

    Args:
        file_path (str): The path to the JSON file containing the balances data.

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

    quote = TrackedBaseModel.last_quote

    conv = CryptoConversion(
        conv_from=Currency.MSATS, value=balance.net_msats, quote=quote
    ).conversion

    memo = f"Opening Balance for {to_account} last transaction {balance.last_timestamp} "
    group_id = generate_hive_trx_id(balance)
    short_id = f"0000-{group_id[:6]}"

    ledger_type = LedgerType.CUSTOM_JSON_TRANSFER
    transfer_ledger_entry = LedgerEntry(
        cust_id=to_account,
        short_id=short_id,
        ledger_type=ledger_type,
        group_id=f"{group_id}-{ledger_type.value}",
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


if __name__ == "__main__":
    import asyncio

    InternalConfig(config_filename="config/devhive.config.yaml")

    async def main():
        db_conn = DBConn()
        await db_conn.setup_database()
        file_path = "src/jupyter/data/v4vapp_voltage.keepsats_with_in_progress.json"
        bad_accounts = await get_bad_hive_accounts()
        try:
            balances = await load_keepsats_balances(file_path)
            print(f"Loaded {len(balances)} balance records.")
            await TrackedBaseModel.update_quote()
            for balance in balances:
                bad_account = balance.id in bad_accounts
                if balance.net_sats >= 2 and not bad_account:
                    ledger_entry = await create_ledger_entry(balance)
                    print(
                        f"Created ledger entry for account {balance.id[:16]:<18}: {balance.net_sats:>14,.0f} {ledger_entry.short_id}"
                    )
                else:
                    print(
                        f"Skipping account {balance.id[:16]:<18} with low balance: {balance.net_sats:>14,.0f} sats"
                    )
        except Exception as e:
            print(f"Error loading balances: {e}")

    asyncio.run(main())


# last line
# last line
