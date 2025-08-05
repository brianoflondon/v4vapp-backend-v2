import pytest
from bson import json_util

from tests.utils import clear_and_reset, close_all_db_connections
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.process.process_tracked_events import process_tracked_event


@pytest.fixture(scope="module", autouse=True)
async def config_file():
    ic = InternalConfig(config_filename="config/devhive.config.yaml")
    db_conn = DBConn()
    await db_conn.setup_database()
    await clear_and_reset()
    yield
    await close_all_db_connections()


def load_hive_deposit(trx: dict) -> Transfer:
    """
    Load a Hive deposit transaction.

    Args:
        trx (dict): The transaction data.

    Returns:
        dict: The loaded transaction data.
    """

    with open("tests/process/data/hive_trx_deposit_hive.json", "r") as file:
        data = json_util.loads(file.read())
    transfer = Transfer.model_validate(data)
    return transfer


async def test_hive_deposit():
    """
    Test the loading of a Hive deposit transaction.

    Args:
        mock_ledger_entry_save: Mock for the ledger entry save method.
    """
    transfer = load_hive_deposit({})

    assert isinstance(transfer, Transfer)
    await process_tracked_event(transfer)
    pass
