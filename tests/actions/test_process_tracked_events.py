"""
These tests test the filtering of tracked operations and the ledger population
and the generation of balance sheets.

The test data comes from a MongoDB dump of the v4vapp-dev.hive_ops collection.

"""

import asyncio
from pathlib import Path
from typing import Generator, List
from unittest.mock import AsyncMock, patch

import pytest
from bson import json_util
from nectar.hive import Hive

from v4vapp_backend_v2.accounting.balance_sheet import get_account_balance_printout
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerEntryDuplicateException
from v4vapp_backend_v2.actions.hive_to_lightning import (
    HiveToLightningError,
    lightning_payment_sent,
    process_hive_to_lightning,
    return_hive_transfer,
)
from v4vapp_backend_v2.actions.process_tracked_events import (
    TrackedAny,
    process_tracked_event,
    tracked_any_filter,
    tracked_transfer_filter,
)
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer, load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.database.db import MongoDBClient, get_mongodb_client_defaults
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import LNDPaymentExpired
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.models.payment_models import Payment


async def drop_collection_and_user(conn_name: str, db_name: str, db_user: str) -> None:
    # Drop the collection and user
    async with MongoDBClient(conn_name, db_name, db_user) as test_client:
        if test_client.db is None:
            raise ValueError(f"Database {db_name} does not exist.")
        # Check if the collection exists by listing collection names
        collection_names = await test_client.db.list_collection_names()
        assert isinstance(collection_names, list)
        ans = await test_client.drop_user()
        assert ans.get("ok") == 1
    await drop_database(conn_name=conn_name, db_name=db_name)


async def drop_database(conn_name: str, db_name: str) -> None:
    async with MongoDBClient(conn_name) as admin_client:
        await admin_client.drop_database(db_name)


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    # Mock asyncio.create_task
    # How to patch notification bot to avoid sending real messages
    with patch("v4vapp_backend_v2.config.notification_protocol.NotificationBot") as mock_bot:
        bot_instance = mock_bot.return_value
        bot_instance.send_message = AsyncMock()
        yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


async def fill_rates_db():
    """
    Fill the rates database with quotes.
    This function is a placeholder for filling the rates database.
    It should be implemented to fetch and store quotes in the database.
    """
    # Placeholder for filling the rates database
    mongodb_export_path_rates = "tests/data/hive_models/mongodb/v4vapp-dev.rates.json"
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    with open(mongodb_export_path_rates, "r") as f:
        raw_data = f.read()
        json_data = json_util.loads(raw_data)
    async with TrackedBaseModel.db_client as db_client:
        await db_client.insert_many("rates", json_data)


def load_tracked_any_from_mongodb_dump(file_path: str | Path) -> Generator[TrackedAny, None, None]:
    """
    Load tracked operations from a MongoDB collection.

    :param file_path: Path to the JSONL file.
    :return: List of tracked operations.
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    with open(file_path, "r") as f:
        raw_data = f.read()
        json_data = json_util.loads(raw_data)
    for tracked_op in json_data:
        try:
            op = tracked_any_filter(tracked_op)
            yield op
        except ValueError as e:
            print(f"Ignoring operation: {e}")
            continue


def load_tracked_transfer_from_mongodb_dump(
    file_path: str | Path,
) -> Generator[TrackedTransfer, None, None]:
    """
    Load tracked operations from a MongoDB collection.

    :param file_path: Path to the JSONL file.
    :return: List of tracked operations.
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    with open(file_path, "r") as f:
        raw_data = f.read()
        json_data = json_util.loads(raw_data)
    for tracked_op in json_data:
        try:
            op = tracked_transfer_filter(tracked_op)
            yield op
        except ValueError as e:
            print(f"Ignoring operation: {e}")
            continue


@pytest.mark.asyncio
async def fill_test_database_single_entries_only() -> List[TrackedTransfer]:
    all_data: List[TrackedTransfer] = []
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    await fill_rates_db()
    input_path = Path("tests/data/hive_models/mongodb/event_chain_testing")
    json_files = ["single_events_v4vapp-dev.hive_ops.json", "funding_v4vapp-dev.invoices.json"]
    for file in json_files:
        file_path = Path(input_path, file)
        # Load tracked operations from each JSON file in the directory
        for tracked_op in load_tracked_transfer_from_mongodb_dump(file_path):
            # Process each tracked operation and insert it into the test database
            await tracked_op.save()
            # insert into the all_data list sorted by timestamp for the hive_ops and creation_data for the others
            all_data.append(tracked_op)

    # sort all_data by timestamp
    all_data.sort(key=lambda x: x.timestamp)
    return all_data


@pytest.mark.asyncio
async def load_all_events_from_mongodb_dump() -> List[TrackedTransfer]:
    """
    Load all tracked operations from the MongoDB dump.
    This function is a placeholder for loading all tracked operations.
    It should be implemented to fetch and return all tracked operations from the database.
    """
    input_path = Path("tests/data/hive_models/mongodb/event_chain_testing")
    all_data: List[TrackedTransfer] = []
    for file in input_path.glob("*.json"):
        if not file.is_file():
            continue
        file_path = Path(input_path, file)
        # Load tracked operations from each JSON file in the directory
        for tracked_op in load_tracked_transfer_from_mongodb_dump(file_path):
            all_data.append(tracked_op)
    return all_data


@pytest.mark.asyncio
async def load_events_from_mongodb_dump_file(file_path: str | Path) -> List[TrackedTransfer]:
    """
    Load tracked operations from a MongoDB collection.

    :param file_path: Path to the JSONL file.
    :return: List of tracked operations.
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    all_data = []
    with open(file_path, "r") as f:
        raw_data = f.read()
        json_data = json_util.loads(raw_data)
    for tracked_op in json_data:
        try:
            op = tracked_any_filter(tracked_op)
            all_data.append(op)
        except ValueError as e:
            print(f"Ignoring operation: {e}")
            continue
    return all_data


@pytest.mark.asyncio
async def process_tracked_events_single_items_no_extra_processes():
    """
    Test the fill_test_database function to ensure it populates the database correctly.
    """
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    await TrackedBaseModel.update_quote()
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    LedgerEntry.db_client = TrackedBaseModel.db_client
    all_data = await fill_test_database_single_entries_only()
    with patch("asyncio.create_task") as mock_create_task:
        mock_create_task.return_value = None
        for tracked_op in all_data:
            # print(f"{tracked_op.timestamp}")
            try:
                print(tracked_op.log_str)
                ledger_entries = await process_tracked_event(tracked_op)
                for ledger_entry in ledger_entries:
                    print(ledger_entry)
                    if ledger_entry.op is not None:
                        short_id = ledger_entry.op.short_id
                        query = TrackedBaseModel.short_id_query(short_id)
                        doc = await TrackedBaseModel.db_client.find_one("hive_ops", query)
                        assert doc is not None, (
                            f"Document with short_id {short_id} not found in hive_ops."
                        )

            except LedgerEntryDuplicateException:
                continue

    # Wait for all tasks to complete
    while asyncio.all_tasks():
        if len(asyncio.all_tasks()) == 1:
            break
        await asyncio.sleep(2)


@pytest.mark.asyncio
async def hive_transfer_refund():
    """
    Test the processing of a transfer and a refund.
    This test checks if the transfer and refund are processed correctly.
    This is hideous but it should work.
    """
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    await TrackedBaseModel.update_quote()
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    LedgerEntry.db_client = TrackedBaseModel.db_client
    await process_tracked_events_single_items_no_extra_processes()

    all_data = await load_events_from_mongodb_dump_file(
        Path(
            "tests/data/hive_models/mongodb/event_chain_testing/hive_refund/hive_refund_v4vapp-dev.hive_ops.json"
        )
    )
    inbound_transfer: TrackedTransfer = all_data[
        0
    ]  # Assuming the first entry is the transfer operation
    outbound_transfer: TrackedTransfer = all_data[
        1
    ]  # Assuming the second entry is the payment operation
    with patch("asyncio.create_task") as mock_create_task:
        mock_create_task.return_value = None
        await inbound_transfer.save()  # Ensure the transfer is saved before processing
        ledger_entries_inbound = await process_tracked_event(
            inbound_transfer
        )  # Process the transfer operation
        ledger_entry_inbound = ledger_entries_inbound[0]

        print(ledger_entry_inbound)
        try:
            await process_hive_to_lightning(inbound_transfer, nobroadcast=True)
        except HiveToLightningError as e:
            assert "Operation already has" in str(e)

        inbound_transfer.replies = []

        with patch(
            "v4vapp_backend_v2.actions.hive_to_lightning.decode_incoming_payment_message",
            new_callable=AsyncMock,
        ) as mock_decode:
            mock_decode.side_effect = LNDPaymentExpired("Lightning invoice is expired")
            await process_hive_to_lightning(inbound_transfer, nobroadcast=True)

        for account in [ledger_entry_inbound.debit, ledger_entry_inbound.credit]:
            balance_printout = await get_account_balance_printout(account)
            print(balance_printout)

        # This ends the processing of the inbound transfer, now we process the refund (return_hive_transfer)
        with patch(
            "v4vapp_backend_v2.actions.hive_to_lightning.get_verified_hive_client",
            new_callable=AsyncMock,
        ) as mock_hive_client:
            mock_hive_client.return_value = [Hive(), "devser.v4vapp"]
            # Assume outbound_transfer is your dict or model instance as shown above
            amount_dict = (
                outbound_transfer["amount"]
                if isinstance(outbound_transfer, dict)
                else outbound_transfer.amount.model_dump()
            )
            trx_id = (
                outbound_transfer["trx_id"]
                if isinstance(outbound_transfer, dict)
                else outbound_transfer.trx_id
            )

            mock_trx = {"operations": [[None, {"amount": amount_dict}]], "trx_id": trx_id}

            with patch(
                "v4vapp_backend_v2.actions.hive_to_lightning.send_transfer",
                new_callable=AsyncMock,
            ) as mock_send_hive_transfer:
                mock_send_hive_transfer.return_value = mock_trx
                await return_hive_transfer(
                    hive_transfer=inbound_transfer,
                    reason="Lightning invoice expired",
                    nobroadcast=True,
                )

    # Now we should process the outbound transfer for a ledger.
    with patch("asyncio.create_task") as mock_create_task:
        mock_create_task.return_value = None
        await outbound_transfer.save()  # Ensure the transfer is saved before processing
        ledger_entries_outbound = await process_tracked_event(
            outbound_transfer
        )  # Process the transfer operation
    ledger_entry_outbound = ledger_entries_outbound[0]
    for account in [ledger_entry_outbound.debit, ledger_entry_outbound.credit]:
        balance_printout = await get_account_balance_printout(account, full_history=True)
        print(balance_printout)

    # Wait for all tasks to complete
    while asyncio.all_tasks():
        if len(asyncio.all_tasks()) == 1:
            break
        await asyncio.sleep(2)

    print("All tasks completed.")


async def test_hive_transfer_successful_payment():
    # This needs to follow the previous test.
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    await TrackedBaseModel.update_quote()
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    LedgerEntry.db_client = TrackedBaseModel.db_client
    await hive_transfer_refund()

    all_data = await load_events_from_mongodb_dump_file(
        Path(
            "tests/data/hive_models/mongodb/event_chain_testing/successful_hive_to_lightning/success_v4vapp-dev.hive_ops.json"
        )
    )
    inbound_transfer: TrackedTransfer = all_data[
        0
    ]  # Assuming the first entry is the transfer operation
    outbound_payment: Payment = all_data[1]  # Assuming the second entry is the payment operation
    # outbound_transfer: TrackedTransfer = all_data[
    #     2
    # ]  # Assuming the third entry is the transfer operation

    with patch("asyncio.create_task") as mock_create_task:
        mock_create_task.return_value = None
        await inbound_transfer.save()
        ledger_entries_inbound = await process_tracked_event(inbound_transfer)  #
        ledger_entry_inbound = ledger_entries_inbound[0]
        print(ledger_entries_inbound[0])

        try:
            await process_hive_to_lightning(inbound_transfer, nobroadcast=True)
        except HiveToLightningError as e:
            assert "Operation already has" in str(e)

        with patch(
            "v4vapp_backend_v2.actions.hive_to_lightning.decode_incoming_payment_message",
            new_callable=AsyncMock,
        ) as mock_decode:
            mock_decode.return_value = (PayReq(), LNDClient("example"))
            with patch(
                "v4vapp_backend_v2.actions.hive_to_lightning.send_lightning_to_pay_req",
                new_callable=AsyncMock,
            ) as mock_send_lightning:
                mock_send_lightning.return_value = outbound_payment
                # We also need to remove the replies from  LedgerEntry OP
                ledger_entry_inbound.op.replies = []
                await ledger_entry_inbound.update_op()
                inbound_transfer.replies = []
                await inbound_transfer.save()
                await process_hive_to_lightning(inbound_transfer, nobroadcast=True)
                # this will have created the payment, we need to save it to the database
                await outbound_payment.save()

    # Now we should process the outbound payment for a ledger.
    with patch("asyncio.create_task") as mock_create_task:
        mock_create_task.return_value = None
        ledger_entries_payment = await process_tracked_event(outbound_payment)
        inbound_transfer = await load_tracked_object(inbound_transfer)
        for ledger_entry_payment in ledger_entries_payment:
            # print(ledger_entry_payment.draw_t_diagram())
            print(ledger_entry_payment)

        with patch(
            "v4vapp_backend_v2.actions.hive_to_lightning.get_verified_hive_client",
            new_callable=AsyncMock,
        ) as mock_get_verified_hive_client:
            mock_get_verified_hive_client.return_value = (Hive(), "devser.v4vapp")
            with patch(
                "v4vapp_backend_v2.actions.hive_to_lightning.send_transfer",
                new_callable=AsyncMock,
            ) as mock_send_transfer:
                mock_send_transfer.return_value = {
                    "operations": [[None, {"amount": "4.154 HIVE"}]],
                    "trx_id": "12345678999",
                }
                await lightning_payment_sent(
                    payment=outbound_payment,
                    hive_transfer=inbound_transfer,
                    nobroadcast=True,
                )

    for ledger_entry in ledger_entries_inbound + ledger_entries_payment:
        for account in [ledger_entry.debit, ledger_entry.credit]:
            balance_printout = await get_account_balance_printout(account, full_history=True)
            print(balance_printout)
            print("*" * 100)

    # Wait for all tasks to complete
    while asyncio.all_tasks():
        if len(asyncio.all_tasks()) == 1:
            break
        await asyncio.sleep(2)

    print("All tasks completed.")
