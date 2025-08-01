import asyncio
import os

import pytest
from nectar.amount import Amount

from tests.actions.test_full_stack import (
    close_all_db_connections,
    get_ledger_count,
    get_lightning_invoice,
    watch_for_ledger_count,
)
from v4vapp_backend_v2.config.setup import HiveRoles, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive.hive_extras import (
    get_verified_hive_client_for_accounts,
    send_custom_json,
    send_transfer,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson

if os.getenv("GITHUB_ACTIONS") == "true":
    pytest.skip("Skipping tests on GitHub Actions", allow_module_level=True)

"""
This module attempts to test the main monitoring and ledger generating parts of the stack by running them
as background processes and then running tests against them.
It includes fixtures for setup and teardown, as well as tests for various payment scenarios.

This must be run after the three watchers are running, as it relies on the watchers to generate the ledgers.


"""


@pytest.fixture(scope="module", autouse=True)
async def config_file():
    ic = InternalConfig(config_filename="config/devhive.config.yaml")
    db_conn = DBConn()
    await db_conn.setup_database()
    yield
    await close_all_db_connections()


async def test_hive_to_lightning_failure_not_enough_sent():
    """
    Test that sending a HIVE transfer which is not enough to cover the value of the Lightning invoice
    so fails tests
    This test:
    - Retrieves the current ledger count.
    - Creates a Lightning invoice with a specified amount and memo.
    - Verifies the invoice payment request is not empty.
    - Obtains a verified Hive client for the test customer account.
    - Determines the server account for the Hive role.
    - Attempts to send a HIVE transfer below the user limit to the server account with the invoice payment request as the memo.
    - Asserts that the transaction was sent.
    - Waits for the ledger to update and checks the latest entry's description for the expected failure reason.
    """

    ledger_count_before = await get_ledger_count()
    invoice = await get_lightning_invoice(
        value_sat=21_000_000, memo="v4vapp.qrc | Your message goes here | #v4vapp"
    )
    assert invoice.payment_request, "Invoice payment request is empty"

    customer = "v4vapp-test"
    hive_client = await get_verified_hive_client_for_accounts([customer])
    hive_config = InternalConfig().config.hive
    server = hive_config.get_hive_role_account(hive_role=HiveRoles.server).name
    # Sending 1.000 HIVE should fail due to user limits
    trx = await send_transfer(
        from_account=customer,
        to_account=server,
        hive_client=hive_client,
        amount=Amount("5.000 HIVE"),
        memo=invoice.payment_request,
    )

    assert trx.get("trx_id"), "Transaction failed to send"
    # Should just be two entries, receipt of Hive and refund of Hive
    ledger_entries = await watch_for_ledger_count(ledger_count_before + 2, timeout=30)

    description = ledger_entries[-1].description
    assert "Not enough sent to process this payment request" in description, (
        f"Expected failure reason not found in description: {description}"
    )
    logger.info(f"Test passed: {description}")


async def test_hive_paywithsats_keepsats_failure_not_enough_keepsats():
    """
    Test that sending HIVE to Keepsats fails when the user does not have enough Keepsats balance.

    This test performs the following steps:
    1. Retrieves the current ledger entry count.
    2. Generates a Lightning invoice with a specified amount and memo.
    3. Verifies that the invoice payment request is not empty.
    4. Obtains a verified Hive client for the test customer account.
    5. Retrieves the server account name from the Hive configuration.
    6. Attempts to send a small amount of HIVE from the customer to the server, expecting failure due to insufficient Keepsats balance.
    7. Asserts that the transaction was sent (trx_id exists).
    8. Waits for the ledger to reflect two new entries.
    9. Checks that the last ledger entry's description contains the expected failure reason: "Insufficient Keepsats balance".
    """
    ledger_count_before = await get_ledger_count()
    invoice = await get_lightning_invoice(
        value_sat=510_000, memo="v4vapp.qrc | Your message goes here |  #v4vapp"
    )
    assert invoice.payment_request, "Invoice payment request is empty"

    customer = "v4vapp-test"
    hive_client = await get_verified_hive_client_for_accounts([customer])
    hive_config = InternalConfig().config.hive
    server = hive_config.get_hive_role_account(hive_role=HiveRoles.server).name
    # Sending 1.000 HIVE should fail due to user limits
    trx = await send_transfer(
        from_account=customer,
        to_account=server,
        hive_client=hive_client,
        amount=Amount("0.001 HIVE"),
        memo=f"{invoice.payment_request} #paywithsats",
    )

    assert trx.get("trx_id"), "Transaction failed to send"
    # There will not be hold and release entries so 2 entries
    ledger_entries = await watch_for_ledger_count(ledger_count_before + 2, timeout=30)

    description = ledger_entries[-1].description
    assert "Insufficient Keepsats balance" in description, (
        f"Expected failure reason not found in description: {description}"
    )
    logger.info(f"Test passed: {description}")


async def test_custom_json_paywithsats_keepsats_failure_not_enough_keepsats():
    """
    Test that sending HIVE to Keepsats fails when the user does not have enough Keepsats balance.

    This test performs the following steps:
    1. Retrieves the current ledger entry count.
    2. Generates a Lightning invoice with a specified amount and memo.
    3. Verifies that the invoice payment request is not empty.
    4. Obtains a verified Hive client for the test customer account.
    5. Retrieves the server account name from the Hive configuration.
    6. Attempts to send a small amount of HIVE from the customer to the server, expecting failure due to insufficient Keepsats balance.
    7. Asserts that the transaction was sent (trx_id exists).
    8. Waits for the ledger to reflect two new entries.
    9. Checks that the last ledger entry's description contains the expected failure reason: "Insufficient Keepsats balance".
    """
    ledger_count_before = await get_ledger_count()
    invoice = await get_lightning_invoice(
        value_sat=510_000, memo="v4vapp.qrc | Your message goes here |  #v4vapp"
    )
    assert invoice.payment_request, "Invoice payment request is empty"

    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        memo=invoice.payment_request,
    )

    customer = "v4vapp-test"
    hive_client = await get_verified_hive_client_for_accounts([customer])
    trx = await send_custom_json(
        json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
        send_account=transfer.from_account,
        active=True,
        id="v4vapp_dev_transfer",
        hive_client=hive_client,
    )

    assert trx.get("trx_id"), "Transaction failed to send"
    # There will only be a dummy ledger for the receipt of the custom_json
    ledger_entries = await watch_for_ledger_count(ledger_count_before + 1, timeout=30)

    await asyncio.sleep(5)
    last_hive_op = await InternalConfig.db["hive_ops"].find_one(
        {"type": "custom_json", "id": "v4vapp_dev_notification"}, sort=[("timestamp", -1)]
    )
    custom_json = CustomJson.model_validate(last_hive_op)
    assert "Insufficient Keepsats balance" in custom_json.json_data.memo, (
        f"Expected failure reason not found in description: {custom_json.json_data.memo}"
    )
    logger.info(f"Test passed: {custom_json.json_data.memo}")
