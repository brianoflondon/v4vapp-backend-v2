import asyncio
import os
from datetime import datetime
from pprint import pprint
from random import choice, uniform
from timeit import default_timer as timeit

import pytest
from nectar.amount import Amount

from tests.utils import (
    close_all_db_connections,
    get_ledger_count,
    get_lightning_invoice,
    watch_for_ledger_count,
)
from v4vapp_backend_v2.accounting.account_balances import keepsats_balance_printout
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.database.db_tools import convert_decimal128_to_decimal
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_to_float_or_int
from v4vapp_backend_v2.hive.hive_extras import (
    get_verified_hive_client_for_accounts,
    send_custom_json,
    send_transfer,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.pending_transaction_class import (
    PendingCustomJson,
    PendingTransaction,
)
from v4vapp_backend_v2.process.process_pending_hive import resend_transactions

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
    InternalConfig(config_filename="config/devhive.config.yaml")
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
    server = InternalConfig().server_id
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
    server = InternalConfig().server_id
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
    Test that sending CustomJson pay invoice with a keepsats balance that fails when the user does not have enough Keepsats balance.

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
    server_id = InternalConfig().server_id
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account=server_id,
        sats=510_000,
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
    # There are no ledger entries but there needs to be a return custom_json
    # ledger_entries = await watch_for_ledger_count(ledger_count_before, timeout=30)

    start = timeit()
    while timeit() - start < 3000:
        await asyncio.sleep(1)
        last_hive_op = await InternalConfig.db["hive_ops"].find_one(
            {"type": "custom_json", "id": "v4vapp_dev_notification"}, sort=[("timestamp", -1)]
        )
        if last_hive_op:
            custom_json = CustomJson.model_validate(last_hive_op)
            if "Insufficient Keepsats balance" in custom_json.json_data.memo:
                break

    if not last_hive_op:
        raise TimeoutError("Did not receive expected Hive operation in time.")

    last_hive_op = convert_decimal128_to_decimal(last_hive_op)
    custom_json = CustomJson.model_validate(last_hive_op)
    assert "Insufficient Keepsats balance" in custom_json.json_data.memo, (
        f"Expected failure reason not found in description: {custom_json.json_data.memo}"
    )
    print("Sending - >")
    pprint(transfer.model_dump(exclude_none=True, exclude_unset=True))
    print("<- Receiving")
    pprint(custom_json.json_data.model_dump(exclude_none=True, exclude_unset=True))
    logger.info(f"Test passed: {custom_json.json_data.memo}")


def random_amount() -> Amount:
    value = round(uniform(0.05, 0.1), 3)
    symbol = choice(["HIVE", "HBD"])
    return Amount(f"{value:.3f} {symbol}")


async def test_send_wrong_authorization_custom_json():
    """
    Test that sending a custom_json transaction with incorrect authorization does not affect the ledger.

    This test performs the following steps:
    1. Obtains a verified Hive client for the specified account.
    2. Retrieves and logs the current ledger count.
    3. Generates a Lightning invoice.
    4. Checks the current Keepsats balance.
    5. Constructs a KeepsatsTransfer and sends a custom_json transaction with required_auths set to the test account.
    6. Waits for the transaction to process.
    7. Verifies that the last Hive operation of type 'custom_json' is unauthorized.
    8. Asserts that the ledger count remains unchanged after the transaction.

    Ensures that unauthorized custom_json transactions do not impact the ledger state.
    """
    hive_client = await get_verified_hive_client_for_accounts(["v4vapp.qrc"])

    ledger_count = await get_ledger_count()
    logger.info(f"Ledger count: {ledger_count}")

    invoice_sats = 4_000

    invoice = await get_lightning_invoice(value_sat=invoice_sats, memo="")

    net_msats_before, balance = await keepsats_balance_printout(cust_id="v4vapp-test")
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account="devser.v4vapp",
        memo=f"{invoice.payment_request} {datetime.now().isoformat()}",
    )
    json_data_converted = convert_decimals_to_float_or_int(
        transfer.model_dump(exclude_none=True, exclude_unset=True)
    )
    trx = hive_client.custom_json(
        id="v4vapp_dev_transfer",
        json_data=json_data_converted,
        required_auths=["v4vapp.qrc"],
    )

    pprint(trx)
    await asyncio.sleep(10)
    # no impact on ledger the ledger will be untouched.

    last_hive_op = await InternalConfig.db["hive_ops"].find_one(
        {"type": "custom_json"}, sort=[("timestamp", -1)]
    )
    last_hive_op = CustomJson.model_validate(last_hive_op)
    assert last_hive_op.authorized is False, "Expected last_hive_op to be unauthorized"

    ledger_count_after = await get_ledger_count()
    assert ledger_count_after == ledger_count, "Expected ledger count to remain unchanged"


# async def test_bad_account_list():
#     hive_client = await get_verified_hive_client_for_accounts(["v4vapp.qrc"])

#     ledger_count = await get_ledger_count()
#     logger.info(f"Ledger count: {ledger_count}")

#     invoice_sats = 1_000

#     invoice = await get_lightning_invoice(value_sat=invoice_sats, memo="")

#     net_msats_before, balance = await keepsats_balance_printout(cust_id="v4vapp-test")
#     transfer = KeepsatsTransfer(
#         from_account="v4vapp.qrc",
#         to_account="bbittrex",
#         sats=invoice_sats,
#         memo=f"Give some sats to bad actor {datetime.now().isoformat()}",
#     )

#     trx = hive_client.custom_json(
#         id="v4vapp_dev_transfer",
#         json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
#         required_auths=["v4vapp.qrc"],
#     )

#     pprint(trx)
#     await asyncio.sleep(10)
#     # no impact on ledger the ledger will be untouched.

#     last_hive_op = await InternalConfig.db["hive_ops"].find_one(
#         {"type": "custom_json"}, sort=[("timestamp", -1)]
#     )
#     last_hive_op = CustomJson.model_validate(last_hive_op)
#     assert last_hive_op.authorized is False, "Expected last_hive_op to be unauthorized"

#     ledger_count_after = await get_ledger_count()
#     assert ledger_count_after == ledger_count, "Expected ledger count to remain unchanged"


@pytest.mark.skip
async def test_store_pending():
    server_id = InternalConfig().server_id
    for n in range(5):
        amount = random_amount()
        memo = f"Test pending transaction {n}"
        store_pending = await PendingTransaction(
            from_account=server_id,
            to_account="v4vapp-test",
            amount=amount,
            memo=memo,
            nobroadcast=True,
            is_private=False,
        ).save()
        store_custom_json = await PendingCustomJson(
            cj_id="testing_custom_json",
            send_account=server_id,
            json_data={"memo": memo},
            nobroadcast=True,
        ).save()

    await resend_transactions()

    assert len(await PendingTransaction.list_all()) == 0
    assert len(await PendingCustomJson.list_all()) == 0
