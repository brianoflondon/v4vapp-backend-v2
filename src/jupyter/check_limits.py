import asyncio
import os
from datetime import datetime, timezone
from pprint import pprint

from v4vapp_backend_v2.accounting.account_balances import (
    get_next_limit_expiry,
    keepsats_balance_printout,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.limit_check_classes import LimitCheckResult
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import limit_check_pipeline
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta

# async def get_limits():
#     cust_id = "v4vapp.qrc"
#     age = timedelta(hours=60)
#     ans = await get_account_lightning_conv(cust_id=cust_id, age=age)
#     pprint(ans.sats)
#     # Example usage of check_hive_conversion_limits
#     limits = await check_hive_conversion_limits(cust_id, line_items=True)

#     for limit in limits:
#         print("Limit OK:", limit.limit_ok)
#         print("Spend Summary:")
#         # pprint(limit.conv_summary)
#         print("Total Sats:", limit.total_sats)
#         print("Total Msats:", limit.total_msats)

#     for limit in limits:
#         print(limit.output_text)

#     limit_ok = all(limit.limit_ok for limit in limits)
#     print("All limits OK:", limit_ok)

#     account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")
#     balance = await one_account_balance(account=account)
#     pprint(balance)


# async def count_hours():
#     for minutes in range(0, 60):
#         age = timedelta(seconds=minutes * 60)
#         ans = await get_account_lightning_conv(cust_id="v4vapp-test", age=age)
#         print(f"Minutes: {minutes}, Sats: {ans.sats}, Msats: {ans.msats}")
#         await asyncio.sleep(0.1)


async def main():
    """
    Main function to run the checks and print results.
    """
    # Example usage of get_account_lightning_conv
    db_conn = DBConn()
    await db_conn.setup_database()
    # # await count_hours()
    # await get_limits()

    # amount_msats = 3_000_000

    # cust_id = "v4vapp-test"
    # # account_printout_str, account_details = await get_account_balance_printout(
    # #     account=LiabilityAccount(name="VSC Liability", sub=cust_id), line_items=False
    # # )

    # # print(account_printout_str)
    # # pprint(account_details)

    cust_id = "v4vapp-test"
    limit_empty = LimitCheckResult()
    print("-------------- Keepsats balance ----------------")
    net_sats, account_balance = await keepsats_balance_printout(cust_id=cust_id, line_items=False)
    logger.info(InternalConfig.db)
    print(f"Net: sats for account {cust_id}: {net_sats}")

    pipeline = limit_check_pipeline(cust_id=cust_id, details=False)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list(length=None)
    pprint(results[0])
    limit_check = LimitCheckResult.model_validate(results[0]) if results else None
    if limit_check:
        print(limit_check)
        print(limit_check.next_limit_expiry)
    expiry_info = await get_next_limit_expiry(cust_id)
    if expiry_info:
        expiry, sats_freed = expiry_info
        expires_in = expiry - datetime.now(tz=timezone.utc)
        print(
            f"Next limit expires in: {format_time_delta(expires_in)}, freeing {sats_freed:,.0f} sats"
        )
    else:
        print("No active limits or transactions")


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    asyncio.run(main())
