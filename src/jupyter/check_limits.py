import asyncio
import os
from datetime import timedelta
from pprint import pprint

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    get_account_lightning_conv,
    keepsats_balance_printout,
    one_account_balance,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn


async def get_limits():
    cust_id = "v4vapp-test"
    age = timedelta(hours=60)
    ans = await get_account_lightning_conv(cust_id=cust_id, age=age)
    pprint(ans.sats)
    # Example usage of check_hive_conversion_limits
    limits = await check_hive_conversion_limits(cust_id, line_items=True)

    for limit in limits:
        print("Limit OK:", limit.limit_ok)
        print("Spend Summary:")
        # pprint(limit.conv_summary)
        print("Total Sats:", limit.total_sats)
        print("Total Msats:", limit.total_msats)

    for limit in limits:
        print(limit.output_text)

    limit_ok = all(limit.limit_ok for limit in limits)
    print("All limits OK:", limit_ok)

    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")
    balance = await one_account_balance(account=account)
    pprint(balance)


async def count_hours():
    for minutes in range(0, 60):
        age = timedelta(seconds=minutes * 60)
        ans = await get_account_lightning_conv(cust_id="v4vapp-test", age=age)
        print(f"Minutes: {minutes}, Sats: {ans.sats}, Msats: {ans.msats}")
        await asyncio.sleep(0.1)


async def main():
    """
    Main function to run the checks and print results.
    """
    # Example usage of get_account_lightning_conv
    db_conn = DBConn()
    await db_conn.setup_database()
    # await count_hours()
    await get_limits()

    amount_msats = 3_000_000

    cust_id = "v4vapp-test"
    # account_printout_str, account_details = await get_account_balance_printout(
    #     account=LiabilityAccount(name="VSC Liability", sub=cust_id), line_items=False
    # )

    # print(account_printout_str)
    # pprint(account_details)

    cust_id = "v4vapp-test"

    print("-------------- Keepsats balance ----------------")
    net_sats, account_balance = await keepsats_balance_printout(cust_id=cust_id, line_items=False)
    logger.info(InternalConfig.db)
    print(f"Net: sats for account {cust_id}: {net_sats}")

    # pprint(account_balance.model_dump())
    # ans, tolerance = await check_balance_sheet_mongodb()
    # print("Balance Sheet Check Result:", ans)
    # print("Balance Sheet Check Tolerance:", tolerance)


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    asyncio.run(main())
