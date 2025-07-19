import asyncio
import os
from datetime import timedelta
from pprint import pprint

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    get_account_lightning_conv,
    get_keepsats_balance,
)
from v4vapp_backend_v2.config.setup import InternalConfig
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
        pprint(limit.conv_summary)
        print("Total Sats:", limit.total_sats)
        print("Total Msats:", limit.total_msats)

    for limit in limits:
        print(limit.output_text)

    limit_ok = all(limit.limit_ok for limit in limits)
    print("All limits OK:", limit_ok)


async def main():
    """
    Main function to run the checks and print results.
    """
    # Example usage of get_account_lightning_conv
    db_conn = DBConn()
    await db_conn.setup_database()

    # await get_limits()

    # amount_msats = 3_000_000

    # cust_id = "v4vapp-test"
    # account_printout_str, account_details = await get_account_balance_printout(
    #     account=LiabilityAccount(name="Customer Liability", sub=cust_id), line_items=False
    # )

    # print(account_printout_str)
    # pprint(account_details)

    cust_id = "v4vapp.qrc"

    print("-------------- Keepsats balance ----------------")
    keepsats_balance, net_sats = await get_keepsats_balance(cust_id=cust_id, line_items=False)
    print("Keepsats Balance Summary:")
    pprint(net_sats)

    aggregation_pipeline = [
        {"$match": {"cust_id": cust_id}},
        {
            "$facet": {
                # Original sum calculations
                "debit_sum": [
                    {
                        "$match": {
                            "debit.name": "Customer Liability",
                            "debit.account_type": "Liability",
                            "debit.sub": cust_id,
                        }
                    },
                    {"$group": {"_id": None, "total": {"$sum": "$debit_amount_signed"}}},
                ],
                "credit_sum": [
                    {
                        "$match": {
                            "credit.name": "Customer Liability",
                            "credit.account_type": "Liability",
                            "credit.sub": cust_id,
                        }
                    },
                    {"$group": {"_id": None, "total": {"$sum": "$credit_amount_signed"}}},
                ],
                # New line item details
                "debit_items": [
                    {
                        "$match": {
                            "debit.name": "Customer Liability",
                            "debit.account_type": "Liability",
                            "debit.sub": cust_id,
                        }
                    },
                    {
                        "$project": {
                            "description": 1,
                            "ledger_type": 1,
                            # "timestamp": 1,
                            # "amount": "$debit_amount",
                            # "unit": "$debit_unit",
                            # "amount_signed": "$debit_amount_signed",
                            # "short_id": 1,
                            # "debit_conv": "$debit_conv",
                        }
                    },
                    {"$sort": {"timestamp": -1}},  # Newest first
                ],
                "credit_items": [
                    {
                        "$match": {
                            "credit.name": "Customer Liability",
                            "credit.account_type": "Liability",
                            "credit.sub": cust_id,
                        }
                    },
                    {
                        "$project": {
                            "description": 1,
                            "ledger_type": 1,
                            "timestamp": 1,
                            "amount": "$credit_amount",
                            "unit": "$credit_unit",
                            "amount_signed": "$credit_amount_signed",
                            "short_id": 1,
                        }
                    },
                    {"$sort": {"timestamp": -1}},  # Newest first
                ],
            }
        },
        {
            "$project": {
                "debit_total": {"$arrayElemAt": ["$debit_sum.total", 0]},
                "credit_total": {"$arrayElemAt": ["$credit_sum.total", 0]},
                "grand_total": {
                    "$add": [
                        {"$arrayElemAt": ["$debit_sum.total", 0]},
                        {"$arrayElemAt": ["$credit_sum.total", 0]},
                    ]
                },
                "debit_items": 1,
                "credit_items": 1,
            }
        },
    ]

    cursor = await InternalConfig.db["ledger"].aggregate(aggregation_pipeline)
    result = await cursor.to_list(length=None)
    pprint(result)


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    asyncio.run(main())
