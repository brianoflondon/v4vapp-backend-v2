import asyncio
import os
from datetime import timedelta
from pprint import pprint

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    get_account_balance_printout,
    get_account_lightning_conv,
    get_keepsats_balance,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db import get_mongodb_client_defaults
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency


async def main():
    """
    Main function to run the checks and print results.
    """
    # Example usage of get_account_lightning_conv
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

    print("-------------- Keepsats balance ----------------")
    keepsats_balance = await get_keepsats_balance(cust_id=cust_id, line_items=False)
    print("Keepsats Balance Summary:")
    pprint(keepsats_balance)
    for ledger_entry in keepsats_balance.ledger_entries:
        print(ledger_entry)

    amount_msats = 3_000_000

    debit_conversion = CryptoConversion(conv_from=Currency.MSATS, value=amount_msats)
    await debit_conversion.get_quote()

    account_printout_str, account_details = await get_account_balance_printout(
        account=LiabilityAccount(name="Customer Liability", sub=cust_id), line_items=False
    )

    print(account_printout_str)
    pprint(account_details)

    # await withdraw_keepsats_to_treasury(
    #     amount_msats=amount_msats,
    #     cust_id=cust_id,
    #     hive_transfer=OpTransfer(),  # Assuming no tracked transfer for this example
    # )
    # ledger_type = LedgerType.HOLD_KEEPSATS
    # withdraw_ledger_entry = LedgerEntry(
    #     cust_id=cust_id,
    #     ledger_type=ledger_type,
    #     group_id=f"test_entry001-{ledger_type.value}",
    #     timestamp=datetime.now(tz=timezone.utc),
    #     description=f"Withdrawal Keepsats {amount_msats / 1000:,.0f} sats for {cust_id}",
    #     debit=LiabilityAccount(
    #         name="Customer Liability",
    #         sub=cust_id,  # This is the CUSTOMER
    #     ),
    #     debit_unit=Currency.MSATS,
    #     debit_amount=amount_msats,
    #     debit_conv=debit_conversion.conversion,
    #     credit=AssetAccount(name="Treasury Lightning", sub="keepsats"),
    #     credit_unit=Currency.MSATS,
    #     credit_amount=amount_msats,
    #     credit_conv=debit_conversion.conversion,
    # )
    # print(withdraw_ledger_entry)
    # await withdraw_ledger_entry.save()

    print("-------------- Keepsats balance ----------------")
    print("Keepsats Balance Summary:")
    pprint(keepsats_balance)
    print(keepsats_balance.net_balance.sats)


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    db_client = get_mongodb_client_defaults()

    TrackedBaseModel.db_client = db_client

    asyncio.run(main())
