import asyncio
import os
from datetime import timedelta
from pprint import pprint

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    get_account_lightning_conv,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db import get_mongodb_client_defaults


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
    limits = await check_hive_conversion_limits(cust_id)
    for limit in limits:
        print(limit.output_text)
        print("Limit OK:", limit.limit_ok)
        print("Spend Summary:")
        pprint(limit.spend_summary)
        print("Total Sats:", limit.total_sats)
        print("Total Msats:", limit.total_msats)

    limit_ok = all(limit.limit_ok for limit in limits)
    print("All limits OK:", limit_ok)


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    db_client = get_mongodb_client_defaults()

    TrackedBaseModel.db_client = db_client

    asyncio.run(main())
