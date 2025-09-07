import asyncio
import os
from pprint import pprint
from typing import List

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive.hive_extras import (
    get_verified_hive_client_for_accounts,
    send_transfer_bulk,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingCustomJson


async def graceful_shutdown():
    await asyncio.sleep(3)


async def main():
    InternalConfig(config_filename="devhive.config.yaml")
    db_conn = DBConn()
    await db_conn.setup_database()

    custom_json_list: List[PendingCustomJson] = []
    for n in range(5):
        transfer = KeepsatsTransfer(
            from_account="keepsats",
            to_account="v4vapp-test",
            sats=1000 + 10 * n,
            memo="Opening Balance",
        )

        custom_json = PendingCustomJson(
            cj_id="v4vapp_dev_transfer",
            send_account="devser.v4vapp",
            json_data=transfer.model_dump(exclude_unset=True, exclude_none=True),
        )

        custom_json_list.append(custom_json)
    hive_client = await get_verified_hive_client_for_accounts(accounts=["devser.v4vapp"])
    trx = await send_transfer_bulk(custom_json_list=custom_json_list, hive_client=hive_client)
    pprint(trx)


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    asyncio.run(main())
