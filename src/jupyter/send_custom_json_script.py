import asyncio
import os

from v4vapp_backend_v2.actions.hive_to_lnd import get_verified_hive_client_for_accounts
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.hive.hive_extras import send_custom_json
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer


async def main():
    """
    Main function to run the checks and print results.
    """
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        sats=1000,
        invoice_message="brianoflondon@walletofsatoshi.com",
    )
    # hive_config = InternalConfig().config.hive
    hive_client = await get_verified_hive_client_for_accounts([transfer.from_account])
    trx = await send_custom_json(
        json_data=transfer.model_dump(),
        send_account=transfer.from_account,
        active=True,
        id="v4vapp_dev_transfer",
        hive_client=hive_client,
    )
    print(f"Transaction sent: {trx}")


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    asyncio.run(main())
