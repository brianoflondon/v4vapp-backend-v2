"""
Code to watch scan the hive blockchain and dummp to a log file all the transactions
which we need for testing
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pprint import pprint

import httpx

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_extras import get_blockchain_instance, get_hive_client
from v4vapp_backend_v2.hive_models.op_base import OpBase, OpRealm
from v4vapp_backend_v2.hive_models.op_base_counters import OpInTrxCounter

found_ops: dict[str, int] = {}


async def scan_hive(op_real_virtual: OpRealm):
    """
    Asynchronously scans the Hive blockchain for specific operations within a given range of blocks.

    This function streams operations from the Hive blockchain, processes them, and logs relevant information.
    It supports scanning for both virtual and real operations, with the ability to filter by operation types.

    Args:
        op_real_virtual (OpRealm): Specifies whether to scan for virtual or real operations.
            Use `OpRealm.VIRTUAL` for virtual operations and `OpRealm.REAL` for real operations.

    Raises:
        KeyboardInterrupt: Gracefully handles user interruption during the scanning process.
        Exception: Logs any unexpected errors that occur during the scanning process.

    Behavior:
        - Initializes the Hive client and blockchain instance.
        - Determines the start and end block numbers for the scan.
        - Streams operations from the blockchain asynchronously.
        - Tracks the number of operations in a transaction (`op_in_trx`).
        - Logs and processes operations of interest, particularly those of type `custom_json` with an ID containing "v4vapp".
        - Validates and logs operation data using the `OpBase` and `CustomJson` models.
        - Maintains a count of found operations by type.
        - Handles interruptions and exceptions gracefully, ensuring proper logging and cleanup.

    Note:
        - The function uses `sync_to_async_iterable` to convert the synchronous blockchain stream into an asynchronous iterable.
        - The `found_ops` dictionary is used to track the count of each operation type encountered.
        - Debugging information is printed for operations of interest, including their details and validation results.
    """
    hive = get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive)
    end_block = int(hive.get_dynamic_global_properties().get("head_block_number"))
    date_block = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
    start_block = blockchain.get_estimated_block_num(date_block)

    found_ops = {}
    all_links = []
    logger.info(
        f"Start scanning hive blockchain virtual: {op_real_virtual}",
        extra={"notification": False},
    )
    all_tasks = []
    # only_virtual_ops = op_real_virtual == OpRealm.VIRTUAL
    async with httpx.AsyncClient() as client:
        try:
            async_stream = sync_to_async_iterable(
                blockchain.stream(
                    start=start_block,
                    stop=end_block,
                    # max_batch_size=MAX_HIVE_BATCH_SIZE,
                    # only_virtual_ops=only_virtual_ops,
                )
            )
            op_in_trx_counter_new = OpInTrxCounter(realm=OpRealm.REAL)
            async for hive_event in async_stream:
                if hive_event["block_num"] > start_block:
                    start_block = hive_event["block_num"]
                    async for virtual_event in sync_to_async_iterable(
                        blockchain.stream(
                            start=start_block - 1,
                            stop=start_block - 1,
                            raw_ops=False,
                            only_virtual_ops=True,
                        )
                    ):
                        op_base = OpBase.model_validate(virtual_event)
                        op_in_trx_counter_new.inc2(op_base)
                        print(op_base.log_str)
                        all_links.append(op_base.link)
                        all_tasks.append(asyncio.create_task(client.get(url=op_base.link)))

                    if found_ops.get(op_base.type):
                        found_ops[op_base.type] += 1
                    else:
                        found_ops[op_base.type] = 1

                op_base = OpBase.model_validate(hive_event)
                op_in_trx_counter_new.inc2(op_base)
                if found_ops.get(op_base.type):
                    found_ops[op_base.type] += 1
                else:
                    found_ops[op_base.type] = 1

                print(op_base.log_str)
                all_links.append(op_base.link)
                all_tasks.append(asyncio.create_task(client.get(url=op_base.link)))

            logger.info("End scanning hive blockchain", extra={"notification": False})
            pprint(found_ops, indent=4)
            await asyncio.sleep(0.01)

            for task in all_tasks:
                try:
                    response = await task
                    if response.status_code == 200:
                        logger.info(f"Link: {response.url} is valid")
                    else:
                        logger.warning(f"Link: {response.url} is invalid")
                except Exception as e:
                    logger.exception(f"Error: {e}", extra={"notification": False})

        # for link in all_links:
        #     async with httpx.AsyncClient() as client:
        #         response = await client.get(url=link)
        #         if response.status_code == 200:
        #             logger.info(f"Link: {link} is valid")
        #         else:
        #             logger.warning(f"Link: {link} is invalid")

        except KeyboardInterrupt:
            logger.info("End scanning hive blockchain", extra={"notification": False})
            pprint(found_ops, indent=4)
            pass

        except Exception as e:
            logger.exception(f"Error: {e}", extra={"notification": False})
            pass


async def main_async_start():
    async with asyncio.TaskGroup() as tg:
        real = tg.create_task(scan_hive(OpRealm.REAL))
        # virtual = tg.create_task(scan_hive(OpRealm.VIRTUAL))

    pprint(real.result())
    # pprint(virtual.result())


if __name__ == "__main__":
    asyncio.run(main_async_start())
    # scan_hive(virtual=False)
