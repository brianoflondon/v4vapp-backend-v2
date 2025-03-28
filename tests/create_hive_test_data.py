"""
Code to watch scan the hive blockchain and dummp to a log file all the transactions
which we need for testing
"""

import asyncio
from pprint import pprint
from typing import Any, Dict

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_extras import (
    MAX_HIVE_BATCH_SIZE,
    get_blockchain_instance,
    get_hive_client,
)
from v4vapp_backend_v2.hive_models.op_base import OpBase, OpInTrxCounter, OpRealm
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_types_enums import (
    OpTypes,
    RealOpsLoopTypes,
    VirtualOpTypes,
)

found_ops = {}


async def scan_hive(op_real_virtual: OpRealm):
    hive = get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive)
    op_in_trx = 0
    hive = get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive)
    end_block = int(hive.get_dynamic_global_properties().get("head_block_number"))
    start_block = int(end_block)
    # start_block = 94415566
    end_block = start_block + 7200000
    if op_real_virtual == OpRealm.VIRTUAL:
        op_names = VirtualOpTypes
    else:
        op_names = RealOpsLoopTypes
        op_names = ["custom_json"]

    logger.info(
        f"Start scanning hive blockchain virtual: {op_real_virtual}",
        extra={"notification": False},
    )
    only_virtual_ops = op_real_virtual == OpRealm.VIRTUAL
    try:
        async_stream = sync_to_async_iterable(
            blockchain.stream(
                opNames=op_names,
                start=start_block,
                stop=end_block,
                # raw_ops=False,
                # max_batch_size=MAX_HIVE_BATCH_SIZE,
                only_virtual_ops=only_virtual_ops,
            )
        )
        op_in_trx_counter = OpInTrxCounter(realm=op_real_virtual)
        async for post in async_stream:
            op_in_trx = op_in_trx_counter.inc(post["trx_id"])
            post["op_in_trx"] = op_in_trx
            if post["block_num"] > start_block:
                start_block = post["block_num"]
            if found_ops.get(post.get("type")):
                found_ops[post.get("type")] += 1
            else:
                found_ops[post.get("type")] = 1
            # if found_ops[post.get("type")] < 20:
            # logger.info(
            #     f"Test data {post['block_num']} - {post.get("type")}",
            #     extra={"hive_event": post},
            # )
            op_base = OpBase.model_validate(post)
            if post.get("type") == "custom_json":
                if "v4vapp" in post.get("id"):
                    print(
                        f"{op_base.op_in_trx:>3} {op_base.trx_id} {op_base.realm:<8} {op_base.type:>30} "
                        f"{found_ops[post.get('type')]:>3}"
                    )
            try:
                custom_json = CustomJson.model_validate(post)
                logger.info(
                    f"{custom_json.log_str}",
                    extra={"notification": True, **custom_json.log_extra},
                )
                print("-----------")
                pprint(post, indent=2)
                print("-----------")
                pprint(custom_json.model_dump(), indent=2)
                print("-----------")
            except ValueError as e:
                pass

        # logger.info(f"End scanning hive blockchain", extra={"notification": False})
        pprint(found_ops, indent=4)
        await asyncio.sleep(0.01)
    except KeyboardInterrupt:
        logger.info(f"End scanning hive blockchain", extra={"notification": False})
        pprint(found_ops, indent=4)
        pass

    except Exception as e:
        logger.exception(f"Error: {e}", extra={"notification": False})
        pass


def op_in_trx_counter(
    op_in_trx: int, last_trx_id: str, post: Dict[str, Any]
) -> tuple[int, str]:
    if last_trx_id == post["trx_id"]:
        op_in_trx += 1
    else:
        op_in_trx = 0
        last_trx_id = post["trx_id"]
    post["op_in_trx"] = op_in_trx
    return op_in_trx, last_trx_id


async def main_async_start():

    async with asyncio.TaskGroup() as tg:
        real = tg.create_task(scan_hive(OpRealm.REAL))
        # virtual = tg.create_task(scan_hive(OpRealm.VIRTUAL))

    pprint(real.result())
    # pprint(virtual.result())


if __name__ == "__main__":
    asyncio.run(main_async_start())
    # scan_hive(virtual=False)
