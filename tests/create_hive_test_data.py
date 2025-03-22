"""
Code to watch scan the hive blockchain and dummp to a log file all the transactions
which we need for testing
"""

from pprint import pprint

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive.hive_extras import (
    MAX_HIVE_BATCH_SIZE,
    get_blockchain_instance,
    get_hive_client,
)
from v4vapp_backend_v2.hive_models.op_types_enums import (
    OpTypes,
    RealOpsLoopTypes,
    VirtualOpTypes,
)

found_ops = {}


def scan_hive(virtual=False):
    hive = get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive)
    op_in_trx = 0
    last_trx_id = ""
    hive = get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive)
    end_block = int(hive.get_dynamic_global_properties().get("head_block_number"))
    start_block = int(end_block - 14000 / 3)
    if virtual:
        op_names = VirtualOpTypes
    else:
        op_names = RealOpsLoopTypes
    logger.info(
        f"Start scanning hive blockchain virtual: {virtual}",
        extra={"notification": False},
    )
    try:
        for post in blockchain.stream(
            opNames=op_names,
            start=start_block,
            stop=end_block,
            raw_ops=False,
            max_batch_size=MAX_HIVE_BATCH_SIZE,
            only_virtual_ops=virtual,
        ):
            if post["block_num"] > start_block:
                start_block = post["block_num"]
            if last_trx_id == post["trx_id"]:
                op_in_trx += 1
            else:
                op_in_trx = 0
                last_trx_id = post["trx_id"]
            post["op_in_trx"] = op_in_trx
            if found_ops.get(post.get("type")):
                found_ops[post.get("type")] += 1
            else:
                found_ops[post.get("type")] = 1
            if found_ops[post.get("type")] < 10:
                logger.info(
                    f"Test data {post['block_num']} - {post.get("type")}",
                    extra={"hive_event": post},
                )
        logger.info(f"End scanning hive blockchain", extra={"notification": False})
        pprint(found_ops, indent=4)

    except KeyboardInterrupt:
        logger.info(f"End scanning hive blockchain", extra={"notification": False})
        pprint(found_ops, indent=4)
        pass

    except Exception as e:
        logger.error(f"Error: {e}", extra={"notification": False})
        pass


if __name__ == "__main__":

    scan_hive(virtual=True)
    scan_hive(virtual=False)
