import asyncio
from typing import AsyncGenerator

from nectar import Hive

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_extras import get_blockchain_instance, get_hive_client
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base_counters import OpInTrxCounter


async def block_stream(
    start: int = None,
    stop: int = None,
    hive: Hive = None,
    opNames: list[str] = None,
) -> AsyncGenerator[OpAny, None]:
    """
    An async generator that yields numbers up to max_value with a delay.

    Args:
        max_value: The maximum number to generate up to

    Yields:
        int: The next number in the sequence
    """
    if hive is None:
        hive = get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive, mode="head")
    if not start:
        start_block = hive.get_dynamic_global_properties().get("head_block_number")
    else:
        start_block = start
    if not stop:
        stop_block = (2**31) - 1  # Maximum value for a 32-bit signed integer
    else:
        stop_block = stop
    while start_block <= stop_block:
        try:
            op_in_trx_counter = OpInTrxCounter()
            async_stream_real = sync_to_async_iterable(
                blockchain.stream(
                    start=start_block, stop=stop_block, only_virtual_ops=False, opNames=opNames
                )
            )
            async for hive_event in async_stream_real:
                op_base = op_any_or_base(hive_event)
                op_in_trx_counter.inc2(op_base)
                if op_base.block_num > start_block:
                    start_block = op_base.block_num
                    async for virtual_event in sync_to_async_iterable(
                        blockchain.stream(
                            start=start_block - 1,
                            stop=start_block - 1,
                            raw_ops=False,
                            only_virtual_ops=True,
                            opNames=opNames,
                        )
                    ):
                        op_virtual_base = op_any_or_base(virtual_event)
                        op_in_trx_counter.inc2(op_virtual_base)
                        yield op_virtual_base
                yield op_base
        except Exception as e:
            logger.warning(f"{start_block:,} | Error in block_stream: {e} restarting")


# Example usage
async def main() -> None:
    opNames = [
        "custom_json",
        "transfer",
        "account_witness_vote",
        "producer_reward",
        "fill_order",
        "limit_order_create",
    ]
    opNames = []
    async for op in block_stream(opNames=opNames):
        logger.info(op.log_str)


# Run the example
if __name__ == "__main__":
    asyncio.run(main())
