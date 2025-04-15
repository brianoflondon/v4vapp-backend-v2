import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

from nectar import Hive
from nectar.blockchain import Blockchain

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_extras import get_blockchain_instance, get_hive_client
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base import OP_TRACKED
from v4vapp_backend_v2.hive_models.op_base_counters import OpInTrxCounter


async def stream_ops_async(
    start: int = None,
    stop: int = None,
    stop_now: bool = False,
    look_back: timedelta = None,
    hive: Hive = None,
    opNames: list[str] = OP_TRACKED,
) -> AsyncGenerator[OpAny, None]:
    """
    An async generator that yields numbers up to max_value with a delay.
    By default only tracks operations listed in OP_TRACKED.

    Args:
        max_value: The maximum number to generate up to

    Yields:
        int: The next number in the sequence
    """
    hive = hive or get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive)

    current_block = blockchain.get_current_block_num()
    if look_back:
        time_now = datetime.now(tz=timezone.utc)
        start_block = blockchain.get_estimated_block_num(time_now - look_back)
    else:
        start_block = start or current_block

    if stop_now:
        stop_block = current_block
    else:
        stop_block = stop or (2**31) - 1  # Maximum value for a 32-bit signed integer
    logger.info(f"Starting Hive scanning at {start_block:,} Ending at {stop_block:,}")
    last_block = start_block
    while last_block < stop_block:
        try:
            op_in_trx_counter = OpInTrxCounter()
            async_stream_real = sync_to_async_iterable(
                blockchain.stream(
                    start=start_block,
                    stop=stop_block,
                    only_virtual_ops=False,
                    opNames=opNames,
                )
            )
            async for hive_event in async_stream_real:
                op_base = op_any_or_base(hive_event)
                op_in_trx_counter.inc2(op_base)
                if op_base.block_num > last_block and op_base.block_num <= stop_block:
                    for virtual_event in blockchain.stream(
                        start=last_block - 1,
                        stop=last_block - 1,
                        raw_ops=False,
                        only_virtual_ops=True,
                        opNames=opNames,
                    ):
                        last_block = op_base.block_num
                        op_virtual_base = op_any_or_base(virtual_event)
                        op_in_trx_counter.inc2(op_virtual_base)
                        yield op_virtual_base
                yield op_base
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info("Async streamer received signal to stop. Exiting...")
            return
        except StopAsyncIteration:
            return
        except Exception as e:
            logger.warning(
                f"{start_block:,} | Error in block_stream: {e} restarting",
                extra={"notification": False},
            )
    return


def get_virtual_ops_block(block_num: int, blockchain: Blockchain):
    """
    Get a block from the blockchain. Can't use this because it doesn't process the ops the way
    the stream method does.
    This function is used to retrieve a block from the blockchain.

    Args:
        block_num (int): The block number to retrieve.
        blockchain (Blockchain): The blockchain instance.

    Returns:
        dict: The block data.
    """
    return blockchain.wait_for_and_get_block(block_number=block_num, only_virtual_ops=True)


# Example usage
async def main() -> None:
    opNames = OP_TRACKED
    async for op in stream_ops_async(
        opNames=opNames, look_back=timedelta(seconds=120), stop_now=False
    ):
        logger.info(f"{op.log_str}", extra={**op.log_extra})


# Run the example
if __name__ == "__main__":
    asyncio.run(main())
