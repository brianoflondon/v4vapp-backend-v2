import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

from nectar.blockchain import Blockchain
from nectar.exceptions import NectarException
from nectar.hive import Hive
from nectarapi.exceptions import NumRetriesReached

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_extras import get_blockchain_instance, get_hive_client
from v4vapp_backend_v2.hive_models.custom_json_data import custom_json_test_data
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base import OP_TRACKED, OpBase, op_realm
from v4vapp_backend_v2.hive_models.op_base_counters import OpInTrxCounter


class SwitchToLiveStream(Exception):
    """
    Exception to indicate that the stream should switch to live mode.
    This is used when the stream has been running for a while and needs to
    switch to live mode to avoid missing any operations.
    """

    pass


async def stream_ops_async(
    start: int = None,
    stop: int = None,
    stop_now: bool = False,
    look_back: timedelta = None,
    hive: Hive = None,
    opNames: list[str] = OP_TRACKED,
    filter_custom_json: bool = True,
) -> AsyncGenerator[OpAny, None]:
    """
    An asynchronous generator function for streaming blockchain operations.

    This function streams operations from a Hive blockchain instance, allowing for
    filtering by operation names, virtual operations, and custom JSON data. It supports
    streaming from a specific start block to a stop block, with options for looking back
    a certain time period or stopping at the current block.

        start (int, optional): The starting block number for the stream. Defaults to None.
        stop (int, optional): The stopping block number for the stream. Defaults to None.
        stop_now (bool, optional): If True, stops streaming at the current block. Defaults to False.
        look_back (timedelta, optional): A timedelta to look back from the current time to determine
            the starting block. Defaults to None.
        hive (Hive, optional): An instance of the Hive client. If not provided, a default instance
            is created. Defaults to None.
        opNames (list[str], optional): A list of operation names to track. Defaults to OP_TRACKED.
        filter_custom_json (bool, optional): If True, filters out operations with custom JSON data
            that do not pass a specific test. Defaults to True.

        OpAny: The next operation in the stream, either a base operation or a virtual operation.

    Raises:
        asyncio.CancelledError: If the streaming is cancelled.
        KeyboardInterrupt: If the process is interrupted by the user.
        Exception: For any other errors encountered during streaming.

    Notes:
        - The function uses an asynchronous generator to yield operations in real-time.
        - Virtual operations are handled separately and streamed when necessary.
        - Logging is used to provide information about the streaming process and errors.

    """
    hive = hive or get_hive_client()
    blockchain = get_blockchain_instance(hive_instance=hive)
    # This ensures the Transaction class has a hive instance with memo keys
    OpBase.hive_inst = hive
    if opNames:
        op_realms = [op_realm(op) for op in opNames]
        only_virtual_ops = all(realm == "virtual" for realm in op_realms)
    else:
        only_virtual_ops = False

    current_block = blockchain.get_current_block_num()
    time_now = datetime.now(tz=timezone.utc)
    start_time = time_now
    if look_back:
        start_time = time_now - look_back
        try:
            start_block = blockchain.get_estimated_block_num(start_time)
        except Exception as e:
            # work out the number of blocks using 3 seconds per block
            start_block = current_block - int(look_back.total_seconds() / 3)
            logger.warning(
                f"Error getting start block from time {start_time} using {look_back.total_seconds()} seconds, "
                f"using estimated block number {start_block:,} instead: {e}"
            )
    else:
        start_block = start or current_block

    if stop_now:
        stop_block = current_block
    else:
        stop_block = stop or (2**31) - 1  # Maximum value for a 32-bit signed integer

    last_block = start_block

    while last_block < stop_block:
        await OpBase.update_quote()
        try:
            op_in_trx_counter = OpInTrxCounter()
            async_stream_real = sync_to_async_iterable(
                blockchain.stream(
                    start=start_block,
                    stop=stop_block,
                    only_virtual_ops=only_virtual_ops,
                    opNames=opNames,
                    threading=False,
                )
            )
            logger.info(
                f"Starting Hive scanning at {start_block:,} {start_time} Ending at {stop_block:,} "
                f"using {hive.rpc.url} no_preview",
                extra={
                    "error_code_clear": "stream_restart",
                    "notification": True,
                    "opNames": opNames,
                },
            )
            async for hive_event in async_stream_real:
                if (
                    not only_virtual_ops
                    and hive_event.get("block_num") > last_block
                    and hive_event.get("block_num") <= stop_block
                ):
                    start_block = last_block
                    for virtual_event in blockchain.stream(
                        start=last_block - 1,
                        stop=last_block - 1,
                        raw_ops=False,
                        only_virtual_ops=True,
                        # Very subtle problem with op_in_trx counter if we filter for opNames here.
                        # opNames=opNames,      # we must filter them after updating op_in_trx counter
                        threading=False,
                    ):
                        last_block = hive_event.get("block_num")
                        op_virtual_base = op_any_or_base(virtual_event)
                        op_in_trx_counter.op_in_trx_inc(op_virtual_base)
                        # print(op_virtual_base.type, op_virtual_base.block_num, op_virtual_base.trx_id, op_virtual_base.op_in_trx)
                        if op_virtual_base.type in opNames:
                            yield op_virtual_base
                if not filter_custom_json and not custom_json_test_data(hive_event):
                    continue
                op_base = op_any_or_base(hive_event)

                if only_virtual_ops:
                    # When streaming virtual ops ONLY we need to perform the updates to start and last_block
                    # here, otherwise we will miss the first block
                    start_block = op_base.block_num
                    last_block = op_base.block_num

                op_in_trx_counter.op_in_trx_inc(op_base)
                last_block = op_base.block_num
                yield op_base
        except SwitchToLiveStream as e:
            logger.info(f"{start_block:,} | {e} {last_block:,} {hive.rpc.url} no_preview")
            continue
        except (asyncio.CancelledError, KeyboardInterrupt) as e:
            logger.info(f"Async streamer received signal to stop. Exiting... {e}")
            return
        except (NectarException, NumRetriesReached) as e:
            logger.warning(
                f"{start_block:,} NectarException in block_stream: {e} restarting",
                extra={"notification": False, "error_code": "stream_restart", "error": e},
            )
            await asyncio.sleep(2)

        except StopAsyncIteration as e:
            logger.error(
                f"{start_block:,} StopAsyncIteration in block_stream stopped unexpectedly: {e}"
            )
        except TypeError as e:
            logger.warning(f"{start_block:,} TypeError in block_stream: {e} restarting")
        except Exception as e:
            logger.exception(
                f"{start_block:,} | Error in block_stream: {e} restarting",
                extra={
                    "notification": False,
                    "error": e,
                    "error_code": "stream_restart",
                },
            )
        finally:
            if last_block >= stop_block:
                logger.info(
                    f"{start_block:,} | Reached stop block {stop_block:,}, stopping stream."
                )
                break
            else:
                logger.info(
                    f"{start_block:,} Stream running smoothly, continuing from {last_block=:,} {hive.rpc.url}"
                )
            current_node = hive.rpc.url
            hive.rpc.next()
            logger.info(
                f"{start_block:,} Switching {current_node} -> {hive.rpc.url} no_preview",
                extra={"notification": True, "error_code": "stream_restart"},
            )


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
    opNames = []
    count = 0
    hive = get_hive_client(nodes=["https://rpc.podping.org"])
    async for op in stream_ops_async(
        opNames=opNames, look_back=timedelta(days=1), stop_now=True, hive=hive
    ):
        # logger.info(f"{op.log_str}", extra={**op.log_extra})
        # print(op.log_str)
        count += 1
        if count % 10_000 == 0:
            logger.info(f"{op.block_num:,} Processed {count:,} operations")


# Run the example
if __name__ == "__main__":
    asyncio.run(main())
