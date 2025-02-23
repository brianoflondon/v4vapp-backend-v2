import json
from timeit import default_timer as timeit

import pytest

from v4vapp_backend_v2.helpers.sync_async import AsyncConverter
from v4vapp_backend_v2.hive.hive_client import HiveClient


@pytest.mark.asyncio
async def test_hive_client():
    hive_client = HiveClient()
    assert hive_client.blockchain.get_current_block_num()
    print("Current block number:", hive_client.blockchain.get_current_block_num())
    assert hive_client.blockchain.get_current_block()


@pytest.mark.asyncio
async def test_watch_hive_blockchain():
    OP_NAMES = ["transfer"]
    hive_client = HiveClient()
    start = timeit()
    start_block = hive_client.blockchain.get_current_block_num() - 10
    end_block = start_block + 15
    async with AsyncConverter() as converter:

        @converter.sync_to_async
        def get_stream():
            stream = hive_client.blockchain.stream(start=start_block, opNames=OP_NAMES)
            yield stream.__next__()
        while True:
            async for post in get_stream():
                print("Block number:", post.get("block_num"))
                print(json.dumps(post, indent=2, default=str))
                if post.get("block_num") > end_block:
                    break

    assert True
