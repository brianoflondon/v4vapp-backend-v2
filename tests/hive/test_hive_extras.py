import pytest

from v4vapp_backend_v2.helpers.hive_extras import (
    call_hive_internal_market,
    get_hive_witness_details,
)


@pytest.mark.asyncio
async def test_get_hive_witness_details():
    witness_details = await get_hive_witness_details("blocktrades")
    assert witness_details is not None
    assert witness_details["witness_name"] == "blocktrades"
    assert witness_details["missed_blocks"] >= 0


@pytest.mark.asyncio
async def test_get_hive_witness_details_error():
    witness_details = await get_hive_witness_details("non_existent_witness")
    assert witness_details is None


@pytest.mark.asyncio
async def test_call_hive_internal_market():
    answer = await call_hive_internal_market()
    assert answer is not None


if __name__ == "__main__":
    pytest.main([__file__])
