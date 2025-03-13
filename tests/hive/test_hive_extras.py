import os

import pytest
from beem.blockchain import Blockchain  # type: ignore

from v4vapp_backend_v2.helpers.hive_extras import (
    call_hive_internal_market,
    decode_memo,
    get_blockchain_instance,
    get_hive_client,
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
    assert witness_details == {}


@pytest.mark.asyncio
async def test_call_hive_internal_market():
    answer = await call_hive_internal_market()
    assert answer is not None


HIVE_ACC_TEST = os.environ.get("HIVE_ACC_TEST", "alice")
HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "TEST_KEY")

TEST_MEMO_TRX_ID = [
    {
        "plain_text": "This is a plain text memo",
        "memo": "This is a plain text memo",
        "trx_id": "29c6e1ea6ee7de0cc2ba56193ccafc48a1bdc79d",
    },
    {
        "plain_text": "This is a test encrypted memo",
        "memo": "#66JSnrfFvvLkLtRmMVWGCYa9ayGo9BYvUm3m1QuQEDRmDwGcXyhbB1Jm3no2jZPpw97D1rKipZpnU63j7WRVLUbWwGRKEkcb2HFUJsMXZeGThJ34Qnyy31ULmiBWAG42YbxePEEPzMaykhcPEsfg2Cf",
        "trx_id": "6c030024fc8df6f20ce33a728cd1cdc318d801b5",
    },
    {
        "plain_text": "#This is another test encrypted memo which starts with a # in the plain text",
        "memo": "#pp5zBwcNfWT7vNRNPjwQJBnSNJ8P8dHf3TDfgHAfP74tZCjDtwMcmiLgPFV1nsLwRLWipCCzmXMrKGf5zuSCqtmztkLZbYtQLGJd7Ws2ja8x8RYYXYTeqHsu58NiZYZ6NWCyeexn2u1DKAA6yT8Nsa4iByv8uRDYLx9HPD2kndLiKhMP36bC9xoPgPeDr6FNjeSJxcuhaMfYTaDn5oUA7DPi",
        "trx_id": "c50bbdb2eff812f6b992c6e489097af654cbf338",
    },
]


@pytest.mark.skipif(
    HIVE_MEMO_TEST_KEY == "TEST_KEY",
    reason="No test key provided.",
)
@pytest.mark.parametrize("test_data", TEST_MEMO_TRX_ID)
def test_decode_memo_from_trx_id(test_data):
    trx_id = test_data["trx_id"]
    expected_plain_text = test_data["plain_text"]
    d_memo = decode_memo(memo_keys=[HIVE_MEMO_TEST_KEY], trx_id=trx_id)
    assert d_memo is not None
    assert d_memo == expected_plain_text
    print("memo: ", d_memo, "expected_plain_text: ", expected_plain_text)


@pytest.mark.skipif(
    HIVE_MEMO_TEST_KEY == "TEST_KEY",
    reason="No test key provided.",
)
@pytest.mark.parametrize("test_data", TEST_MEMO_TRX_ID)
def test_decode_memo_from_memo_text_lookup_hive(test_data):
    trx_id = test_data["trx_id"]
    expected_plain_text = test_data["plain_text"]
    hive_inst = get_hive_client(keys=[HIVE_MEMO_TEST_KEY])
    blockchain = get_blockchain_instance(hive_instance=hive_inst)
    trx = blockchain.get_transaction(trx_id)
    memo = trx.get("operations")[0].get("value").get("memo")
    d_memo = decode_memo(
        memo=memo,
        hive_inst=hive_inst,
        memo_keys=[HIVE_MEMO_TEST_KEY],
    )
    assert memo is not None
    assert d_memo == expected_plain_text
    print(
        "memo: ", memo, "d_memo", d_memo, "expected_plain_text: ", expected_plain_text
    )


@pytest.mark.skipif(
    HIVE_MEMO_TEST_KEY == "TEST_KEY",
    reason="No test key provided.",
)
@pytest.mark.parametrize("test_data", TEST_MEMO_TRX_ID)
def test_decode_memo_from_memo_text(test_data):
    memo = test_data["memo"]
    expected_plain_text = test_data["plain_text"]
    d_memo = decode_memo(
        memo=memo,
        memo_keys=[HIVE_MEMO_TEST_KEY],
    )
    assert memo is not None
    assert d_memo == expected_plain_text
    print(
        "memo: ", memo, "d_memo", d_memo, "expected_plain_text: ", expected_plain_text
    )


if __name__ == "__main__":
    pytest.main([__file__])
