import os
from unittest.mock import MagicMock, patch

import pytest

from v4vapp_backend_v2.hive.hive_extras import (
    CustomJsonSendError,
    account_hive_balances,
    call_hive_internal_market,
    decode_memo,
    get_blockchain_instance,
    get_hive_client,
    send_custom_json,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer


@pytest.mark.asyncio
async def test_call_hive_internal_market():
    answer = await call_hive_internal_market()
    assert answer is not None


HIVE_ACC_TEST = os.environ.get("HIVE_ACC_TEST", "alice")
HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "NO_KEY_PROVIDED")
HIVE_POSTING_TEST_KEY = os.environ.get("HIVE_POSTING_TEST_KEY", "NO_KEY_PROVIDED")
HIVE_ACTIVE_TEST_KEY = os.environ.get("HIVE_ACTIVE_TEST_KEY", "NO_KEY_PROVIDED")

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
    {
        "plain_text": "Returning encrypted memo",
        "memo": "#A3sob1bjm6pAQsfCbHTLuWkwuDiJyynh3NSzUFd3kDNe4cfRgbVLtuC2i6wYx9Gex3eK6CNGyKASv7dk5mwWNbqViBTFDMPyCNtrKFKKGBfGxwKRe2DoW4ygmvQAJ7zs395UARQQWy447Er5pTurREY",
        "trx_id": "500df0a588053570dedcb5401f17962bf3cfb7ce",
    },
    {
        "plain_text": "Returning plain text memo",
        "memo": "Returning plain text memo",
        "trx_id": "bfcd467390486388e61a7a958921b23150942907",
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
    HIVE_MEMO_TEST_KEY == "NO_KEY_PROVIDED",
    reason="No test key provided.",
)
@pytest.mark.parametrize("test_data", TEST_MEMO_TRX_ID)
def test_decode_memo_from_memo_text_lookup_hive(test_data):
    if HIVE_MEMO_TEST_KEY == "NO_KEY_PROVIDED":
        pytest.skip("Shouldn't reach this No test key provided.")
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
    print("memo: ", memo, "d_memo", d_memo, "expected_plain_text: ", expected_plain_text)


@pytest.mark.skipif(
    HIVE_MEMO_TEST_KEY == "NO_KEY_PROVIDED",
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
    print("memo: ", memo, "d_memo", d_memo, "expected_plain_text: ", expected_plain_text)


@pytest.mark.asyncio
async def test_get_hive_client_error():
    # Mock `get_hive_client` to raise the TypeError
    with patch(
        "v4vapp_backend_v2.hive.hive_extras.Hive",
    ) as mock_get_hive:
        mock_get_hive.side_effect = TypeError("string indices must be integers, not 'str'")

        # Call the function that uses `get_hive_client` and assert it handles the error
        with pytest.raises(ValueError) as e:
            _ = get_hive_client(keys=["5JPoEfF4GbrV9QqKYrHDBo3K8n78PdgWtWVaEqyAjZ8teaHVgTq"])
            assert "No working node found" in str(e)


@pytest.mark.skipif(
    HIVE_ACTIVE_TEST_KEY == "NO_KEY_PROVIDED",
    reason="Active key not provided.",
)
@pytest.mark.asyncio
async def test_send_custom_json_active_key():
    test_data = KeepsatsTransfer(
        from_account="alice",
        to_account="bob",
        sats=1000,
        memo="Test message",
    )
    dict_data = test_data.model_dump()
    try:
        answer = await send_custom_json(
            hive_client=get_hive_client(keys=[HIVE_ACTIVE_TEST_KEY]),
            id="test_id",
            send_account=HIVE_ACC_TEST,
            json_data=dict_data,
            active=True,
            nobroadcast=True,
        )
        assert answer is not None
    except Exception as e:
        pytest.fail(f"send_custom_json raised an exception: {e}")


@pytest.mark.skipif(
    HIVE_POSTING_TEST_KEY == "NO_KEY_PROVIDED",
    reason="Active key not provided.",
)
@pytest.mark.asyncio
async def test_send_custom_json_posting_key():
    test_data = KeepsatsTransfer(
        from_account="alice",
        to_account="bob",
        sats=1000,
        memo="Test message",
    )
    dict_data = test_data.model_dump()
    try:
        answer = await send_custom_json(
            hive_client=get_hive_client(keys=[HIVE_POSTING_TEST_KEY]),
            id="test_id",
            send_account=HIVE_ACC_TEST,
            json_data=dict_data,
            active=False,
            nobroadcast=True,
        )
        assert answer is not None
    except Exception as e:
        pytest.fail(f"send_custom_json raised an exception: {e}")


@pytest.mark.skipif(
    HIVE_POSTING_TEST_KEY == "NO_KEY_PROVIDED",
    reason="Active key not provided.",
)
@pytest.mark.asyncio
async def test_send_custom_json_fail_posting_key_instead_active():
    test_data = KeepsatsTransfer(
        from_account="alice",
        to_account="bob",
        sats=1000,
        memo="Test message",
    )
    dict_data = test_data.model_dump()
    try:
        answer = await send_custom_json(
            hive_client=get_hive_client(keys=[HIVE_POSTING_TEST_KEY]),
            id="test_id",
            send_account=HIVE_ACC_TEST,
            json_data=dict_data,
            active=True,
            nobroadcast=True,
        )
        assert answer is not None
    except CustomJsonSendError:
        assert True
    except Exception as e:
        pytest.fail(f"send_custom_json raised an exception: {e}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "json_data, send_account, hive_client, keys, expected_exception, match_message",
    [
        # Test case: json_data is not a dictionary
        (None, "test_account", None, [], ValueError, "json_data must be a dictionary"),
        # Test case: json_data is empty
        ({}, "test_account", None, [], ValueError, "json_data must not be empty"),
        # Test case: No hive_client or keys provided
        (
            {"key": "value"},
            "test_account",
            None,
            [],
            ValueError,
            "No hive_client or keys provided",
        ),
        # Test case: UnhandledRPCError
        (
            {"key": "value"},
            "test_account",
            MagicMock(custom_json=MagicMock(side_effect=Exception("UnhandledRPCError"))),
            [],
            CustomJsonSendError,
            "Error sending custom_json: UnhandledRPCError",
        ),
        # Test case: Generic exception
        (
            {"key": "value"},
            "test_account",
            MagicMock(custom_json=MagicMock(side_effect=Exception("Generic error"))),
            [],
            CustomJsonSendError,
            "Error sending custom_json: Generic error",
        ),
    ],
)
async def test_send_custom_json_failures(
    json_data, send_account, hive_client, keys, expected_exception, match_message
):
    """
    Parameterized test for failure scenarios in send_custom_json.
    """
    with pytest.raises(expected_exception, match=match_message):
        await send_custom_json(
            json_data=json_data,
            send_account=send_account,
            hive_client=hive_client,
            keys=keys,
        )


def test_account_hive_balances():
    balances = account_hive_balances("v4vapp-test")
    assert isinstance(balances, dict)
    assert "HIVE" in balances
    assert "HBD" in balances


if __name__ == "__main__":
    pytest.main([__file__])
