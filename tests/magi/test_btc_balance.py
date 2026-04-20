from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from v4vapp_backend_v2.magi.btc_balance import MagiBTCBalance, get_magi_btc_balance_by_account


@pytest.mark.asyncio
async def test_get_btc_balance_by_account_success(mocker):
    mock_response = Mock()
    mock_response.raise_for_status = Mock()
    mock_response.json.return_value = {
        "data": {
            "btc_mapping_balances": [{"account": "hive:devser.v4vapp", "balance_sats": 12345}]
        }
    }

    mock_http_post = mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    mock_http_post.return_value = mock_response

    result = await get_magi_btc_balance_by_account("hive:devser.v4vapp")

    assert isinstance(result, MagiBTCBalance)
    assert result.account == "hive:devser.v4vapp"
    assert result.balance_sats == Decimal(12345)
    assert result.balance_msats == Decimal(12345000)
    mock_http_post.assert_awaited_once()
    _, kwargs = mock_http_post.call_args
    assert kwargs["json"]["variables"]["account"] == "hive:devser.v4vapp"


@pytest.mark.asyncio
async def test_get_btc_balance_by_account_empty_result(mocker):
    mock_response = Mock()
    mock_response.raise_for_status = Mock()
    mock_response.json.return_value = {"data": {"btc_mapping_balances": []}}

    mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response)

    result = await get_magi_btc_balance_by_account("hive:missing")
    result_test = MagiBTCBalance(account="hive:missing", balance_sats=Decimal(0))
    assert result.account == result_test.account
    assert result.balance_sats == result_test.balance_sats


@pytest.mark.asyncio
async def test_get_btc_balance_by_account_graphql_error(mocker):
    mock_response = Mock()
    mock_response.raise_for_status = Mock()
    mock_response.json.return_value = {"errors": [{"message": "something went wrong"}]}

    mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response)

    with pytest.raises(RuntimeError, match="GraphQL errors"):
        await get_magi_btc_balance_by_account("hive:devser.v4vapp")


@pytest.mark.asyncio
async def test_get_btc_balance_by_account_fallback_to_second_endpoint(mocker):
    mock_response_success = Mock()
    mock_response_success.raise_for_status = Mock()
    mock_response_success.json.return_value = {
        "data": {
            "btc_mapping_balances": [{"account": "hive:devser.v4vapp", "balance_sats": 54321}]
        }
    }

    mocker.patch(
        "v4vapp_backend_v2.magi.btc_balance.MAGI_ENDPOINTS",
        ["https://first.example/graphql", "https://second.example/graphql"],
    )

    mock_http_post = mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    mock_http_post.side_effect = [
        httpx.HTTPStatusError(
            "Server error",
            request=Mock(),
            response=Mock(status_code=500),
        ),
        mock_response_success,
    ]

    result = await get_magi_btc_balance_by_account("devser.v4vapp")

    assert isinstance(result, MagiBTCBalance)
    assert result.account == "hive:devser.v4vapp"
    assert result.balance_sats == Decimal(54321)
    assert mock_http_post.await_count == 2
    assert mock_http_post.call_args_list[0][0][0] == "https://first.example/graphql"
    assert mock_http_post.call_args_list[1][0][0] == "https://second.example/graphql"
