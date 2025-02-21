import json
from unittest.mock import AsyncMock, patch

import pytest
from mongomock_motor import AsyncMongoMockClient

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.helpers.pub_key_alias import (
    LOCAL_PUB_KEY_ALIAS_CACHE,
    get_all_pub_key_aliases,
    update_payment_route_with_alias,
)
from v4vapp_backend_v2.models.payment_models import ListPaymentsResponse


def read_list_payments_raw(file_path: str) -> lnrpc.ListPaymentsResponse:
    with open(file_path, "rb") as file:
        return lnrpc.ListPaymentsResponse.FromString(file.read())


def read_lnd_monitor_v2_voltage_pub_keys(file_path: str) -> dict:
    with open(file_path, "r") as file:
        mongodb_pub_keys = json.load(file)
    return mongodb_pub_keys


def test_route_in_payments():
    lnrpc_list_payments = read_list_payments_raw(
        "tests/data/lnd_lists/list_payments_raw.bin"
    )
    assert lnrpc_list_payments
    assert isinstance(lnrpc_list_payments, lnrpc.ListPaymentsResponse)
    list_payment_response = ListPaymentsResponse(lnrpc_list_payments)

    for payment in list_payment_response.payments:
        try:
            _ = payment.route_fees_ppm
            _ = payment.route_str
            _ = payment.destination
        except Exception as e:
            print(e)
            assert False


@pytest.mark.asyncio
async def pub_key_aliases() -> dict:
    # Load test data
    mongodb_pub_keys = read_lnd_monitor_v2_voltage_pub_keys(
        "tests/data/lnd_lists/lnd_monitor_v2_voltage_pub_keys.json"
    )
    # Set up the mock collection
    col_pub_keys = AsyncMongoMockClient()["test_db"]["pub_keys"]
    for item in mongodb_pub_keys:
        await col_pub_keys.insert_one(item)
    cursor = col_pub_keys.find({})
    pub_key_alias_cache = {}
    async for document in cursor:
        pub_key_alias_cache[document["pub_key"]] = document["alias"]
    return pub_key_alias_cache


@pytest.mark.asyncio
async def test_get_all_pub_key_aliases(mocker):
    # Load test data
    mongodb_pub_keys = read_lnd_monitor_v2_voltage_pub_keys(
        "tests/data/lnd_lists/lnd_monitor_v2_voltage_pub_keys.json"
    )

    # Set up the mock collection
    col_pub_keys = AsyncMongoMockClient()["test_db"]["pub_keys"]
    for item in mongodb_pub_keys:
        await col_pub_keys.insert_one(item)

    # Create a mock db_client
    db_client = AsyncMock()

    # Mock the find method to return the cursor from col_pub_keys
    async def mock_find(collection_name, query):
        return col_pub_keys.find(query)  # Returns a cursor-like object

    db_client.find = mocker.patch.object(db_client, "find", side_effect=mock_find)

    # Call the function with the mocked db_client
    pub_key_alias_cache = await get_all_pub_key_aliases(db_client)

    # Assertions
    assert len(pub_key_alias_cache) == len(mongodb_pub_keys)
    for item in mongodb_pub_keys:
        assert pub_key_alias_cache[item["pub_key"]] == item["alias"]


# Reset the global cache before each test
@pytest.fixture(autouse=True)
def reset_cache():
    global LOCAL_PUB_KEY_ALIAS_CACHE
    LOCAL_PUB_KEY_ALIAS_CACHE = {}


# Helper to simulate an async iterable for mocking cursors
async def async_iterable(sync_iterable):
    for item in sync_iterable:
        yield item


@pytest.mark.asyncio
async def test_update_payment_route_with_alias_fill_cache():
    # Mock dependencies
    db_client = AsyncMock()
    lnd_client = AsyncMock()
    mock_aliases = await pub_key_aliases()

    lnrpc_list_payments = read_list_payments_raw(
        "tests/data/lnd_lists/list_payments_raw.bin"
    )
    list_payment_response = ListPaymentsResponse(lnrpc_list_payments)
    first_call = True
    for payment in list_payment_response.payments:
        # Mock get_all_pub_key_aliases return value
        with patch(
            "v4vapp_backend_v2.helpers.pub_key_alias.get_all_pub_key_aliases",
            new_callable=AsyncMock,
        ) as mock_get_all:
            mock_get_all.return_value = mock_aliases

            # Patch get_node_info to return None
            with patch(
                "v4vapp_backend_v2.helpers.pub_key_alias.get_node_info",
                new_callable=AsyncMock,
            ) as mock_get_node_info:
                mock_get_node_info.return_value = lnrpc.NodeInfo()

                # Call the function with fill_cache=True
                await update_payment_route_with_alias(
                    db_client=db_client,
                    lnd_client=lnd_client,
                    payment=payment,
                    fill_cache=True,
                    force_update=False,
                    col_pub_keys="pub_keys",
                )

                # Assertions
                if payment.destination_pub_keys:
                    assert len(payment.route) > 0
                    assert payment.destination
                    route_str = payment.route_str
                    # assert route_str
                    assert payment.destination
                    print(payment.payment_index, route_str)

                    if first_call:
                        mock_get_all.assert_called_once_with(db_client, "pub_keys")
                        first_call = False
