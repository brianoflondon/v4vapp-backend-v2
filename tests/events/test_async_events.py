from v4vapp_backend_v2.events.async_event import (
    get_subscribers,
    async_subscribe,
    async_publish,
    clear_subscribers,
)
import pytest

from v4vapp_backend_v2.events.event_models import Events


@pytest.fixture(autouse=True)
def clear_subscribers_fixture():
    clear_subscribers()
    yield
    clear_subscribers()


@pytest.mark.asyncio
async def test_async_subscribe():
    async def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    async def subscriber2(x: int, y: int):
        print("Subscriber2 called with", x, y)
        pass

    await async_subscribe(Events.LND_INVOICE_CREATED, subscriber)
    await async_subscribe(Events.LND_INVOICE_CREATED, subscriber2)
    subscribers = get_subscribers()
    assert Events.LND_INVOICE_CREATED in subscribers
    assert subscriber in subscribers[Events.LND_INVOICE_CREATED]
    assert subscriber2 in subscribers[Events.LND_INVOICE_CREATED]


@pytest.mark.asyncio
async def test_async_publish():
    async def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    async def subscriber2(x: int = 5, y: int = 6, z: int = 7):
        print("Subscriber2 called with", x, y, z)
        pass

    await async_subscribe(Events.LND_PAYMENT_RECEIVED, subscriber)
    await async_subscribe(Events.LND_PAYMENT_RECEIVED, subscriber2)
    print("Subscribers", get_subscribers())
    await async_publish(Events.LND_PAYMENT_RECEIVED, 1, 2)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT_RECEIVED in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT_RECEIVED]
    assert subscriber2 in subscribers[Events.LND_PAYMENT_RECEIVED]


@pytest.mark.asyncio
async def test_async_publish_wrong_args():
    async def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    await async_subscribe(Events.LND_PAYMENT_RECEIVED, subscriber)
    try:
        await async_publish(Events.LND_PAYMENT_RECEIVED, 1)
    except Exception as e:
        print(e)
    await async_publish(Events.LND_PAYMENT_RECEIVED, 1, 2)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT_RECEIVED in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT_RECEIVED]
