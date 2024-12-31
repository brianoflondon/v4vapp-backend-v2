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

    async_subscribe(Events.LND_INVOICE, subscriber)
    async_subscribe(Events.LND_INVOICE, subscriber2)
    subscribers = get_subscribers()
    assert Events.LND_INVOICE in subscribers
    assert subscriber in subscribers[Events.LND_INVOICE]
    assert subscriber2 in subscribers[Events.LND_INVOICE]


@pytest.mark.asyncio
async def test_async_publish():
    async def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    async def subscriber2(x: int = 5, y: int = 6, z: int = 7):
        print("Subscriber2 called with", x, y, z)
        pass

    async_subscribe(Events.LND_PAYMENT, subscriber)
    async_subscribe(Events.LND_PAYMENT, subscriber2)
    print("Subscribers", get_subscribers())
    async_publish(Events.LND_PAYMENT, 1, 2)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT]
    assert subscriber2 in subscribers[Events.LND_PAYMENT]


@pytest.mark.asyncio
async def test_async_publish_wrong_args():
    async def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    async_subscribe(Events.LND_PAYMENT, subscriber)
    try:
        async_publish(Events.LND_PAYMENT, 1)
    except Exception as e:
        print(e)
    async_publish(Events.LND_PAYMENT, 1, 2)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT]
