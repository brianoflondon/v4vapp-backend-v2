from v4vapp_backend_v2.events.event import async_get_subscribers, async_subscribe, async_publish
import pytest

from v4vapp_backend_v2.events.event_models import Events

@pytest.mark.asyncio
async def test_async_subscribe():
    async def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    await async_subscribe(Events.LND_INVOICE_CREATED, subscriber)
    subscribers = await async_get_subscribers()
    assert Events.LND_INVOICE_CREATED in subscribers
    assert subscriber in subscribers[Events.LND_INVOICE_CREATED]

@pytest.mark.asyncio
async def test_async_publish():
    async def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    await async_subscribe(Events.LND_PAYMENT_RECEIVED, subscriber)
    await async_publish(Events.LND_PAYMENT_RECEIVED, 1, 2)
    subscribers = await async_get_subscribers()
    assert Events.LND_PAYMENT_RECEIVED in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT_RECEIVED]