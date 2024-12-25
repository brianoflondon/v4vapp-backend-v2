from typing import Any, Awaitable, Callable, Dict
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.config.setup import logger

AsyncCallable = Callable[..., Awaitable[None]]

subscribers: Dict[Events, callable] = dict()
async_subscribers: Dict[Events, AsyncCallable] = dict()

async def async_subscribe(event_name: Events, subscriber: AsyncCallable):
    if event_name not in async_subscribers:
        async_subscribers[event_name] = []
    async_subscribers[event_name].append(subscriber)

def subscribe(event_name: Events, subscriber: Callable[..., Any]):
    if event_name not in subscribers:
        subscribers[event_name] = []
    subscribers[event_name].append(subscriber)

def publish(event_name: Events, *args: Any) -> None:
    if event_name in subscribers:
        for subscriber in subscribers[event_name]:
            try:
                subscriber(*args)
            except Exception as e:
                logger.error(f"Error in event {event_name}: {e}")

async def async_publish(event_name: Events, *args: Any) -> None:
    if event_name in async_subscribers:
        for subscriber in async_subscribers[event_name]:
            try:
                await subscriber(*args)
            except Exception as e:
                logger.error(f"Error in event {event_name}: {e}")

def clear_subscribers():
    subscribers.clear()


def get_subscribers():
    return subscribers

async def async_get_subscribers():
    return async_subscribers

def get_subscribers_for_event(event_name: Events):
    return subscribers.get(event_name, [])


def remove_subscriber(event_name: Events, subscriber: callable):
    if event_name not in subscribers:
        return
    subscribers[event_name].remove(subscriber)
    if not subscribers[event_name]:
        del subscribers[event_name]


def remove_all_subscribers(event_name: Events):
    if event_name in subscribers:
        del subscribers[event_name]


def remove_all_subscribers():
    subscribers.clear()
