import asyncio
from typing import Any, Awaitable, Callable, Dict
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.config.setup import logger

AsyncCallable = Callable[..., Awaitable[None]]

async_subscribers: Dict[Events, list[AsyncCallable]] = dict()


def async_subscribe(event_name: Events, subscriber: AsyncCallable):
    """
    Asynchronously subscribes a subscriber to a specified event.

    Args:
        event_name (Events): The name of the event to subscribe to.
        subscriber (AsyncCallable): The asynchronous callable to be
        invoked when the event is triggered.

    Raises:
        None

    Returns:
        None
    """
    if event_name not in async_subscribers:
        async_subscribers[event_name] = []
    async_subscribers[event_name].append(subscriber)


def async_publish(event_name: Events, *args: Any) -> None:
    if event_name in async_subscribers:
        for subscriber in async_subscribers[event_name]:
            asyncio.create_task(subscriber(*args))

            # try:
            #     await subscriber(*args)
            # except Exception as e:
            #     logger.error(f"Error in event {event_name}: {e}")


def clear_subscribers():
    async_subscribers.clear()


def get_subscribers():
    return async_subscribers


def get_subscribers_for_event(event_name: Events):
    return async_subscribers.get(event_name, [])


def remove_subscriber(event_name: Events, subscriber: callable):
    if event_name not in async_subscribers:
        return
    async_subscribers[event_name].remove(subscriber)
    if not async_subscribers[event_name]:
        del async_subscribers[event_name]


def remove_all_subscribers(event_name: Events):
    if event_name in async_subscribers:
        del async_subscribers[event_name]


def remove_all_subscribers():
    async_subscribers.clear()
