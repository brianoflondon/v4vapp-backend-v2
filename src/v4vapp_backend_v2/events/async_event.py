import asyncio
from typing import Any, Awaitable, Callable, Dict, List

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.events.event_models import Events

AsyncCallable = Callable[..., Awaitable[None]]

async_subscribers: Dict[Events, list[AsyncCallable]] = {}


def async_subscribe(event_names: Events | List[Events], subscriber: AsyncCallable):
    """
    Asynchronously subscribes a subscriber to one or more specified events.

    Args:
        event_names (Union[Events, List[Events]]): The name(s) of the event(s)
        to subscribe to.
        subscriber (AsyncCallable): The asynchronous callable to be
        invoked when the event is triggered.

    Raises:
        None

    Returns:
        None
    """
    if not isinstance(event_names, list):
        event_names = [event_names]

    for event_name in event_names:
        if event_name not in async_subscribers:
            async_subscribers[event_name] = []
        async_subscribers[event_name].append(subscriber)


def async_publish(event_name: Events, *args: Any, **kwargs: Any) -> None:
    if event_name in async_subscribers:
        for subscriber in async_subscribers[event_name]:
            if asyncio.iscoroutinefunction(subscriber):
                asyncio.create_task(subscriber(*args, **kwargs))
            else:
                logger.error(f"Subscriber {subscriber} is not a coroutine function")

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


def remove_subscriber(event_name: Events, subscriber: Callable):
    if event_name not in async_subscribers:
        return
    async_subscribers[event_name].remove(subscriber)
    if not async_subscribers[event_name]:
        del async_subscribers[event_name]


def remove_all_subscribers(event_name: Events):
    if event_name in async_subscribers:
        del async_subscribers[event_name]
