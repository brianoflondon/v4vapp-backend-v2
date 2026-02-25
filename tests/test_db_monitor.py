import asyncio
from typing import Any, Dict

import pytest
from pymongo.errors import OperationFailure

# the monitor lives at the workspace root, not inside the package
import db_monitor
from db_monitor import ResumeToken


class DummyCollection:
    """Fake Mongo collection that simulates a watch failing on enter."""

    def __init__(self):
        self.watch_kwargs = None

    async def watch(self, **kwargs):
        self.watch_kwargs = kwargs

        class Ctx:
            async def __aenter__(inner):
                # simulate a non-resumable error immediately on creation
                raise OperationFailure(
                    "Executor error during getMore :: caused by :: cannot resume stream; the resume token was not found.",
                    code=280,
                )

            async def __aexit__(inner, exc_type, exc_val, exc_tb):
                return False

        return Ctx()


class DummyDB:
    def __init__(self, collection: DummyCollection):
        self._col = collection

    def __getitem__(self, name: str) -> DummyCollection:
        # ignore name; always return same fake collection
        return self._col


test_results: Dict[str, Any] = {}


def fake_create_task(coro, name=None):
    # record that subscribe_stream was spawned again
    test_results["task_called"] = True
    test_results["task_name"] = name
    # mark the coroutine as "consumed" so asyncio doesn't warn about it
    try:
        coro.close()
    except Exception:
        pass

    # return a dummy task (it will not run)
    class DummyTask:
        def cancel(self):
            pass

    return DummyTask()


@pytest.mark.asyncio
async def test_subscribe_stream_discards_bad_resume(monkeypatch):
    """When watch() raises a resume-token failure, the token is cleared and a
    fresh subscription (no resume) is scheduled."""

    dummy_col = DummyCollection()
    monkeypatch.setattr(db_monitor.InternalConfig, "db", DummyDB(dummy_col))

    # intercept resume token operations
    cleared = {"deleted": False}

    original_delete = ResumeToken.delete_token

    def fake_delete(self):
        cleared["deleted"] = True
        return original_delete(self)

    monkeypatch.setattr(ResumeToken, "delete_token", fake_delete)

    # intercept asyncio.create_task so we can inspect its arguments
    monkeypatch.setattr(asyncio, "create_task", fake_create_task)

    # run the function, it should catch the OperationFailure and return an error code
    code = await db_monitor.subscribe_stream(collection_name="foo", pipeline=None, use_resume=True)
    assert code == "db_monitor_foo"
    assert cleared["deleted"], "resume token must be deleted"
    # new task should have been scheduled without resume (flag assures we went through branch)
    assert test_results.get("task_called"), "retry task should be scheduled"
    # we don't need to actually run the retry task here


@pytest.mark.asyncio
async def test_subscribe_stream_no_token_starts_fresh(monkeypatch):
    """When no resume token exists, watch should start with start_at_operation_time.
    Curling the fake collection records the kwargs for inspection."""

    dummy_col = DummyCollection()
    monkeypatch.setattr(db_monitor.InternalConfig, "db", DummyDB(dummy_col))

    # ensure resume.token returns None by leaving redis empty; no special patch needed
    # run subscribe_stream but bail out early by canceling the iteration from within
    # we'll simulate a normal watch that yields nothing and then closes.

    # modify DummyCollection to behave normally for this test
    async def normal_watch(**kwargs):
        dummy_col.watch_kwargs = kwargs

        class Ctx:
            async def __aenter__(inner):
                return inner

            async def __aexit__(inner, exc_type, exc_val, exc_tb):
                return False

            async def __aiter__(inner):
                if False:
                    yield

        return Ctx()

    monkeypatch.setattr(dummy_col, "watch", normal_watch)

    code = await db_monitor.subscribe_stream(collection_name="bar", pipeline=None, use_resume=True)
    # since the watch didn't raise, code should be None
    assert code is None
    # verify that the kwargs included start_at_operation_time when no resume token
    assert "start_at_operation_time" in dummy_col.watch_kwargs
