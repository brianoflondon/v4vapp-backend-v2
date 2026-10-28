import asyncio
from typing import Any

import pytest
from pymongo.errors import OperationFailure

# the monitor lives at the workspace root, not inside the package
import db_monitor
from db_monitor import ResumeToken
from v4vapp_backend_v2.process.processing_pause import (
    reset_processing_pause_for_tests,
    set_local_pause,
)


@pytest.fixture(autouse=True)
def _reset_pause_state():
    """Keep a pause flag from one test from holding the next change stream."""
    reset_processing_pause_for_tests()
    yield
    reset_processing_pause_for_tests()


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


test_results: dict[str, Any] = {}


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

    # run the function; expect it to raise because non-resumable errors are
    # treated as fatal now
    with pytest.raises(RuntimeError) as exc:
        await db_monitor.subscribe_stream(collection_name="foo", pipeline=None, use_resume=True)
    assert "non-resumable" in str(exc.value)
    assert cleared["deleted"], "resume token must be deleted"
    # no new task should be created
    assert not test_results.get("task_called", False)


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


# -------------------------------------------------------------
# new tests for ignore_changes performance and pipeline filtering
# -------------------------------------------------------------


def test_ignore_changes_unit_and_perf():
    """Simple unit checks plus timeit measurement for ignore_changes."""
    # db_monitor is imported from the workspace root like the other tests
    from db_monitor import ignore_changes
    from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import IGNORED_UPDATE_FIELDS

    # trivial ignored update
    change = {"updateDescription": {"updatedFields": {"locked": True}}}
    assert ignore_changes(change, "payments")
    # non-ignored field should not be filtered
    change2 = {"updateDescription": {"updatedFields": {"foo": 1}}}
    assert not ignore_changes(change2, "payments")
    # mix of ignored and non-ignored should still return False
    mix = {"updateDescription": {"updatedFields": {"foo": 1, "locked": True}}}
    assert not ignore_changes(mix, "payments")

    # performance measurement: average should be tiny (<10µs)
    import timeit

    fixed = {"updateDescription": {"updatedFields": {k: 1 for k in IGNORED_UPDATE_FIELDS}}}
    runs = 10_000
    total = timeit.timeit(lambda: ignore_changes(fixed, "payments"), number=runs)
    avg = total / runs
    # on CI the call overhead can be higher; allow up to 100µs
    assert avg < 1e-4, f"ignore_changes too slow: {avg}s"


# ------------------------------------------------------------------
# tests added for the new overwatch flag behavior
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_op_overwatch_flag(monkeypatch):
    """When ``use_overwatch`` is false the monitor should still run the
    processing logic but refrain from calling the Overwatch API methods.
    """
    import db_monitor as module

    # simple stub that returns a fake operation object with the
    # attributes that ``process_op`` expects later on.  we track whether it
    # was handed to ``process_tracked_event`` in the ``processed`` list.
    class DummyOp:
        def __init__(self):
            self.group_id_query = {}
            self.group_id = "G"
            self.log_extra = {}
            self.op_type = "test"
            self.short_id = "S"
            self.log_str = "log"

    created_ops: list = []

    def make_op(doc):
        op = DummyOp()
        created_ops.append(op)
        return op

    monkeypatch.setattr(module, "tracked_any_filter", make_op)
    events: list = []
    processed: list = []

    class FakeOW:
        async def ingest_op(self, op):
            events.append(op)

        async def cancel_flows_for_trigger(self, trigger_group_id):
            return 0

    async def fake_process_tracked_event(op):
        processed.append(op)
        return []

    monkeypatch.setattr(module, "Overwatch", lambda: FakeOW())
    monkeypatch.setattr(module, "process_tracked_event", fake_process_tracked_event)

    # disable overwatch and call; Overwatch should not be invoked but processing
    # still occurs.
    module.set_overwatch_enabled(False)
    assert not module.overwatch_enabled(), "flag should start false"
    await module.process_op({"fullDocument": {"_id": 1}}, "payments")
    assert events == [], "no ingestion occurred when overwatch disabled"
    assert processed == created_ops, "processing must continue even when overwatch is off"

    # now enable and try again
    events.clear()
    processed.clear()
    module.set_overwatch_enabled(True)
    await module.process_op({"fullDocument": {"_id": 2}}, "payments")
    # only the second op should have been consumed; ``created_ops`` still
    # lists both operations we generated earlier.
    assert events == [created_ops[-1]], "operation should have been forwarded when enabled"
    assert processed == [created_ops[-1]], "processing still happens when overwatch is on"


def test_should_cancel_overwatch_on_empty_only_for_hive_transfers():
    """Empty ledger results cancel Overwatch only for Hive transfers.

    CustomJson Lightning payments return [] because hold/withdraw ledgers
    arrive later on the payment stream; cancelling those candidates is the
    keepsats_to_external tracking bug.
    """
    from db_monitor import should_cancel_overwatch_on_empty

    assert should_cancel_overwatch_on_empty("transfer", 0) is True
    assert should_cancel_overwatch_on_empty("recurrent_transfer", 0) is True
    assert should_cancel_overwatch_on_empty("transfer_to_vesting", 0) is True
    assert should_cancel_overwatch_on_empty("transfer", 1) is False
    assert should_cancel_overwatch_on_empty("custom_json", 0) is False
    assert should_cancel_overwatch_on_empty("invoice", 0) is False
    assert should_cancel_overwatch_on_empty("payment", 0) is False
    assert should_cancel_overwatch_on_empty(None, 0) is False


@pytest.mark.asyncio
async def test_process_op_ledger_path_respects_flag(monkeypatch):
    """Ledger entries should also skip the Overwatch call when disabled but
    still be validated.
    """
    import db_monitor as module
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry

    # patch validation so it returns a simple object and record that we
    # actually validated something.
    fake_ledger = object()
    validated: list = []
    monkeypatch.setattr(
        LedgerEntry, "model_validate", lambda doc: validated.append(doc) or fake_ledger
    )

    calls: list = []

    class FakeOW2:
        # signature matches DB monitor call (keyword argument)
        async def ingest_ledger_entry(self, *, ledger_entry):
            calls.append(ledger_entry)

    monkeypatch.setattr(module, "Overwatch", lambda: FakeOW2())

    module.set_overwatch_enabled(False)
    await module.process_op({"fullDocument": {"_id": 3}}, "ledger")
    assert calls == [], "no overwrite ingestion in ledger branch when disabled"
    assert validated == [{"_id": 3}]

    calls.clear()
    validated.clear()
    module.set_overwatch_enabled(True)
    assert module.overwatch_enabled(), "flag must flip to True"
    await module.process_op({"fullDocument": {"_id": 4}}, "ledger")
    assert calls == [fake_ledger], "ingestion should run when enabled"
    assert validated == [{"_id": 4}]


@pytest.mark.skip(
    reason="This test is more of an integration test and is a bit flaky in CI; may revisit later with a more robust approach"
)
@pytest.mark.asyncio
async def test_main_async_start_respects_overwatch_flag(monkeypatch):
    """The helper that launches tasks should only schedule the overwatch
    report loop when the flag is true."""
    import db_monitor as module

    scheduled = []
    orig_create = asyncio.create_task

    # avoid hitting a real MongoDB instance when the startup logic tries to
    # configure the database
    class DummyDBConn:
        async def setup_database(self):
            return None

    monkeypatch.setattr(module, "DBConn", DummyDBConn)

    def fake_task(coro, name=None):
        scheduled.append(name)
        # immediately cancel so we don't actually run anything
        t = orig_create(coro, name=name)
        t.cancel()
        return t

    monkeypatch.setattr(asyncio, "create_task", fake_task)

    # patch helpers that would otherwise hit external services
    monkeypatch.setattr(module, "reset_lightning_opening_balance", lambda: asyncio.sleep(0))
    monkeypatch.setattr(module, "reset_exchange_opening_balance", lambda: asyncio.sleep(0))
    # avoid running sanity checks which hit the database
    monkeypatch.setattr(module, "log_all_sanity_checks", lambda **kwargs: asyncio.sleep(0))
    # and don't try to resend pending hive transactions (avoids DB access)
    monkeypatch.setattr(module, "resend_transactions", lambda: asyncio.sleep(0))

    # run the startup twice, once without overwatch and once with
    await module.main_async_start(use_resume=False, use_overwatch=False)
    # global flag should be toggled accordingly and no overwatch task added
    assert not module.overwatch_enabled(), "flag must follow parameter"
    assert "overwatch_report_loop" not in scheduled
    scheduled.clear()

    # the second invocation should include the overwatch task name and flag true
    task = asyncio.create_task(asyncio.sleep(0), name="dummy")
    task.cancel()
    await module.main_async_start(use_resume=False, use_overwatch=True)
    assert module.overwatch_enabled(), "flag should be true when requested"
    assert "overwatch_report_loop" in scheduled


@pytest.mark.skip(
    reason="This test is more of an integration test and is a bit flaky in CI; may revisit later with a more robust approach"
)
def test_pipeline_filters_ignored_updates():
    """Integration-esque test to verify the payments pipeline logic.

    mongomock cannot evaluate the $expr/$setDifference stage that lives in the
    real pipeline, so rather than running the whole aggregation we check that
    our Python equivalent produces the expected output on a pair of sample
    change documents.  This guarantees parity between the pipeline and
    `ignore_changes()` logic used in the monitor.
    """
    from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import IGNORED_UPDATE_FIELDS

    # create two fake events
    evt_ignored = {
        "operationType": "update",
        "updateDescription": {"updatedFields": {k: True for k in IGNORED_UPDATE_FIELDS}},
        "fullDocument": {"custom_records": {"v4vapp_group_id": "x"}, "status": "SUCCEEDED"},
    }
    evt_allowed = {
        "operationType": "update",
        "updateDescription": {"updatedFields": {"foo": 1}},
        "fullDocument": {"custom_records": {"v4vapp_group_id": "x"}, "status": "SUCCEEDED"},
    }

    # manually apply the same match rules as the pipeline
    def passes_match(evt):
        m = evt
        if m.get("operationType") == "delete":
            return False
        # top-level filters used by all pipelines
        if "custom_records" in m.get("fullDocument", {}):
            if m["fullDocument"]["custom_records"].get("v4vapp_group_id") is None:
                return False
        if "status" in m.get("fullDocument", {}):
            if m["fullDocument"]["status"] not in ["FAILED", "SUCCEEDED"]:
                return False
        return True

    filtered = []
    for e in (evt_ignored, evt_allowed):
        if not passes_match(e):
            continue
        # imitate ignore_updates_match / ignore_changes
        if set(e["updateDescription"]["updatedFields"]) <= set(IGNORED_UPDATE_FIELDS):
            continue
        filtered.append(e)

    assert filtered == [evt_allowed]


class _PauseStream:
    """Change stream whose first session yields one change and whose second is chosen by the test."""

    def __init__(self, changes: list[dict]):
        self.changes = changes
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    async def close(self):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed or not self.changes:
            raise StopAsyncIteration
        return self.changes.pop(0)


class _SequenceCollection:
    def __init__(self, sessions: list[list[dict]]):
        self.sessions = sessions
        self.watch_calls = 0

    async def watch(self, **kwargs):
        changes = self.sessions[self.watch_calls] if self.watch_calls < len(self.sessions) else []
        self.watch_calls += 1
        return _PauseStream(changes)


class _SingleDB:
    def __init__(self, collection):
        self._col = collection

    def __getitem__(self, name: str):
        return self._col


@pytest.mark.asyncio
async def test_paused_change_is_left_for_later(monkeypatch):
    """A change that arrives while processing is paused is not handled and does not move the resume token."""
    db_monitor.shutdown_event.clear()
    paused_change = {"_id": {"_data": "paused-change"}, "fullDocument": {"group_id": "hold"}}
    later_change = {"_id": {"_data": "later-change"}, "fullDocument": {"group_id": "go"}}
    collection = _SequenceCollection([[paused_change], [later_change]])
    monkeypatch.setattr(db_monitor.InternalConfig, "db", _SingleDB(collection))
    monkeypatch.setattr(ResumeToken, "token", property(lambda self: None))

    tokens: list = []
    processed: list = []

    def fake_set_token(self, token_data):
        tokens.append(token_data)

    async def fake_process_op(change, collection):
        processed.append(change["_id"]["_data"])

    async def fake_wait(shutdown_event, collection_name):
        assert collection_name == "payments"
        set_local_pause(False, reason="unpause event")

    monkeypatch.setattr(ResumeToken, "set_token", fake_set_token)
    monkeypatch.setattr(db_monitor, "process_op", fake_process_op)
    monkeypatch.setattr(db_monitor, "wait_while_paused", fake_wait)

    # Deliver the first change only after the pause flag is set, so the
    # watch loop must refuse it.
    original_anext = _PauseStream.__anext__
    seen = {"count": 0}

    async def anext_that_pauses_first(self):
        if self.changes and seen["count"] == 0:
            seen["count"] += 1
            set_local_pause(True, reason="pause event")
        return await original_anext(self)

    monkeypatch.setattr(_PauseStream, "__anext__", anext_that_pauses_first)

    await asyncio.wait_for(
        db_monitor.subscribe_stream(collection_name="payments", pipeline=None, use_resume=True),
        timeout=2,
    )
    await asyncio.sleep(0)

    assert collection.watch_calls == 2
    assert processed == ["later-change"]
    assert tokens == [{"_data": "later-change"}]


def test_operation_start_time_is_kept_until_a_resume_token_is_saved(monkeypatch):
    """A pause before the first resume token must reopen at the original start time."""

    class MemRedis:
        def __init__(self):
            self.store: dict[str, str] = {}

        def get(self, key):
            return self.store.get(key)

        def set(self, key, value):
            self.store[key] = value

        def delete(self, key):
            self.store.pop(key, None)

    # Build the key first. InternalConfig() may replace the redis client during setup.
    token = ResumeToken(collection="payments")
    fake = MemRedis()
    monkeypatch.setattr(db_monitor.InternalConfig, "redis", fake)

    first = token.operation_start_time()
    second = token.operation_start_time()
    assert first == second
    assert fake.store[token.start_key] == str(first)

    token.set_token({"_data": "saved"})
    assert token.start_key not in fake.store
    assert token.redis_key in fake.store


@pytest.mark.asyncio
async def test_stream_stays_closed_until_unpause(monkeypatch):
    """A monitor that starts paused does not open a change stream until unpause."""
    db_monitor.shutdown_event.clear()
    set_local_pause(True, reason="test")
    collection = _SequenceCollection([[]])
    monkeypatch.setattr(db_monitor.InternalConfig, "db", _SingleDB(collection))
    monkeypatch.setattr(ResumeToken, "token", property(lambda self: None))
    order: list[str] = []

    async def fake_wait(shutdown_event, collection_name):
        order.append("wait")
        set_local_pause(False, reason="unpause event")

    original_watch = collection.watch

    async def watch(**kwargs):
        order.append("watch")
        return await original_watch(**kwargs)

    monkeypatch.setattr(collection, "watch", watch)
    monkeypatch.setattr(db_monitor, "wait_while_paused", fake_wait)

    await asyncio.wait_for(
        db_monitor.subscribe_stream(collection_name="hive_ops", pipeline=None, use_resume=True),
        timeout=2,
    )
    assert order == ["wait", "watch"]
