"""Direct buildInfo versions on the replica-set member summary."""

import logging
from types import SimpleNamespace
from typing import Any, Self

import pytest
from pymongo.errors import PyMongoError

from v4vapp_backend_v2.database import db_pymongo

ADMIN_URI = "mongodb://admin:secret@h1:27017,h2:27017,h3:27017/?authSource=admin&replicaSet=rs0"


class _DirectClient:
    """Stand-in for AsyncMongoClient that records directConnection and buildInfo."""

    def __init__(self, uri: str, **kwargs: Any) -> None:
        self.uri = uri
        self.kwargs = kwargs
        created.append(self)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False

    def __getitem__(self, name: str) -> "_DirectClient":
        assert name == "admin"
        return self

    async def command(self, spec: object) -> dict[str, Any]:
        assert spec == "buildInfo"
        host = self.uri.split("@", 1)[1].split("/", 1)[0]
        if host == "h1:27017":
            return {
                "version": "8.0.4",
                "versionArray": [8, 0, 4],
                "gitVersion": "abc",
                "modules": [],
            }
        raise PyMongoError(f"unread {host}")


created: list[_DirectClient] = []


class _AdminDB:
    def __init__(self, members: list[dict[str, Any]]) -> None:
        self.members = members
        self.commands: list[object] = []

    async def command(self, spec: object) -> dict[str, Any]:
        self.commands.append(spec)
        if spec == "replSetGetStatus":
            return {"set": "rs0", "members": self.members}
        return {"featureCompatibilityVersion": {"version": "8.0"}}


class _AdminClient:
    def __init__(self, admin_db: _AdminDB) -> None:
        self.admin_db = admin_db

    def __getitem__(self, name: str) -> _AdminDB:
        assert name == "admin"
        return self.admin_db


def _patch_config(monkeypatch: pytest.MonkeyPatch, replica_set: str) -> None:
    connection = SimpleNamespace(replica_set=replica_set)
    config = SimpleNamespace(dbs_config=SimpleNamespace(connections={"conn_1": connection}))

    class _InternalConfig:
        def __init__(self) -> None:
            self.config = config
            self.app_name = "test-app"

    monkeypatch.setattr(db_pymongo, "InternalConfig", _InternalConfig)
    monkeypatch.setattr(db_pymongo, "AsyncMongoClient", _DirectClient)


def _db() -> SimpleNamespace:
    return SimpleNamespace(db_conn="conn_1", admin_uri=ADMIN_URI)


@pytest.fixture(autouse=True)
def _clear_clients() -> None:
    created.clear()


async def test_member_summary_includes_direct_buildinfo_version(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _patch_config(monkeypatch, replica_set="rs0")
    members = [
        {"name": "h1:27017", "stateStr": "PRIMARY", "health": 1.0},
        {"name": "h2:27017", "stateStr": "SECONDARY", "health": 1.0},
        {"name": "h3:27017", "stateStr": "SECONDARY", "health": 0.0},
    ]
    admin_db = _AdminDB(members)
    caplog.set_level(logging.DEBUG, logger="backend")

    await db_pymongo.DBConn._log_replica_set_versions_with_admin_client(
        _db(), admin_client=_AdminClient(admin_db)
    )

    assert [client.kwargs.get("directConnection") for client in created] == [True, True, True]
    assert [client.uri.split("@", 1)[1].split("/", 1)[0] for client in created] == [
        "h1:27017",
        "h2:27017",
        "h3:27017",
    ]
    assert all("replicaSet" not in client.uri for client in created)
    assert all("directConnection=true" in client.uri for client in created)

    summary = next(record for record in caplog.records if hasattr(record, "members"))
    assert summary.levelno == logging.WARNING
    assert summary.members == {
        "h1:27017": {
            "state": "PRIMARY",
            "health": 1.0,
            "fcv": "8.0",
            "version": "8.0.4",
            "fcv_doc": {"version": "8.0"},
        },
        "h2:27017": {
            "state": "SECONDARY",
            "health": 1.0,
            "fcv": "8.0",
            "version": None,
            "fcv_doc": {"version": "8.0"},
        },
        "h3:27017": {
            "state": "SECONDARY",
            "health": 0.0,
            "fcv": "8.0",
            "version": None,
            "fcv_doc": {"version": "8.0"},
        },
    }
    assert (
        "h1:27017 PRIMARY health=1.0 version=8.0.4, "
        "h2:27017 SECONDARY health=1.0 version=unknown, "
        "h3:27017 SECONDARY health=0.0 version=unknown"
    ) in summary.getMessage()
    assert "h2:27017 SECONDARY version unread:" in caplog.text
    assert "h3:27017 SECONDARY health 0.0 version unknown" in caplog.text


async def test_skips_members_when_connection_is_not_a_replica_set(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _patch_config(monkeypatch, replica_set="")
    admin_db = _AdminDB([])
    caplog.set_level(logging.DEBUG, logger="backend")

    await db_pymongo.DBConn._log_replica_set_versions_with_admin_client(
        _db(), admin_client=_AdminClient(admin_db)
    )

    assert created == []
    assert admin_db.commands == []
    assert caplog.records == []
