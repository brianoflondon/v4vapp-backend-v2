import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from v4vapp_backend_v2.helpers.bad_actors_list import fetch_bad_actor_list, get_bad_hive_accounts


@pytest.mark.asyncio
async def test_fetch_success_writes_tmp_and_sets_redis(mocker, tmp_path):
    # Prepare mock HTTP response with backtick-wrapped list
    sample_list = ["alice", "bob", "carol"]
    content = "`" + "\n".join(sample_list) + "`"

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.text = content

    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_resp))

    mock_redis = mocker.patch("v4vapp_backend_v2.config.setup.InternalConfig.redis_decoded")
    mock_redis.setex = Mock(return_value=None)

    # Run
    result = await fetch_bad_actor_list()

    assert result == set(sample_list)

    # Check Redis setex called with TTL 3600
    assert mock_redis.setex.call_count == 1
    _, kwargs = mock_redis.setex.call_args
    assert kwargs.get("time") == 3600
    # Validate payload is JSON and contains our list
    payload = json.loads(kwargs.get("value"))
    assert set(payload) == set(sample_list)

    # Check tmp file exists in system temp dir and contains our items
    tmp_file = Path(tempfile.gettempdir()) / "bad_actors_backup_list.txt"
    assert tmp_file.exists()
    txt = tmp_file.read_text(encoding="utf-8").splitlines()
    assert set(txt) == set(sample_list)


@pytest.mark.asyncio
async def test_fetch_failure_uses_redis(mocker):
    # Simulate HTTP error
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=httpx.HTTPError("boom")))

    mock_redis = mocker.patch("v4vapp_backend_v2.config.setup.InternalConfig.redis_decoded")
    sample_list = ["x", "y"]
    mock_redis.get = Mock(return_value=json.dumps(sample_list))

    result = await fetch_bad_actor_list()
    assert result == set(sample_list)


@pytest.mark.asyncio
async def test_fetch_failure_uses_tmp_file(mocker, tmp_path):
    # Simulate HTTP error
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=httpx.HTTPError("boom")))

    mock_redis = mocker.patch("v4vapp_backend_v2.config.setup.InternalConfig.redis_decoded")
    mock_redis.get = Mock(return_value=None)

    tmp_file = Path(tempfile.gettempdir()) / "bad_actors_backup_list.txt"
    # ensure clean
    if tmp_file.exists():
        tmp_file.unlink()

    try:
        tmp_file.write_text("alpha\nbeta\ngamma\n", encoding="utf-8")
        result = await fetch_bad_actor_list()
        assert result == {"alpha", "beta", "gamma"}
    finally:
        if tmp_file.exists():
            tmp_file.unlink()


@pytest.mark.asyncio
async def test_fetch_failure_uses_bundled_file(mocker):
    # Simulate HTTP error
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=httpx.HTTPError("boom")))

    mock_redis = mocker.patch("v4vapp_backend_v2.config.setup.InternalConfig.redis_decoded")
    mock_redis.get = Mock(return_value=None)

    module_bundle = (
        Path(__file__).parents[1]
        / ".."
        / "src"
        / "v4vapp_backend_v2"
        / "helpers"
        / "bad_actors_backup_list.txt"
    )
    # Normalize path
    module_bundle = module_bundle.resolve()

    # Backup original content and write a small test content
    original = module_bundle.read_text(encoding="utf-8")
    try:
        test_content = "`\none\ntwo\nthree\n`"
        module_bundle.write_text(test_content, encoding="utf-8")
        result = await fetch_bad_actor_list()
        assert result == {"one", "two", "three"}
    finally:
        module_bundle.write_text(original, encoding="utf-8")


@pytest.mark.asyncio
async def test_local_bad_accounts_include_tre(mocker):
    mocker.patch(
        "v4vapp_backend_v2.helpers.bad_actors_list.fetch_bad_actor_list",
        new=AsyncMock(return_value=set()),
    )

    result = await get_bad_hive_accounts()
    assert "tre" in result
