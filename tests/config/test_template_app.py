# import asyncio
# import sys
# from pathlib import Path
# from unittest.mock import AsyncMock, MagicMock, patch

# import pytest
# from typer.testing import CliRunner
# from yaml import safe_load

# # Adjust import path based on your project structure
# from template_app import CONFIG, InternalConfig, app, logger, main_async_start

# runner = CliRunner()


# # Mock the InternalConfig for testing
# @pytest.fixture
# def mock_config():
#     class MockConfig:
#         version = raw_config["version"]  # Match the actual version from your app
#         default_db_connection = "test_db_conn"
#         default_db_name = "test_db"
#         default_lnd_connection = "test_lnd"
#         db_connections_names = ["test_db_conn", "prod_db_conn"]
#         dbs_names = ["test_db", "prod_db"]
#         lnd_connections_names = ["test_lnd", "prod_lnd"]

#     # Ensure the mock replaces the real InternalConfig
#     with patch(
#         "template_app.InternalConfig", return_value=MagicMock(config=MockConfig())
#     ) as mock:
#         # Also patch the global CONFIG to use our mock
#         with patch("template_app.CONFIG", MockConfig()):
#             yield MockConfig()

# config_file = Path("tests/data/config", "config.yaml")
# with open(config_file) as f_in:
#     raw_config = safe_load(f_in)

# @pytest.fixture
# def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
#     global raw_config
#     test_config_path = Path("tests/data/config")
#     monkeypatch.setattr(
#         "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
#     )
#     test_config_logging_path = Path(test_config_path, "logging/")
#     monkeypatch.setattr(
#         "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
#         test_config_logging_path,
#     )

#     yield


# @pytest.fixture
# def mock_logger():
#     with patch("template_app.logger") as mock_log:
#         yield mock_log


# @pytest.mark.asyncio
# async def test_main_async_start(mock_logger):
#     """Test the async main function"""
#     database_connection = "test_db_conn"
#     db_name = "test_db"
#     lnd_connection = "test_lnd"

#     await main_async_start(database_connection, db_name, lnd_connection)

#     mock_logger.info.assert_called_once_with(
#         f"🔗 Database connection: {database_connection} "
#         f"🔗 Database name: {db_name} "
#         f"🔗 Lightning node: {lnd_connection} "
#     )


# def test_cli_default_params(set_base_config_path, mock_logger, capsys):
#     """Test CLI with default parameters"""
#     result = runner.invoke(app, [])

#     assert result.exit_code == 0
#     mock_logger.info.assert_any_call(
#         f"🏆 ✅ Template App. Started. Version: {raw_config[""]}"
#     )
#     captured = capsys.readouterr()
#     assert "👋 Goodbye!" in captured.out


# def test_cli_custom_params(mock_config, mock_logger, capsys):
#     """Test CLI with custom parameters"""
#     result = runner.invoke(app, ["test_db_conn", "prod_db", "prod_lnd"])

#     assert result.exit_code == 0
#     mock_logger.info.assert_any_call(
#         f"🏆 ✅ Template App. Started. Version: {mock_config.version}"
#     )
#     captured = capsys.readouterr()
#     assert "👋 Goodbye!" in captured.out


# def test_cli_help(mock_config):
#     """Test CLI help command"""
#     result = runner.invoke(app, ["--help"])

#     assert result.exit_code == 0
#     assert "database_connection" in result.output  # Match snake_case
#     assert "db_name" in result.output
#     assert "lnd_connection" in result.output
#     assert f"{mock_config.db_connections_names}" in result.output
#     assert f"{mock_config.dbs_names}" in result.output
#     assert f"{mock_config.lnd_connections_names}" in result.output


# @pytest.mark.asyncio
# async def test_keyboard_interrupt(mock_config, mock_logger, capsys):
#     """Test handling of KeyboardInterrupt"""
#     with patch("template_app.asyncio.run", side_effect=KeyboardInterrupt):
#         result = runner.invoke(app, [])
#         assert result.exit_code == 0  # Typer handles this gracefully
#         captured = capsys.readouterr()
#         assert "👋 Goodbye!" in captured.out


# @pytest.mark.asyncio
# async def test_exception_handling(mock_config, mock_logger, capsys):
#     """Test handling of general exceptions"""
#     with patch("template_app.asyncio.run", side_effect=Exception("Test error")):
#         result = runner.invoke(app, [])
#         assert result.exit_code == 1  # Should exit with error code
#         mock_logger.exception.assert_called_once()
#         captured = capsys.readouterr()
#         assert "👋 Goodbye!" not in captured.out


# def test_invalid_params(mock_config, mock_logger, capsys):
#     """Test CLI with invalid parameters"""
#     result = runner.invoke(app, ["invalid_db", "invalid_db_name", "invalid_lnd"])

#     assert result.exit_code == 0  # Still exits cleanly, just uses the params
#     captured = capsys.readouterr()
#     assert "👋 Goodbye!" in captured.out


# @pytest.fixture
# def mock_asyncio_run(monkeypatch):
#     """Mock asyncio.run to properly handle async calls"""
#     original_run = asyncio.run

#     async def mock_run(coro):
#         try:
#             return await coro
#         except KeyboardInterrupt:
#             print("👋 Goodbye!")
#             sys.exit(0)
#         except Exception as e:
#             logger.exception(e)
#             sys.exit(1)

#     monkeypatch.setattr("template_app.asyncio.run", mock_run)
#     yield
#     monkeypatch.setattr("template_app.asyncio.run", original_run)
