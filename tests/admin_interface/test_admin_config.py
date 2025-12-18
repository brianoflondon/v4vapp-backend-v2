"""
Admin Test Configuration

Pytest configuration and fixtures specifically for admin interface testing.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from v4vapp_backend_v2.admin.admin_app import create_admin_app


@pytest.fixture(scope="session")
def admin_app():
    """Create admin app instance for the test session"""
    return create_admin_app(config_filename="devhive.config.yaml")


@pytest.fixture
def admin_client(admin_app):
    """Create a test client for the admin app"""
    return TestClient(admin_app)


@pytest.fixture
def template_env():
    """Create Jinja2 environment for template testing"""
    # Resolve repository root relative to this test file and find admin templates
    admin_dir = Path(__file__).parent.parent.parent / "src" / "v4vapp_backend_v2" / "admin"
    templates_dir = admin_dir / "templates"

    from jinja2 import Environment, FileSystemLoader

    return Environment(loader=FileSystemLoader(str(templates_dir)))


@pytest.fixture
def mock_user_data():
    """Mock user data for testing"""
    return [
        {
            "sub": "testuser1",
            "balance_sats": 1500000,
            "balance_sats_fmt": "1,500,000",
            "has_transactions": True,
            "error": None,
        },
        {
            "sub": "testuser2",
            "balance_sats": -50000,
            "balance_sats_fmt": "-50,000",
            "has_transactions": True,
            "error": None,
        },
        {
            "sub": "inactive_user",
            "balance_sats": 0,
            "balance_sats_fmt": "0",
            "has_transactions": False,
            "error": None,
        },
        {
            "sub": "error_user",
            "balance_sats": None,
            "balance_sats_fmt": "Error",
            "has_transactions": False,
            "error": "Connection timeout",
        },
    ]


@pytest.fixture
def mock_nav_items():
    """Mock navigation items for testing"""
    return [
        {"name": "Dashboard", "url": "/admin", "active": False},
        {"name": "Users", "url": "/admin/users", "active": True},
        {"name": "V4V Config", "url": "/admin/v4vconfig", "active": False},
        {"name": "Account Balances", "url": "/admin/accounts", "active": False},
        {"name": "Financial Reports", "url": "/admin/financial-reports", "active": False},
    ]


@pytest.fixture
def mock_summary_data():
    """Mock summary data for testing"""
    return {
        "total_users": 4,
        "active_users": 2,
        "total_positive_balance": 1500000,
        "total_positive_balance_fmt": "1,500,000",
        "error_count": 1,
    }


# Test markers
def pytest_configure(config):
    """Add custom markers for admin tests"""
    config.addinivalue_line("markers", "admin: marks tests as admin interface tests")
    config.addinivalue_line("markers", "template: marks tests as template rendering tests")
    config.addinivalue_line("markers", "navigation: marks tests as navigation tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
