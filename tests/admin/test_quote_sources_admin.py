"""Admin page for turning quote services on and off."""

import inspect
import re
from datetime import UTC, datetime

import pytest
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.testclient import TestClient

from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults
from v4vapp_backend_v2.admin.admin_app import AdminApp
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers import quote_sources
from v4vapp_backend_v2.hive.v4v_config import (
    QUOTE_SERVICE_NAMES,
    V4VConfigData,
    normalize_quote_service_flags,
)

TEMPLATES = "src/v4vapp_backend_v2/admin/templates"
STATIC = "src/v4vapp_backend_v2/admin/static"


class FakeConfig:
    def __init__(self):
        self.data = V4VConfigData()
        self.server_accname = "devser.v4vapp"
        self.timestamp = datetime(2026, 9, 29, tzinfo=UTC)
        self.loaded_quote_services_from_hive = True
        self.saved = None
        self.hive_result = True

    def check(self):
        return None

    def fetch(self):
        return self.loaded_quote_services_from_hive

    async def update_quote_services(self, flags):
        normalized = normalize_quote_service_flags(flags)
        if not any(normalized.values()):
            raise ValueError("At least one quote service must stay enabled")
        self.saved = normalized
        self.data.quote_services = dict(normalized)
        return self.hive_result


@pytest.fixture
def fake_config(monkeypatch):
    fake = FakeConfig()

    async def no_pending():
        return []

    async def no_sanity():
        return SanityCheckResults()

    monkeypatch.setattr(quote_sources, "get_v4v_config", lambda: fake)
    monkeypatch.setattr(quote_sources.PendingTransaction, "list_all_str", no_pending)
    monkeypatch.setattr(quote_sources, "run_all_sanity_checks", no_sanity)
    monkeypatch.setattr(
        quote_sources,
        "read_quote_service_flags",
        lambda: normalize_quote_service_flags(fake.data.quote_services),
    )
    monkeypatch.setattr(
        quote_sources,
        "publish_quote_service_flags",
        lambda flags, overwrite=True: None,
    )
    return fake


@pytest.fixture
def client(fake_config):
    app = FastAPI()
    app.mount("/admin/static", StaticFiles(directory=STATIC), name="static")
    templates = Jinja2Templates(directory=TEMPLATES)
    templates.env.globals["sidebar_color"] = "#2563eb"
    templates.env.globals["favicon_path"] = "/admin/static/favicon/favicon_dev/favicon.ico"
    templates.env.globals["favicon_manifest"] = ""
    quote_sources.set_templates_and_nav(templates, NavigationManager())
    app.include_router(quote_sources.router, prefix="/admin/quote-sources")
    with TestClient(app) as test_client:
        yield test_client


def _input_tag(html: str, service: str) -> str:
    match = re.search(rf'<input[^>]*id="quote-{service}"[^>]*>', html, re.DOTALL)
    assert match, f"missing switch for {service}"
    return match.group(0)


def test_navigation_places_quote_sources_after_v4v_configuration():
    items = NavigationManager().get_navigation_items("/admin/quote-sources")
    names = [item.name for item in items]
    assert names.index("Quote Sources") == names.index("V4V Configuration") + 1
    quote = next(item for item in items if item.name == "Quote Sources")
    assert quote.active
    assert quote.url == "/admin/quote-sources"
    assert quote.badge == "Prices"
    v4v = next(item for item in items if item.name == "V4V Configuration")
    assert v4v.active is False
    crumbs = NavigationManager().get_breadcrumbs("/admin/quote-sources")
    assert crumbs[-1]["name"] == "Quote Sources"


def test_admin_app_mounts_quote_sources_router():
    source = inspect.getsource(AdminApp._setup_routers)
    assert "quote_sources" in source
    assert "/admin/quote-sources" in source


def test_quote_sources_page_lists_every_service_on(client):
    response = client.get("/admin/quote-sources")
    assert response.status_code == 200
    html = response.text
    assert "Quote Sources" in html
    assert "rate-limits this server" in html
    for name in QUOTE_SERVICE_NAMES:
        assert "checked" in _input_tag(html, name)
    assert "/admin/quote-sources" in html


def test_quote_sources_page_shows_drift(client, fake_config, monkeypatch):
    monkeypatch.setattr(
        quote_sources,
        "read_quote_service_flags",
        lambda: {**default_flags(), "CoinGecko": False},
    )
    response = client.get("/admin/quote-sources")
    assert response.status_code == 200
    assert "do not match" in response.text
    active = response.text.split("Services called on the next quote fetch", 1)[1]
    active = active.split("</p>", 1)[0]
    assert "CoinGecko" not in active


def default_flags():
    return normalize_quote_service_flags({})


def test_post_turns_coingecko_off(client, fake_config):
    response = client.post(
        "/admin/quote-sources/update",
        data={"enabled": ["Binance", "CoinMarketCap", "HiveInternalMarket"]},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/admin/quote-sources?success=1"
    assert fake_config.saved["CoinGecko"] is False
    assert fake_config.saved["Binance"] is True
    assert fake_config.saved["HiveInternalMarket"] is True


def test_post_rejects_turning_every_service_off(client, fake_config):
    response = client.post("/admin/quote-sources/update", data={})
    assert response.status_code == 200
    assert "At least one quote service must stay enabled" in response.text
    assert fake_config.saved is None
    for name in QUOTE_SERVICE_NAMES:
        assert "checked" not in _input_tag(response.text, name)


def test_hive_save_failure_warns_and_keeps_the_selection(client, fake_config):
    fake_config.hive_result = False
    response = client.post(
        "/admin/quote-sources/update",
        data={"enabled": ["Binance", "CoinMarketCap", "HiveInternalMarket"]},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "hive_warning=1" in response.headers["location"]

    page = client.get("/admin/quote-sources?success=1&hive_warning=1")
    assert page.status_code == 200
    assert "saving it on Hive failed" in page.text
    assert "Configuration updated successfully" in page.text
    assert "checked" not in _input_tag(page.text, "CoinGecko")
    assert "checked" in _input_tag(page.text, "Binance")


def test_refresh_publishes_hive_flags(client, fake_config, monkeypatch):
    published = []

    def publish(flags, overwrite=True):
        published.append((dict(flags), overwrite))

    monkeypatch.setattr(quote_sources, "publish_quote_service_flags", publish)
    fake_config.data.quote_services = normalize_quote_service_flags({"CoinGecko": False})
    response = client.get("/admin/quote-sources/refresh", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"].endswith("?refreshed=1")
    assert published[0][1] is True
    assert published[0][0]["CoinGecko"] is False


def test_refresh_does_not_overwrite_redis_when_hive_fetch_fails(client, fake_config, monkeypatch):
    published = []
    monkeypatch.setattr(
        quote_sources,
        "publish_quote_service_flags",
        lambda flags, overwrite=True: published.append(flags),
    )
    fake_config.loaded_quote_services_from_hive = False
    response = client.get("/admin/quote-sources/refresh", follow_redirects=False)
    assert response.status_code == 303
    assert "error=refresh_failed" in response.headers["location"]
    assert published == []

    page = client.get("/admin/quote-sources?error=refresh_failed")
    assert "left as they are" in page.text


def test_api_can_turn_one_service_off(client, fake_config):
    response = client.post("/admin/quote-sources/api", json={"CoinGecko": False})
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["hive_saved"] is True
    assert body["quote_services"]["CoinGecko"] is False
    assert body["quote_services"]["Binance"] is True
    assert body["effective"]["CoinGecko"] is False


def test_api_rejects_all_off(client, fake_config):
    response = client.post(
        "/admin/quote-sources/api",
        json={name: False for name in QUOTE_SERVICE_NAMES},
    )
    assert response.status_code == 400
    assert fake_config.saved is None
