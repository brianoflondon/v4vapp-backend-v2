"""
Quote Sources router.

Turns the external price services in AllQuotes.get_all_quotes on or off.
The switches are stored with the Hive V4V config and copied to Redis so the
next quote fetch skips a disabled service without waiting for an hourly refresh.
"""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.datastructures import FormData

from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults, run_all_sanity_checks
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_prices import (
    publish_quote_service_flags,
    read_quote_service_flags,
)
from v4vapp_backend_v2.hive.v4v_config import QUOTE_SERVICE_NAMES, normalize_quote_service_flags
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction

from .v4vconfig import get_v4v_config

router = APIRouter()
templates: Jinja2Templates | None = None
nav_manager: NavigationManager | None = None

ICON = "💱"

QUOTE_SERVICE_DETAILS: dict[str, dict[str, str]] = {
    "CoinGecko": {
        "title": "CoinGecko",
        "summary": (
            "Public prices for BTC, HIVE, and HBD. Turn this off when CoinGecko "
            "rate-limits this server. Conversions keep using Binance while that quote succeeds."
        ),
    },
    "Binance": {
        "title": "Binance",
        "summary": "Book ticker. This is the authoritative quote when the request succeeds.",
    },
    "CoinMarketCap": {
        "title": "CoinMarketCap",
        "summary": "Extra USD prices. Included in the average only when Binance has no quote.",
    },
    "HiveInternalMarket": {
        "title": "Hive internal market",
        "summary": (
            "HIVE/HBD price from the Hive internal market. When Binance is available, "
            "this rate replaces Binance's HIVE/HBD price."
        ),
    },
}


def set_templates_and_nav(tmpl: Jinja2Templates, nav: NavigationManager) -> None:
    """Set the shared templates and navigation manager."""
    global templates, nav_manager
    templates = tmpl
    nav_manager = nav


def service_rows(flags: dict[str, bool]) -> list[dict[str, str | bool]]:
    """Rows for the quote-source form, in fetch order."""
    rows: list[dict[str, str | bool]] = []
    for name in QUOTE_SERVICE_NAMES:
        detail = QUOTE_SERVICE_DETAILS[name]
        rows.append(
            {
                "name": name,
                "title": detail["title"],
                "summary": detail["summary"],
                "enabled": bool(flags.get(name, True)),
            }
        )
    return rows


def flags_from_form(form: FormData) -> dict[str, bool]:
    """Checked boxes are on. A missing checkbox is off."""
    selected = set(form.getlist("enabled"))
    return {name: name in selected for name in QUOTE_SERVICE_NAMES}


async def _render(
    request: Request,
    flags: dict[str, bool] | None = None,
    *,
    error: str | None = None,
):
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    config = get_v4v_config()
    config.check()
    stored = normalize_quote_service_flags(config.data.quote_services)
    if flags is None:
        flags = stored
    effective = read_quote_service_flags()
    nav_items = nav_manager.get_navigation_items(str(request.url.path))
    breadcrumbs = nav_manager.get_breadcrumbs(str(request.url.path))
    query_error = request.query_params.get("error")
    if error is None and query_error == "refresh_failed":
        error = (
            "Could not refresh quote sources from Hive. "
            "The running switches were left as they are."
        )
    return templates.TemplateResponse(
        request,
        "quote_sources/index.html.jinja",
        {
            "request": request,
            "title": "Quote Sources",
            "nav_items": nav_items,
            "breadcrumbs": breadcrumbs,
            "services": service_rows(flags),
            "active_titles": [
                QUOTE_SERVICE_DETAILS[name]["title"]
                for name in QUOTE_SERVICE_NAMES
                if effective.get(name, True)
            ],
            "drift": stored != effective,
            "error": error,
            "hive_warning": request.query_params.get("hive_warning"),
            "server_account": config.server_accname,
            "timestamp": config.timestamp,
            "pending_transactions": await PendingTransaction.list_all_str(),
            "sanity_results": await run_all_sanity_checks(),
        },
    )


@router.get("/", response_class=HTMLResponse)
async def quote_sources_page(request: Request):
    """Show the quote-service switches stored on Hive."""
    try:
        return await _render(request)
    except Exception as ex:
        logger.error(f"{ICON} Error loading quote sources: {ex}")
        raise HTTPException(status_code=500, detail=f"Failed to load quote sources: {ex}")


@router.post("/update", response_class=HTMLResponse)
async def update_quote_sources(request: Request):
    """Save the switches from the form."""
    form = await request.form()
    flags = flags_from_form(form)
    try:
        config = get_v4v_config()
        config.check()
        saved = await config.update_quote_services(flags)
    except ValueError as ex:
        logger.warning(f"{ICON} Rejected quote service update: {ex}")
        try:
            return await _render(request, flags, error=str(ex))
        except Exception as render_ex:
            logger.error(f"{ICON} Error re-rendering quote sources: {render_ex}")
            raise HTTPException(status_code=400, detail=str(ex))
    except Exception as ex:
        logger.error(f"{ICON} Error updating quote sources: {ex}")
        if not templates or not nav_manager:
            raise HTTPException(status_code=500, detail=f"Quote source update failed: {ex}")
        nav_items = nav_manager.get_navigation_items(str(request.url.path))
        return templates.TemplateResponse(
            request,
            "error.html.jinja",
            {
                "request": request,
                "title": "Quote Sources Error",
                "nav_items": nav_items,
                "error": f"Quote source update failed: {ex}",
                "back_url": "/admin/quote-sources",
                "pending_transactions": await PendingTransaction.list_all_str(),
                "sanity_results": SanityCheckResults(),
            },
        )

    url = "/admin/quote-sources?success=1"
    if not saved:
        url += "&hive_warning=1"
    return RedirectResponse(url=url, status_code=303)


@router.get("/refresh")
async def refresh_quote_sources():
    """Reload switches from Hive and, when that succeeds, copy them onto Redis."""
    try:
        config = get_v4v_config()
        config.fetch()
        if not getattr(config, "loaded_quote_services_from_hive", False):
            return RedirectResponse(
                url="/admin/quote-sources?error=refresh_failed",
                status_code=303,
            )
        publish_quote_service_flags(config.data.quote_services, overwrite=True)
        logger.info(f"{ICON} Quote sources refreshed from Hive")
        return RedirectResponse(url="/admin/quote-sources?refreshed=1", status_code=303)
    except Exception as ex:
        logger.error(f"{ICON} Error refreshing quote sources: {ex}")
        return RedirectResponse(url="/admin/quote-sources?error=refresh_failed", status_code=303)


@router.get("/api")
async def get_quote_sources_api():
    """Current stored switches and the set the next quote fetch will call."""
    try:
        config = get_v4v_config()
        config.check()
        stored = normalize_quote_service_flags(config.data.quote_services)
        return {
            "success": True,
            "quote_services": stored,
            "effective": read_quote_service_flags(),
            "server_account": config.server_accname,
        }
    except Exception as ex:
        logger.error(f"{ICON} Error reading quote sources: {ex}")
        raise HTTPException(status_code=500, detail=f"Failed to read quote sources: {ex}")


@router.post("/api")
async def update_quote_sources_api(payload: dict[str, bool]):
    """Update switches. Omitted services keep their current value. All-off is rejected."""
    try:
        config = get_v4v_config()
        config.check()
        current = normalize_quote_service_flags(config.data.quote_services)
        overlay = {name: payload[name] for name in QUOTE_SERVICE_NAMES if name in payload}
        saved = await config.update_quote_services({**current, **overlay})
        stored = normalize_quote_service_flags(config.data.quote_services)
        return {
            "success": True,
            "hive_saved": saved,
            "quote_services": stored,
            "effective": read_quote_service_flags(),
        }
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except Exception as ex:
        logger.error(f"{ICON} Error updating quote sources via API: {ex}")
        raise HTTPException(status_code=500, detail=f"Failed to update quote sources: {ex}")
