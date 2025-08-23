"""
V4V Configuration Router

FastAPI router for managing V4VApp configuration settings.
"""

import json
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError

from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive.v4v_config import V4VConfig, V4VConfigData, V4VConfigRateLimits

# Setup router and templates
router = APIRouter()
templates_dir = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))
nav_manager = NavigationManager()


def get_v4v_config() -> V4VConfig:
    """Get V4VConfig instance"""
    # You might want to make this configurable or get from dependency injection
    return V4VConfig(server_accname="v4vapp")


@router.get("/", response_class=HTMLResponse)
async def v4vconfig_dashboard(request: Request):
    """V4V Configuration dashboard"""
    try:
        config = get_v4v_config()
        config.check()  # Ensure we have fresh data

        nav_items = nav_manager.get_navigation_items(str(request.url.path))
        breadcrumbs = nav_manager.get_breadcrumbs(str(request.url.path))

        return templates.TemplateResponse(
            "v4vconfig/dashboard.html",
            {
                "request": request,
                "title": "V4V Configuration",
                "nav_items": nav_items,
                "breadcrumbs": breadcrumbs,
                "config": config.data,
                "timestamp": config.timestamp,
                "server_account": config.server_accname,
            },
        )

    except Exception as e:
        logger.error(f"Error loading V4V config dashboard: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load configuration: {e}")


@router.get("/api")
async def get_v4vconfig_api():
    """API endpoint to get current V4V configuration as JSON"""
    try:
        config = get_v4v_config()
        config.check()

        return {
            "success": True,
            "config": config.data.model_dump(),
            "timestamp": config.timestamp.isoformat() if config.timestamp else None,
            "server_account": config.server_accname,
        }

    except Exception as e:
        logger.error(f"Error getting V4V config via API: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get configuration: {e}")


@router.post("/api")
async def update_v4vconfig_api(new_config: V4VConfigData):
    """API endpoint to update V4V configuration"""
    try:
        config = get_v4v_config()

        # Validate the new configuration
        validated_config = V4VConfigData.model_validate(new_config.model_dump())

        # Store old config for logging
        old_config = config.data.model_copy() if config.data else None

        # Update the configuration
        config.data = validated_config

        # Save to Hive
        await config.put()

        logger.info(
            "V4V Configuration updated via admin API",
            extra={
                "old_config": old_config.model_dump() if old_config else None,
                "new_config": validated_config.model_dump(),
                "server_account": config.server_accname,
            },
        )

        return {
            "success": True,
            "message": "Configuration updated successfully",
            "config": validated_config.model_dump(),
        }

    except ValidationError as e:
        logger.warning(f"V4V config validation error: {e}")
        raise HTTPException(status_code=400, detail=f"Validation error: {e}")
    except Exception as e:
        logger.error(f"Error updating V4V configuration: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update configuration: {e}")


@router.post("/update", response_class=HTMLResponse)
async def update_v4vconfig_form(
    request: Request,
    hive_return_fee: float = Form(...),
    conv_fee_percent: float = Form(...),
    conv_fee_sats: int = Form(...),
    minimum_invoice_payment_sats: int = Form(...),
    maximum_invoice_payment_sats: int = Form(...),
    max_acceptable_lnd_fee_msats: int = Form(...),
    closed_get_lnd: bool = Form(False),
    closed_get_hive: bool = Form(False),
    v4v_frontend_iri: str = Form(""),
    v4v_api_iri: str = Form(""),
    v4v_fees_streaming_sats_to_hive_percent: float = Form(...),
    dynamic_fees_url: str = Form(""),
    dynamic_fees_permlink: str = Form(""),
    rate_limits_json: str = Form("{}"),
):
    """Handle form submission for V4V configuration updates"""
    try:
        config = get_v4v_config()

        # Parse rate limits
        try:
            rate_limits_data = json.loads(rate_limits_json) if rate_limits_json.strip() else []
            rate_limits = [V4VConfigRateLimits(**rl) for rl in rate_limits_data]
        except (json.JSONDecodeError, ValidationError) as e:
            logger.warning(f"Rate limits parsing error, keeping existing: {e}")
            # Keep existing rate limits if parsing fails
            current_config = config.data
            rate_limits = current_config.lightning_rate_limits if current_config else []

        # Create new configuration
        new_config = V4VConfigData(
            hive_return_fee=hive_return_fee,
            conv_fee_percent=conv_fee_percent,
            conv_fee_sats=conv_fee_sats,
            minimum_invoice_payment_sats=minimum_invoice_payment_sats,
            maximum_invoice_payment_sats=maximum_invoice_payment_sats,
            max_acceptable_lnd_fee_msats=max_acceptable_lnd_fee_msats,
            closed_get_lnd=closed_get_lnd,
            closed_get_hive=closed_get_hive,
            v4v_frontend_iri=v4v_frontend_iri,
            v4v_api_iri=v4v_api_iri,
            v4v_fees_streaming_sats_to_hive_percent=v4v_fees_streaming_sats_to_hive_percent,
            lightning_rate_limits=rate_limits,
            dynamic_fees_url=dynamic_fees_url,
            dynamic_fees_permlink=dynamic_fees_permlink,
        )

        # Update configuration
        old_config = config.data.model_copy() if config.data else None
        config.data = new_config
        await config.put()

        logger.info(
            "V4V Configuration updated via admin form",
            extra={
                "old_config": old_config.model_dump() if old_config else None,
                "new_config": new_config.model_dump(),
                "server_account": config.server_accname,
            },
        )

        return RedirectResponse(url="/admin/v4vconfig?success=1", status_code=303)

    except ValidationError as e:
        logger.error(f"V4V config form validation error: {e}")
        nav_items = nav_manager.get_navigation_items(str(request.url.path))
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "title": "Configuration Error",
                "nav_items": nav_items,
                "error": f"Validation error: {e}",
                "back_url": "/admin/v4vconfig",
            },
        )
    except Exception as e:
        logger.error(f"V4V config form update error: {e}")
        nav_items = nav_manager.get_navigation_items(str(request.url.path))
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "title": "Configuration Error",
                "nav_items": nav_items,
                "error": str(e),
                "back_url": "/admin/v4vconfig",
            },
        )


@router.get("/refresh")
async def refresh_v4vconfig():
    """Force refresh V4V configuration from Hive"""
    try:
        config = get_v4v_config()
        config.fetch()
        logger.info("V4V Configuration refreshed from Hive via admin interface")
        return RedirectResponse(url="/admin/v4vconfig?refreshed=1", status_code=303)
    except Exception as e:
        logger.error(f"Error refreshing V4V config: {e}")
        return RedirectResponse(url="/admin/v4vconfig?error=refresh_failed", status_code=303)


@router.post("/validate")
async def validate_v4vconfig(config_data: Dict[str, Any]):
    """Validate V4V configuration without saving"""
    try:
        V4VConfigData.model_validate(config_data)
        return {"valid": True, "message": "Configuration is valid"}
    except ValidationError as e:
        return {"valid": False, "errors": e.errors()}
