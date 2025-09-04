"""
Financial Reports Router

Handles routes for displaying financial reports including balance sheet, profit and loss, and comprehensive account reports.
"""

from typing import Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    list_all_accounts,
)
from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    generate_balance_sheet_mongodb,
)
from v4vapp_backend_v2.accounting.ledger_entries import get_ledger_entries
from v4vapp_backend_v2.accounting.profit_and_loss import (
    generate_profit_and_loss_report,
    profit_and_loss_printout,
)
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction

router = APIRouter()

# Will be set by the main app
templates: Optional[Jinja2Templates] = None
nav_manager: Optional[NavigationManager] = None


def set_templates_and_nav(tmpl: Jinja2Templates, nav: NavigationManager):
    """Set the templates and navigation manager"""
    global templates, nav_manager
    templates = tmpl
    nav_manager = nav


@router.get("/", response_class=HTMLResponse)
async def financial_reports_page(request: Request):
    """Main financial reports page with report selector"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    nav_items = nav_manager.get_navigation_items("/admin/financial-reports")

    return templates.TemplateResponse(
        "financial_reports/index.html",
        {
            "request": request,
            "title": "Financial Reports",
            "nav_items": nav_items,
            "pending_transactions": await PendingTransaction.list_all_str(),
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Financial Reports", "url": "/admin/financial-reports"},
            ],
        },
    )


@router.get("/balance-sheet", response_class=HTMLResponse)
async def balance_sheet_page(request: Request):
    """Balance Sheet page"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Generate balance sheet
        balance_sheet = await generate_balance_sheet_mongodb()
        balance_sheet_currencies_str = balance_sheet_all_currencies_printout(balance_sheet)

        # Convert datetime to string for JSON serialization
        balance_sheet_for_template = balance_sheet.copy()
        if "as_of_date" in balance_sheet_for_template:
            balance_sheet_for_template["as_of_date"] = balance_sheet_for_template[
                "as_of_date"
            ].isoformat()

        nav_items = nav_manager.get_navigation_items("/admin/financial-reports")

        return templates.TemplateResponse(
            "financial_reports/balance_sheet.html",
            {
                "request": request,
                "title": "Balance Sheet",
                "nav_items": nav_items,
                "balance_sheet_text": balance_sheet_currencies_str,
                "balance_sheet_data": balance_sheet_for_template,
                "pending_transactions": await PendingTransaction.list_all(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Financial Reports", "url": "/admin/financial-reports"},
                    {"name": "Balance Sheet", "url": "/admin/financial-reports/balance-sheet"},
                ],
            },
        )
    except Exception as e:
        nav_items = nav_manager.get_navigation_items("/admin/financial-reports")
        return templates.TemplateResponse(
            "financial_reports/error.html",
            {
                "request": request,
                "title": "Balance Sheet Error",
                "nav_items": nav_items,
                "error": str(e),
                "report_type": "Balance Sheet",
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Financial Reports", "url": "/admin/financial-reports"},
                    {"name": "Error", "url": "#"},
                ],
            },
        )


@router.get("/profit-loss", response_class=HTMLResponse)
async def profit_loss_page(request: Request):
    """Profit and Loss page"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Generate profit and loss report
        pl_report = await generate_profit_and_loss_report()
        profit_loss_str = await profit_and_loss_printout(pl_report=pl_report)

        nav_items = nav_manager.get_navigation_items("/admin/financial-reports")

        return templates.TemplateResponse(
            "financial_reports/profit_loss.html",
            {
                "request": request,
                "title": "Profit & Loss",
                "nav_items": nav_items,
                "profit_loss_text": profit_loss_str,
                "profit_loss_data": pl_report,
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Financial Reports", "url": "/admin/financial-reports"},
                    {"name": "Profit & Loss", "url": "/admin/financial-reports/profit-loss"},
                ],
            },
        )
    except Exception as e:
        nav_items = nav_manager.get_navigation_items("/admin/financial-reports")
        return templates.TemplateResponse(
            "financial_reports/error.html",
            {
                "request": request,
                "title": "Profit & Loss Error",
                "nav_items": nav_items,
                "error": str(e),
                "report_type": "Profit & Loss",
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Financial Reports", "url": "/admin/financial-reports"},
                    {"name": "Error", "url": "#"},
                ],
            },
        )


@router.get("/complete-report", response_class=HTMLResponse)
async def complete_report_page(
    request: Request,
    include_ledger_entries: bool = False,
):
    """Complete financial report page - includes balance sheet, P&L, and all account balances"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Generate balance sheet
        balance_sheet = await generate_balance_sheet_mongodb()
        balance_sheet_currencies_str = balance_sheet_all_currencies_printout(balance_sheet)

        # Generate profit and loss
        pl_report = await generate_profit_and_loss_report()
        profit_loss_str = await profit_and_loss_printout(pl_report=pl_report)

        # Get all accounts and their balances
        all_accounts = await list_all_accounts()
        account_balances = []

        for account in all_accounts:
            printout, details = await account_balance_printout(
                account=account,
                line_items=True,
                user_memos=True,
            )
            account_balances.append({"account": account, "printout": printout, "details": details})

        # Optionally get ledger entries
        ledger_entries_text = ""
        if include_ledger_entries:
            try:
                ledger_entries = await get_ledger_entries()
                ledger_entries_text = "\n".join(str(entry) for entry in ledger_entries)
            except Exception:
                ledger_entries_text = "Error loading ledger entries"

        nav_items = nav_manager.get_navigation_items("/admin/financial-reports")

        return templates.TemplateResponse(
            "financial_reports/complete_report.html",
            {
                "request": request,
                "title": "Complete Financial Report",
                "nav_items": nav_items,
                "balance_sheet_text": balance_sheet_currencies_str,
                "profit_loss_text": profit_loss_str,
                "account_balances": account_balances,
                "ledger_entries_text": ledger_entries_text,
                "include_ledger_entries": include_ledger_entries,
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Financial Reports", "url": "/admin/financial-reports"},
                    {"name": "Complete Report", "url": "/admin/financial-reports/complete-report"},
                ],
            },
        )
    except Exception as e:
        nav_items = nav_manager.get_navigation_items("/admin/financial-reports")
        return templates.TemplateResponse(
            "financial_reports/error.html",
            {
                "request": request,
                "title": "Complete Report Error",
                "nav_items": nav_items,
                "error": str(e),
                "report_type": "Complete Financial Report",
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Financial Reports", "url": "/admin/financial-reports"},
                    {"name": "Error", "url": "#"},
                ],
            },
        )


@router.post("/complete-report", response_class=HTMLResponse)
async def complete_report_post(
    request: Request,
    include_ledger_entries: Optional[str] = Form("false"),
):
    """Handle form submission for complete report options"""
    include_entries = include_ledger_entries and include_ledger_entries.lower() in (
        "true",
        "on",
        "1",
    )

    # Redirect to GET with query parameter
    from fastapi.responses import RedirectResponse

    redirect_url = (
        f"/admin/financial-reports/complete-report?include_ledger_entries={include_entries}"
    )
    return RedirectResponse(url=redirect_url, status_code=302)
