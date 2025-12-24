"""
Main Admin Application

FastAPI application for V4VApp backend administration.
"""

from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2 import __version__ as project_version
from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.sanity_checks import log_all_sanity_checks
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers import v4vconfig
from v4vapp_backend_v2.config.decorators import async_time_stats_decorator
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive.hive_extras import account_hive_balances
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction

# LND and accounting helpers used on dashboard
from v4vapp_backend_v2.models.lnd_balance_models import NodeBalances


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Access config_filename from app.state
    config_filename = app.state.config_filename
    InternalConfig(config_filename=config_filename, log_filename="admin_v2.jsonl")
    db_conn = DBConn()
    await db_conn.setup_database()
    logger.info("Admin Interface and API started", extra={"notification": False})
    yield


class AdminApp:
    """Main admin application class"""

    def __init__(self, config_filename: str = "devhive.config.yaml"):
        InternalConfig(config_filename=config_filename, log_filename="admin_v2.jsonl")
        self.app = FastAPI(
            lifespan=lifespan,
            title="V4VApp Admin Interface",
            description="Administration interface for V4VApp backend services",
            version="1.0.0",
            docs_url="/admin/docs",
            redoc_url="/admin/redoc",
        )
        logger.info(
            f"Initializing Admin Interface on {InternalConfig().local_machine_name} {self.app.version}",
            extra={"notification": True},
        )

        # Add proxy middleware to trust headers from reverse proxy
        # This allows FastAPI to correctly detect HTTPS when behind nginx proxy
        @self.app.middleware("http")
        async def proxy_middleware(request: Request, call_next):
            # Trust common proxy headers
            if "x-forwarded-proto" in request.headers:
                request.scope["scheme"] = request.headers["x-forwarded-proto"]
            if "x-forwarded-host" in request.headers:
                request.scope["server"] = (request.headers["x-forwarded-host"], None)

            response = await call_next(request)
            return response

        # Store config_filename in app state
        self.app.state.config_filename = config_filename

        # Initialize internal config
        self.config = InternalConfig(config_filename=config_filename)

        # Setup paths
        self.admin_dir = Path(__file__).parent
        self.templates_dir = self.admin_dir / "templates"
        self.static_dir = self.admin_dir / "static"

        # Setup templates and static files
        self.templates = Jinja2Templates(directory=str(self.templates_dir))

        # Setup navigation
        self.nav_manager = NavigationManager()

        # Setup routes and middleware
        self._setup_static_files()
        self._setup_routers()
        self._setup_main_routes()

    def _setup_static_files(self):
        """Setup static file serving"""
        self.app.mount("/admin/static", StaticFiles(directory=str(self.static_dir)), name="static")

    def _setup_routers(self):
        """Setup all admin routers"""
        # Set the admin config for the v4vconfig router
        v4vconfig.set_admin_config(self.config)

        # V4V Config router
        self.app.include_router(
            v4vconfig.router, prefix="/admin/v4vconfig", tags=["V4V Configuration"]
        )

        # Accounts router
        from v4vapp_backend_v2.admin.routers import accounts

        accounts.set_templates_and_nav(self.templates, self.nav_manager)
        self.app.include_router(
            accounts.router, prefix="/admin/accounts", tags=["Account Balances"]
        )

        # Users router
        from v4vapp_backend_v2.admin.routers import users

        users.set_templates_and_nav(self.templates, self.nav_manager)
        self.app.include_router(users.router, prefix="/admin/users", tags=["Users"])

        # Financial Reports router
        from v4vapp_backend_v2.admin.routers import financial_reports

        financial_reports.set_templates_and_nav(self.templates, self.nav_manager)
        self.app.include_router(
            financial_reports.router, prefix="/admin/financial-reports", tags=["Financial Reports"]
        )

        # Ledger Entries router
        from v4vapp_backend_v2.admin.routers import ledger_entries

        ledger_entries.set_templates_and_nav(self.templates, self.nav_manager)
        self.app.include_router(
            ledger_entries.router, prefix="/admin/ledger-entries", tags=["Ledger Entries"]
        )

        # Add more routers here as needed
        # self.app.include_router(other_router, prefix="/admin/other", tags=["Other"])

    def _setup_main_routes(self):
        """Setup main admin routes"""

        @async_time_stats_decorator(runs=100)
        @self.app.get("/admin", response_class=HTMLResponse)
        @self.app.get("/admin/", response_class=HTMLResponse)
        async def admin_dashboard(request: Request):
            """
            Render the admin dashboard page.

            This asynchronous handler composes the context required to render the admin
            dashboard template. It performs the following operations:

            - Runs sanity checks via `log_all_sanity_checks` (only logs failures; notifications
                suppressed) and includes the results in the context.
            - Retrieves navigation items from `self.nav_manager`.
            - Reads `server_id` and the list of highlighted users from `InternalConfig()`.
            - For each highlighted user:
                    - Calls the potentially-blocking `account_hive_balances` inside
                        `run_in_threadpool`.
                    - Normalizes balance keys to upper-case (preferring "HIVE" and "HBD").
                    - Attempts to coerce balance values to floats using several fallbacks:
                            1. Direct float conversion
                            2. Converting an `amount` attribute or its string representation
                            3. Parsing the leading numeric token from the string (stripping commas)
                        If coercion fails the recorded value will be None.
                    - On retrieval errors, stores an `{"error": <message>}` dict for that user.
                    - Adds formatted string representations for "HIVE_fmt" and "HBD_fmt" for display,
                        using a 3-decimal format (falls back to the raw string on formatting error).
            - Fetches pending transactions via `PendingTransaction.list_all_str()`.
            - If the server account is among the highlighted users and its balance retrieval
                did not fail, attempts to verify the "Customer Deposits Hive" account balances:
                    - Builds an `AssetAccount(name="Customer Deposits Hive", sub=server_id)` and
                        calls `one_account_balance`.
                    - Compares reported HIVE and HBD balances with the actual wallet values using
                        `Decimal` with a tolerance of 0.001. Sets `server_balance_check` to one of:
                        {"status": "match", "icon": "✅"}, {"status": "mismatch", "icon": "❌"},
                        {"status": "error", "icon": "⚠️"}.
                    - Logs a warning with `extra={"notification": False}` if this verification fails.
            - Returns a TemplateResponse (via `self.templates.TemplateResponse`) rendering
                "dashboard.html" with context keys:
                    - request, title, nav_items, hive_balances, pending_transactions,
                        sanity_results, and admin_info containing:
                            version, project_version, config_file, server_account,
                            server_balance_check, local_machine_name.

            Notes:
            - The function handles most exceptions locally and encodes error information
                into the returned context rather than propagating them.
            - It relies on several external helpers and objects being available in scope:
                `log_all_sanity_checks`, `run_in_threadpool`, `account_hive_balances`,
                `InternalConfig`, `PendingTransaction`, `AssetAccount`, `one_account_balance`,
                `Currency`, `Decimal`, `logger`, `self.templates`, and `self.nav_manager`.
            - `project_version` must be defined in the surrounding scope before calling.
            """
            # node_name = InternalConfig().node_name
            # nb = NodeBalances(node=node_name)

            # async with TaskGroup() as tg:
            #     sanity_task = tg.create_task(
            #         log_all_sanity_checks(
            #             local_logger=logger, log_only_failures=True, notification=False
            #         )
            #     )
            #     # Fetch pending transactions
            #     pending_transactions_task = tg.create_task(PendingTransaction.list_all_str())
            #     # Attempt to read latest stored node balances first (fast)
            #     fetch_balances_task = tg.create_task(nb.fetch_balances())
            #     asset = AssetAccount(name="External Lightning Payments", sub=node_name)
            #     ledger_details_task = tg.create_task(one_account_balance(account=asset))

            # sanity_results = await sanity_task
            # pending_transactions = await pending_transactions_task
            # ledger_details = await ledger_details_task
            # await fetch_balances_task

            sanity_results = await log_all_sanity_checks(
                local_logger=logger, log_only_failures=True, notification=False
            )
            nav_items = self.nav_manager.get_navigation_items()
            server_id = InternalConfig().server_id
            # Gather hive account balances for display
            hive_balances: dict = {}
            for acc in InternalConfig().config.admin_config.highlight_users:
                try:
                    balances = await run_in_threadpool(account_hive_balances, acc)
                    # Normalize and convert amounts to floats for rendering/formatting
                    balances_norm: dict = {}
                    for k, v in (balances or {}).items():
                        key = str(k).upper()
                        # Prefer HIVE and HBD keys
                        if key in ("HIVE", "HBD") or key.lower() in ("hive", "hbd"):
                            # Try multiple ways to coerce to float
                            val = None
                            try:
                                val = Decimal(v)
                            except Exception:
                                try:
                                    # some Amount objects may expose 'amount' property
                                    val = Decimal(str(getattr(v, "amount", v)))
                                except Exception:
                                    try:
                                        # fallback: try parsing numeric from string
                                        val = Decimal(str(v).split()[0].replace(",", ""))
                                    except Exception:
                                        val = None
                            balances_norm[key] = val
                except Exception as e:
                    balances_norm = {"error": str(e)}
                # Add formatted string representations for display in template
                try:
                    if "error" not in balances_norm:
                        for k in ("HIVE", "HBD"):
                            val = balances_norm.get(k)
                            if val is None:
                                balances_norm[f"{k}_fmt"] = "0.000"
                            else:
                                try:
                                    balances_norm[f"{k}_fmt"] = f"{float(val):,.3f}"
                                except Exception:
                                    balances_norm[f"{k}_fmt"] = str(val)
                except Exception:
                    # keep original dict on any formatting error
                    pass
                hive_balances[acc] = balances_norm

            # Fetch pending transactions
            pending_transactions = await PendingTransaction.list_all_str()

            # Check customer deposits balance for server account
            server_balance_check = {"status": "unknown", "icon": "❓"}
            if server_id in hive_balances and "error" not in hive_balances[server_id]:
                try:
                    if "server_account_hive_balances" in [
                        name for name, _ in sanity_results.failed
                    ]:
                        server_balance_check = {"status": "mismatch", "icon": "❌"}
                    else:
                        server_balance_check = {"status": "match", "icon": "✅"}

                except Exception as e:
                    logger.warning(
                        f"Failed to check customer deposits balance: {e}",
                        extra={"notification": False},
                    )
                    server_balance_check = {"status": "error", "icon": "⚠️"}

            # LND / External balances for System Information
            lnd_info = {
                "node": None,
                "node_balance": None,
                "node_balance_fmt": "N/A",
                "external_sats": None,
                "external_sats_fmt": "N/A",
                "delta": None,
                "delta_fmt": "N/A",
            }

            try:
                # Get configured default LND node (if any)
                node_name = InternalConfig().node_name
                lnd_info["node"] = node_name
                if node_name:
                    # Attempt to read latest stored node balances first (fast)
                    try:
                        nb = NodeBalances(node=node_name)
                        await nb.fetch_balances()
                        if nb.channel and nb.channel.local_balance:
                            lnd_info["node_balance"] = int(nb.channel.local_balance.sat)

                    except Exception:
                        # Non-fatal: leave node_balance as None
                        lnd_info["node_balance"] = None

                    # External Lightning Payments asset balance (sats)
                    try:
                        asset = AssetAccount(name="External Lightning Payments", sub=node_name)
                        ledger_details = await one_account_balance(account=asset)
                        lnd_info["external_sats"] = (
                            int(ledger_details.sats)
                            if ledger_details and ledger_details.sats is not None
                            else None
                        )
                    except Exception:
                        lnd_info["external_sats"] = None

                    # Compute delta if possible
                    try:
                        if (
                            lnd_info["node_balance"] is not None
                            and lnd_info["external_sats"] is not None
                        ):
                            lnd_info["delta"] = int(
                                lnd_info["node_balance"] - lnd_info["external_sats"]
                            )
                    except Exception:
                        lnd_info["delta"] = None

                    # Formatting helpers
                    def fmt_sats(x):
                        try:
                            return f"{int(x):,}"
                        except Exception:
                            return "N/A"

                    lnd_info["node_balance_fmt"] = (
                        fmt_sats(lnd_info["node_balance"])
                        if lnd_info["node_balance"] is not None
                        else "N/A"
                    )
                    lnd_info["external_sats_fmt"] = (
                        fmt_sats(lnd_info["external_sats"])
                        if lnd_info["external_sats"] is not None
                        else "N/A"
                    )
                    lnd_info["delta_fmt"] = (
                        fmt_sats(lnd_info["delta"]) if lnd_info["delta"] is not None else "N/A"
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to fetch LND/external balances for admin dashboard: {e}",
                    extra={"notification": False},
                )
                # Use defaults in lnd_info
                pass

            return self.templates.TemplateResponse(
                "dashboard.html",
                {
                    "request": request,
                    "title": "Admin Dashboard",
                    "nav_items": nav_items,
                    "hive_balances": hive_balances,
                    "pending_transactions": pending_transactions,
                    "sanity_results": sanity_results,
                    "admin_info": {
                        "version": "1.0.0",
                        "project_version": project_version,
                        "config_file": self.config.config_filename,
                        "server_account": server_id,
                        "server_balance_check": server_balance_check,
                        "local_machine_name": InternalConfig().local_machine_name,
                    },
                    "lnd_info": lnd_info,
                },
            )

        @self.app.get("/favicon.ico", include_in_schema=False)
        async def favicon():
            """Serve favicon"""
            favicon_path = self.static_dir / "favicon.ico"
            if favicon_path.exists():
                return FileResponse(favicon_path)
            else:
                # Return a default favicon or 404
                return FileResponse(self.static_dir / "favicon.ico", status_code=404)


def create_admin_app(config_filename: str = "devhive.config.yaml") -> FastAPI:
    """Factory function to create admin app"""
    admin = AdminApp(config_filename=config_filename)
    return admin.app


# For running directly
if __name__ == "__main__":
    import uvicorn

    app = create_admin_app()
    uvicorn.run(app, host="127.0.0.1", port=8080, reload=True)

# Last line of the file
