"""
Main Admin Application

FastAPI application for V4VApp backend administration.
"""

from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2 import __version__ as project_version
from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.sanity_checks import log_all_sanity_checks, run_all_sanity_checks
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers import v4vconfig
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive.hive_extras import account_hive_balances
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction


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

        @self.app.get("/admin", response_class=HTMLResponse)
        @self.app.get("/admin/", response_class=HTMLResponse)
        async def admin_dashboard(request: Request):
            """Main admin dashboard"""
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
                                val = float(v)
                            except Exception:
                                try:
                                    # some Amount objects may expose 'amount' property
                                    val = float(str(getattr(v, "amount", v)))
                                except Exception:
                                    try:
                                        # fallback: try parsing numeric from string
                                        val = float(str(v).split()[0].replace(",", ""))
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
                    # Get customer deposits balance
                    customer_deposits_account = AssetAccount(
                        name="Customer Deposits Hive", sub=server_id
                    )
                    deposits_details = await one_account_balance(customer_deposits_account)

                    # Get balances with tolerance
                    hive_deposits = deposits_details.balances_net.get(Currency.HIVE, 0.0)
                    hbd_deposits = deposits_details.balances_net.get(Currency.HBD, 0.0)

                    hive_actual = hive_balances[server_id].get("HIVE", 0.0)
                    hbd_actual = hive_balances[server_id].get("HBD", 0.0)

                    # Check with tolerance
                    tolerance = Decimal(0.001)
                    hive_match = abs(Decimal(hive_deposits) - Decimal(hive_actual)) <= tolerance
                    hbd_match = abs(Decimal(hbd_deposits) - Decimal(hbd_actual)) <= tolerance

                    if hive_match and hbd_match:
                        server_balance_check = {"status": "match", "icon": "✅"}
                    else:
                        server_balance_check = {"status": "mismatch", "icon": "❌"}

                except Exception as e:
                    logger.warning(
                        f"Failed to check customer deposits balance: {e}",
                        extra={"notification": False},
                    )
                    server_balance_check = {"status": "error", "icon": "⚠️"}

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
                },
            )

        @self.app.get("/", response_class=RedirectResponse)
        async def root_redirect():
            """Redirect root to admin"""
            return RedirectResponse(url="/admin", status_code=302)

        @self.app.get("/admin/health")
        async def health_check():
            """Health check endpoint"""
            sanity_results = await run_all_sanity_checks()
            return {
                "status": "healthy",
                "admin_version": "1.0.0",
                "project_version": project_version,
                "config": self.config.config_filename,
                "local_machine_name": InternalConfig().local_machine_name,
                "server_id": InternalConfig().server_id,
                "sanity_checks": sanity_results,
            }

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
