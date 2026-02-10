"""
Main Admin Application

FastAPI application for V4VApp backend administration.
"""

from contextlib import asynccontextmanager
from pathlib import Path
from timeit import default_timer as timer

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from single_source import get_version

from v4vapp_backend_v2 import __version__ as project_version
from v4vapp_backend_v2.accounting.sanity_checks import run_all_sanity_checks
from v4vapp_backend_v2.admin.data_helpers import admin_data_helper
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers import v4vconfig
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn

# LND and accounting helpers used on dashboard

ADMIN_VERSION = get_version(__name__, Path(__file__).parent, default_return="1.1.0") or "1.1.0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Access config_filename from app.state
    config_filename = app.state.config_filename
    InternalConfig(config_filename=config_filename)
    db_conn = DBConn()
    await db_conn.setup_database()
    logger.info("Admin Interface and API started", extra={"notification": False})
    yield


class AdminApp:
    """Main admin application class"""

    def __init__(self, config_filename: str = "devhive.config.yaml"):
        InternalConfig(config_filename=config_filename)
        self.app = FastAPI(
            lifespan=lifespan,
            title="V4VApp Admin Interface",
            description="Administration interface for V4VApp backend services",
            version=ADMIN_VERSION,
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
            """
            Render the Admin Dashboard page.

            Asynchronous request handler that composes the context required to render the
            "dashboard.html" template. The handler performs the following high-level steps:

            - Runs sanity checks (via `log_all_sanity_checks`) and includes results in the
                context (failures are logged; notifications suppressed).
            - Loads server and admin configuration from `InternalConfig`.
            - For each highlighted user in the configuration:
                    - Calls `account_hive_balances` inside `run_in_threadpool` (may perform
                        blocking I/O).
                    - Normalizes balance keys to preferred forms ("HIVE", "HBD").
                    - Attempts to coerce balance values to floats using multiple fallbacks:
                            2. Using an `amount` attribute or converting the value's string form
                            3. Parsing the leading numeric token in a string (commas stripped)
                        If coercion fails the numeric value is recorded as None; on retrieval
                        errors the entry for that user is an `{"error": <message>}` dict.
                    - Adds formatted display strings ("HIVE_fmt", "HBD_fmt") using 3 decimal
                        places where possible (falls back to the raw string on formatting error).
            - Retrieves pending transactions via `PendingTransaction.list_all_str()`.
            - If the configured server account is among the highlighted users and its
                balance retrieval succeeded, attempts to verify the "Customer Deposits Hive"
                wallet balances using `one_account_balance` and Decimal comparison with a
                tolerance of 0.001. The verification result is encoded as
                `server_balance_check` with values like:
                    {"status": "match", "icon": "✅"},
                    {"status": "mismatch", "icon": "❌"},
                Verification failures are logged with `extra={"notification": False}`.

            Returned template context includes:
            - request: the incoming Request
            - title: "Admin Dashboard"
            - nav_items: navigation items for the UI
            - hive_balances: mapping of highlighted users to balance info or error dicts
            - pending_transactions: pending transaction list strings
            - sanity_results: results from sanity checks
            - admin_info: dict with keys:
                    - version (str), project_version (must be defined in the surrounding scope),
                    - config_file, server_account, server_balance_check, local_machine_name
            - lnd_info: any LND-related info collected by the admin helper

            - Most exceptions are handled locally; error details are encoded in the returned
                context rather than propagated.
            - Relies on external helpers/objects being present: `log_all_sanity_checks`,
                `run_in_threadpool`, `account_hive_balances`, `InternalConfig`,
                `PendingTransaction`, `AssetAccount`, `one_account_balance`, `Currency`,
                `Decimal`, `logger`, `self.templates`, and `self.nav_manager`.
            - This function is async and returns a TemplateResponse (via
                `self.templates.TemplateResponse`).

            """
            start = timer()
            nav_items = self.nav_manager.get_navigation_items()
            admin_data = await admin_data_helper()
            server_id = InternalConfig().server_id

            return self.templates.TemplateResponse(
                "dashboard.html",
                {
                    "request": request,
                    "title": "Admin Dashboard",
                    "nav_items": nav_items,
                    "hive_balances": admin_data.hive_balances,
                    "pending_transactions": admin_data.pending_transactions,
                    "sanity_results": admin_data.sanity_results,
                    "admin_info": {
                        "admin_version": ADMIN_VERSION,
                        "project_version": project_version,
                        "config_file": self.config.config_filename,
                        "server_account": server_id,
                        "server_balance_check": admin_data.server_balance_check,
                        "local_machine_name": InternalConfig().local_machine_name,
                    },
                    "lnd_info": admin_data.lnd_info,
                    "load_time": timer() - start,
                },
            )

        @self.app.get("/", response_class=RedirectResponse)
        async def root_redirect():
            """Redirect root to admin"""
            return RedirectResponse(url="/admin", status_code=302)

        @self.app.get("/status")
        @self.app.get("/admin/health")
        async def health_check() -> JSONResponse:
            """Health check endpoint"""
            start = timer()
            sanity_results = await run_all_sanity_checks()
            if sanity_results.failed:
                response_status = status.HTTP_503_SERVICE_UNAVAILABLE
            else:
                response_status = status.HTTP_200_OK

            payload = {
                "status": "FAIL" if sanity_results.failed else "OK",
                "admin_version": ADMIN_VERSION,
                "project_version": project_version,
                "config": self.config.config_filename,
                "local_machine_name": InternalConfig().local_machine_name,
                "server_id": InternalConfig().server_id,
                # encode the sanity object into JSON-serializable data
                "sanity_checks": jsonable_encoder(sanity_results.model_dump()),
                "load_time": timer() - start,
            }
            return JSONResponse(content=payload, status_code=response_status)


def create_admin_app(config_filename: str = "devhive.config.yaml") -> FastAPI:
    """Factory function to create admin app"""
    admin = AdminApp(config_filename=config_filename)
    return admin.app


# For running directly
if __name__ == "__main__":
    import uvicorn

    app = create_admin_app()
    uvicorn.run(app, host="127.0.0.1", port=8080, reload=True)
