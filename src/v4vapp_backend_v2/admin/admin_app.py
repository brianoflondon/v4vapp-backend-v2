"""
Main Admin Application

FastAPI application for V4VApp backend administration.
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2 import __version__ as project_version
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers import v4vconfig
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive.hive_extras import account_hive_balances


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

        # Financial Reports router
        from v4vapp_backend_v2.admin.routers import financial_reports

        financial_reports.set_templates_and_nav(self.templates, self.nav_manager)
        self.app.include_router(
            financial_reports.router, prefix="/admin/financial-reports", tags=["Financial Reports"]
        )

        # Add more routers here as needed
        # self.app.include_router(other_router, prefix="/admin/other", tags=["Other"])

    def _setup_main_routes(self):
        """Setup main admin routes"""

        @self.app.get("/admin", response_class=HTMLResponse)
        @self.app.get("/admin/", response_class=HTMLResponse)
        async def admin_dashboard(request: Request):
            """Main admin dashboard"""
            nav_items = self.nav_manager.get_navigation_items()
            server_id = InternalConfig().server_id
            # Gather hive account balances for display
            hive_accounts = getattr(InternalConfig().config.hive, "hive_accs", []) or []
            hive_balances: dict = {}
            for acc in hive_accounts:
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
            return self.templates.TemplateResponse(
                "dashboard.html",
                {
                    "request": request,
                    "title": "Admin Dashboard",
                    "nav_items": nav_items,
                    "hive_balances": hive_balances,
                    "admin_info": {
                        "version": "1.0.0",
                        "project_version": project_version,
                        "config_file": self.config.config_filename,
                        "server_account": server_id,
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
            return {
                "status": "healthy",
                "admin_version": "1.0.0",
                "project_version": project_version,
                "config": self.config.config_filename,
            }


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
