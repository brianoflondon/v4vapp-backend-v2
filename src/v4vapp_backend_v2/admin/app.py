"""
Main Admin Application

FastAPI application for V4VApp backend administration.
"""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers import v4vconfig
from v4vapp_backend_v2.config.setup import InternalConfig


class AdminApp:
    """Main admin application class"""

    def __init__(self, config_filename: str = "devhive.config.yaml"):
        self.app = FastAPI(
            title="V4VApp Admin Interface",
            description="Administration interface for V4VApp backend services",
            version="1.0.0",
            docs_url="/admin/docs",
            redoc_url="/admin/redoc",
        )

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
        # V4V Config router
        self.app.include_router(
            v4vconfig.router, prefix="/admin/v4vconfig", tags=["V4V Configuration"]
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

            return self.templates.TemplateResponse(
                "dashboard.html",
                {
                    "request": request,
                    "title": "Admin Dashboard",
                    "nav_items": nav_items,
                    "admin_info": {
                        "version": "1.0.0",
                        "config_file": self.config.config_filename,
                        "server_account": getattr(self.config, "server_account", "N/A"),
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
            return {"status": "healthy", "version": "1.0.0", "config": self.config.config_filename}


def create_admin_app(config_filename: str = "devhive.config.yaml") -> FastAPI:
    """Factory function to create admin app"""
    admin = AdminApp(config_filename=config_filename)
    return admin.app


# For running directly
if __name__ == "__main__":
    import uvicorn

    app = create_admin_app()
    uvicorn.run(app, host="127.0.0.1", port=8080, reload=True)
    uvicorn.run(app, host="127.0.0.1", port=8080, reload=True)
