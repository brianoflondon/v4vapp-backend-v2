#!/usr/bin/env python3
"""
V4VApp Admin Server Runner

Standalone script to run the V4VApp admin interface.
"""

import argparse
import sys
from pathlib import Path

import uvicorn

from v4vapp_backend_v2.admin.admin_app import create_admin_app
from v4vapp_backend_v2.config.setup import logger

# Add the src directory to the path
src_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(src_dir))


def main():
    """Main entry point for the admin server"""
    parser = argparse.ArgumentParser(
        description="V4VApp Admin Interface Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_admin.py                              # Run with defaults
  python run_admin.py --host 0.0.0.0 --port 8080  # Run on all interfaces
  python run_admin.py --config production.yaml    # Use different config
        """,
    )

    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind to (default: 8080)")
    parser.add_argument(
        "--config",
        default="devdocker.config.yaml",
        help="Configuration file to use (default: devdocker.config.yaml)",
    )
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument(
        "--log-level",
        default="warning",
        choices=["critical", "error", "warning", "info", "debug"],
        help="Log level (default: warning)",
    )

    args = parser.parse_args()

    # Create the admin app
    try:
        app = create_admin_app(config_filename=args.config)
    except Exception as e:
        logger.error(f"❌ Failed to create admin app: {e}")
        sys.exit(1)

    # Print startup information
    logger.info("🚀 Starting V4VApp Admin Interface")
    logger.info(f"📁 Config file: {args.config}")
    logger.info(f"🌐 Server: http://{args.host}:{args.port}/admin")
    logger.info(f"📊 API Docs: http://{args.host}:{args.port}/admin/docs")
    logger.info(f"❤️ Health: http://{args.host}:{args.port}/admin/health")

    if args.reload:
        logger.info("🔄 Auto-reload enabled (development mode)")
    logger.info("-" * 50)
    logger.info("Press Ctrl+C to stop the server")
    logger.info("-" * 50)

    # Run the server
    try:
        if args.reload:
            # For reload to work, we need to pass the app as an import string
            # Create a temporary module-level app instance
            import os

            os.environ["V4VAPP_ADMIN_CONFIG"] = args.config

            uvicorn.run(
                "v4vapp_backend_v2.admin.run_admin:app",
                host=args.host,
                port=args.port,
                reload=args.reload,
                log_level=args.log_level,
                access_log=True,
                log_config=None,
            )
        else:
            uvicorn.run(
                app,
                host=args.host,
                port=args.port,
                reload=False,
                log_level=args.log_level,
                access_log=True,
            )
    except KeyboardInterrupt:
        logger.info("\n👋 Server stopped")
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        sys.exit(1)


# Module-level app instance for reload mode
app = None


def get_app():
    """Get or create the app instance"""
    global app
    if app is None:
        import os

        config_filename = os.environ.get("V4VAPP_ADMIN_CONFIG", "devdocker.config.yaml")
        app = create_admin_app(config_filename=config_filename)
    return app


# Create app instance for reload mode
if __name__ != "__main__":
    # Only create the app instance when imported (for reload mode)
    # Parse command line args to get config
    import os

    if len(sys.argv) > 1:
        # Parse args when running as module
        parser = argparse.ArgumentParser()
        parser.add_argument("--config", default="devdocker.config.yaml")
        args, _ = parser.parse_known_args()
        os.environ["V4VAPP_ADMIN_CONFIG"] = args.config

    app = get_app()

if __name__ == "__main__":
    main()
