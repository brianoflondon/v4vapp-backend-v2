from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.process.hold_release_keepsats import (
    archive_old_hold_release_keepsats_entries,
)


async def main():
    db_conn = DBConn()
    await db_conn.setup_database()

    # await archive_old_hold_release_keepsats_entries(older_than_days=0, reverse_archive=True)
    await archive_old_hold_release_keepsats_entries(older_than_days=1)


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Find and move old ledger entries to the archive")
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help=(
            "Config file1name (relative to config/ folder). "
            "If omitted, you'll be prompted to choose (default: dev server)."
        ),
    )
    args = parser.parse_args()

    # When no config was provided interactively prompt the user (default -> devhive).
    # For non-interactive sessions (CI) we fall back to the dev config.
    import sys

    def _choose_config_interactive() -> str:
        options = {
            "1": ("dev server", "devhive.config.yaml"),
            "2": ("live server", "production.fromhome.config.yaml"),
        }
        print("\nSelect which config to use (press Enter for default, or 'q' to quit):")
        for key, (label, fname) in options.items():
            default_mark = " (default)" if key == "1" else ""
            print(f"  {key}) {label} -> {fname}{default_mark}")

        if not sys.stdin.isatty():
            # not interactive (e.g., CI) — return default
            print("Non-interactive session detected; using default: devhive.config.yaml")
            return options["1"][1]

        try:
            choice = input("Enter choice [1-2, q to quit]: ").strip()
        except KeyboardInterrupt:
            # Handle Ctrl-C cleanly
            print("\nAborted by user (Ctrl-C). Exiting.")
            sys.exit(0)

        # explicit quit handling
        if choice.lower() in ("q", "quit", "exit"):
            print("User cancelled; exiting.")
            sys.exit(0)

        if choice == "":
            return options["1"][1]
        if choice in options:
            return options[choice][1]

        # accept short names or filename fragments
        low = choice.lower()
        if low.startswith("dev"):
            return options["1"][1]
        if low.startswith("live") or low.startswith("prod"):
            return options["2"][1]

        # Unknown input — warn and default
        print(f"Unknown selection '{choice}', defaulting to devhive.config.yaml")
        return options["1"][1]

    if args.config is None:
        args.config = _choose_config_interactive()
    else:
        # allow short names like 'dev' / 'live' when passed on the CLI
        cfg_l = args.config.strip().lower()
        if cfg_l in ("dev", "devhive"):
            args.config = "devhive.config.yaml"
        elif cfg_l in ("live", "production", "prod"):
            args.config = "production.fromhome.config.yaml"

    # InternalConfig is a singleton — the FIRST call with a config_filename wins.
    # None of the top-level imports trigger InternalConfig(), so this is guaranteed
    # to be the first call.
    InternalConfig(config_filename=args.config)

    asyncio.run(main())
