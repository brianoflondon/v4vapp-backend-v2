#!/usr/bin/env python3
"""
Tail and follow JSONL log files with pretty formatting.

Reads JSONL log files and displays them in a format similar to docker logs output.
Supports following files in real-time with the -f option.

Usage:
    python scripts/tail_jsonl_logs.py /path/to/log.jsonl -f
    python scripts/tail_jsonl_logs.py /path/to/log.jsonl --tail 100
    python scripts/tail_jsonl_logs.py /path/to/log.jsonl --level WARNING
"""

import asyncio
import json
import os
import re
import sys
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, List, Optional

import aiofiles
import typer
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# Add src to path for running standalone
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

app = typer.Typer(
    name="tail-jsonl",
    help="Tail and follow JSONL log files with pretty formatting",
    add_completion=False,
)


class LogLevel(str, Enum):
    """Log level filter options."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ANSI color codes for different log levels
LEVEL_COLORS = {
    "DEBUG": "\033[36m",  # Cyan
    "INFO": "\033[37m",  # White/default
    "WARNING": "\033[33m",  # Yellow
    "ERROR": "\033[31m",  # Red
    "CRITICAL": "\033[35m",  # Magenta
}
RESET_COLOR = "\033[0m"

# Log level priority for filtering
LEVEL_PRIORITY = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}


def strip_ansi_codes(text: str) -> str:
    """Remove ANSI escape codes from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


def format_log_entry(
    log_entry: dict,
    show_colors: bool = True,
    compact: bool = True,
    show_extras: bool = False,
) -> Optional[str]:
    """
    Format a single log entry for display.

    Args:
        log_entry: Parsed JSON log entry
        show_colors: Whether to use ANSI colors for levels
        compact: Use compact format (like docker logs)
        show_extras: Show extra fields from log entry

    Returns:
        Formatted string or None if entry is invalid
    """
    try:
        level = log_entry.get("level", "INFO")
        message = log_entry.get("message", "")
        timestamp_str = log_entry.get("timestamp", "")
        module = log_entry.get("module", "unknown")
        line_num = log_entry.get("line", 0)

        # Parse timestamp
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                # Convert to local time and format compactly
                local_time = timestamp.astimezone()
                time_str = local_time.strftime("%m-%dT%H:%M:%S.%f")[:-3]
            except ValueError:
                time_str = timestamp_str[:23]
        else:
            time_str = "??-??T??:??:??.???"

        # Apply color based on level
        if show_colors:
            color = LEVEL_COLORS.get(level, "")
            reset = RESET_COLOR
        else:
            color = ""
            reset = ""

        # Build the formatted output
        if compact:
            # Format: 01-15T09:36:38.194 INFO     module                39 : message [extras]
            extras_str = ""
            if show_extras:
                # Collect extra fields that aren't standard
                standard_keys = {
                    "level",
                    "message",
                    "timestamp",
                    "module",
                    "line",
                    "logger",
                    "function",
                    "thread_name",
                    "human_time",
                    "_error_tracking_processed",
                    "_error_tracking_result",
                }
                extras = {k: v for k, v in log_entry.items() if k not in standard_keys}
                if extras:
                    extras_list = ", ".join(extras.keys())
                    extras_str = f" [{extras_list}]"

            formatted = (
                f"{time_str} {color}{level:<8}{reset} "
                f"{module:<25} {line_num:>4} : {message}{extras_str}"
            )
        else:
            # Verbose format
            logger = log_entry.get("logger", "")
            function = log_entry.get("function", "")
            formatted = (
                f"{time_str} | {color}{level:<8}{reset} | "
                f"{logger}:{module}.{function}:{line_num} | {message}"
            )

        return formatted

    except Exception as e:
        return f"[Error formatting log entry: {e}]"


def process_line(
    line: str,
    min_level: Optional[str] = None,
    grep_pattern: Optional[re.Pattern] = None,
    show_colors: bool = True,
    compact: bool = True,
    show_extras: bool = False,
) -> Optional[str]:
    """
    Process a single log line.

    Args:
        line: Raw line from log file
        min_level: Minimum log level to display
        grep_pattern: Regex pattern to filter messages
        show_colors: Whether to use colors
        compact: Use compact format
        show_extras: Show extra fields

    Returns:
        Formatted string or None if filtered out
    """
    line = line.strip()
    if not line:
        return None

    try:
        log_entry = json.loads(line)
    except json.JSONDecodeError:
        # Not valid JSON, print as-is (could be plain text logs)
        return line

    # Filter by level
    if min_level:
        entry_level = log_entry.get("level", "INFO")
        entry_priority = LEVEL_PRIORITY.get(entry_level, 20)
        min_priority = LEVEL_PRIORITY.get(min_level, 0)
        if entry_priority < min_priority:
            return None

    # Filter by grep pattern
    if grep_pattern:
        message = log_entry.get("message", "")
        # Strip ANSI codes for matching
        clean_message = strip_ansi_codes(message)
        if not grep_pattern.search(clean_message):
            return None

    return format_log_entry(
        log_entry,
        show_colors=show_colors,
        compact=compact,
        show_extras=show_extras,
    )


async def read_last_n_lines(file_path: str, n: int) -> List[str]:
    """
    Read the last N lines from a file efficiently.

    Args:
        file_path: Path to the file
        n: Number of lines to read

    Returns:
        List of lines (most recent last)
    """
    async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
        # Seek to end
        await f.seek(0, os.SEEK_END)
        file_size = await f.tell()

        if file_size == 0:
            return []

        # Read in chunks from the end
        block_size = 8192
        blocks: List[str] = []
        remaining = file_size

        while remaining > 0:
            read_size = min(block_size, remaining)
            remaining -= read_size
            await f.seek(remaining)
            block = await f.read(read_size)
            blocks.insert(0, block)

            # Check if we have enough lines
            content = "".join(blocks)
            if content.count("\n") >= n + 1:
                break

        content = "".join(blocks)
        lines = content.splitlines()

        # Return the last n lines
        return lines[-n:] if len(lines) > n else lines


async def tail_file(
    file_path: str,
    n_lines: int,
    min_level: Optional[str],
    grep_pattern: Optional[re.Pattern],
    show_colors: bool,
    compact: bool,
    show_extras: bool,
) -> int:
    """
    Print the last N lines of a file.

    Returns:
        The file position after reading
    """
    lines = await read_last_n_lines(file_path, n_lines)

    for line in lines:
        formatted = process_line(
            line,
            min_level=min_level,
            grep_pattern=grep_pattern,
            show_colors=show_colors,
            compact=compact,
            show_extras=show_extras,
        )
        if formatted:
            print(formatted)

    # Return current file size for follow mode
    async with aiofiles.open(file_path, "r") as f:
        await f.seek(0, os.SEEK_END)
        return await f.tell()


class LogFileHandler(FileSystemEventHandler):
    """Watch a log file for changes and print new lines."""

    def __init__(
        self,
        file_path: str,
        loop: asyncio.AbstractEventLoop,
        last_position: int,
        min_level: Optional[str],
        grep_pattern: Optional[re.Pattern],
        show_colors: bool,
        compact: bool,
        show_extras: bool,
    ):
        self.file_path = file_path
        self.loop = loop
        self.last_position = last_position
        self.min_level = min_level
        self.grep_pattern = grep_pattern
        self.show_colors = show_colors
        self.compact = compact
        self.show_extras = show_extras
        self._processing = False

    async def process_new_lines(self):
        """Read and print new lines since last position."""
        if self._processing:
            return
        self._processing = True

        try:
            async with aiofiles.open(self.file_path, "r", encoding="utf-8") as f:
                await f.seek(self.last_position)
                async for line in f:
                    formatted = process_line(
                        line,
                        min_level=self.min_level,
                        grep_pattern=self.grep_pattern,
                        show_colors=self.show_colors,
                        compact=self.compact,
                        show_extras=self.show_extras,
                    )
                    if formatted:
                        print(formatted, flush=True)
                self.last_position = await f.tell()
        finally:
            self._processing = False

    def on_modified(self, event):
        """Called when the watched file is modified."""
        if event.src_path == self.file_path:
            asyncio.run_coroutine_threadsafe(self.process_new_lines(), self.loop)


async def follow_file(
    file_path: str,
    start_position: int,
    min_level: Optional[str],
    grep_pattern: Optional[re.Pattern],
    show_colors: bool,
    compact: bool,
    show_extras: bool,
):
    """
    Follow a file for new content (like tail -f).
    """
    loop = asyncio.get_event_loop()
    handler = LogFileHandler(
        file_path=file_path,
        loop=loop,
        last_position=start_position,
        min_level=min_level,
        grep_pattern=grep_pattern,
        show_colors=show_colors,
        compact=compact,
        show_extras=show_extras,
    )

    observer = Observer()
    watch_dir = os.path.dirname(file_path) or "."
    observer.schedule(handler, watch_dir, recursive=False)
    observer.start()

    print(f"--- Following {file_path} (Ctrl+C to stop) ---", file=sys.stderr)

    try:
        while True:
            await asyncio.sleep(0.5)
            # Also check periodically in case watchdog misses events
            await handler.process_new_lines()
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()


def get_jsonl_files(path: str) -> List[str]:
    """
    Get JSONL files from a path.

    If path is a file, return it.
    If path is a directory, return all .jsonl files sorted by modification time.
    """
    path_obj = Path(path)

    if path_obj.is_file():
        return [str(path_obj)]

    if path_obj.is_dir():
        # Find all .jsonl files
        files = list(path_obj.glob("*.jsonl")) + list(path_obj.glob("*.jsonl.*"))
        # Sort by modification time (oldest first)
        files.sort(key=lambda f: f.stat().st_mtime)
        return [str(f) for f in files]

    return []


@app.command()
def main(
    log_path: Annotated[
        str,
        typer.Argument(
            help="Path to JSONL log file or directory containing log files"
        ),
    ],
    follow: Annotated[
        bool,
        typer.Option(
            "-f",
            "--follow",
            help="Follow the log file for new entries (like tail -f)",
        ),
    ] = False,
    tail: Annotated[
        int,
        typer.Option(
            "-n",
            "--tail",
            help="Number of lines to show from the end",
        ),
    ] = 50,
    level: Annotated[
        Optional[LogLevel],
        typer.Option(
            "-l",
            "--level",
            help="Minimum log level to display",
            case_sensitive=False,
        ),
    ] = None,
    grep: Annotated[
        Optional[str],
        typer.Option(
            "-g",
            "--grep",
            help="Filter messages by regex pattern",
        ),
    ] = None,
    no_color: Annotated[
        bool,
        typer.Option(
            "--no-color",
            help="Disable colored output",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option(
            "-v",
            "--verbose",
            help="Use verbose output format",
        ),
    ] = False,
    extras: Annotated[
        bool,
        typer.Option(
            "-e",
            "--extras",
            help="Show extra fields in log entries",
        ),
    ] = False,
):
    """
    Tail and follow JSONL log files with pretty formatting.

    Reads structured JSONL log files and displays them in a format similar
    to docker logs output, with proper emoji handling and optional filtering.

    Examples:
        # Show last 50 lines and follow
        python tail_jsonl_logs.py logs/lnd_monitor.jsonl -f

        # Show last 100 lines with WARNING or higher
        python tail_jsonl_logs.py logs/hive_monitor.jsonl -n 100 -l WARNING

        # Filter by pattern and follow
        python tail_jsonl_logs.py logs/api.jsonl -f -g "error|failed"
    """
    # Validate path
    if not os.path.exists(log_path):
        typer.echo(f"Error: Path not found: {log_path}", err=True)
        raise typer.Exit(code=1)

    # Get log files
    log_files = get_jsonl_files(log_path)
    if not log_files:
        typer.echo(f"Error: No JSONL files found at: {log_path}", err=True)
        raise typer.Exit(code=1)

    # Compile grep pattern if provided
    grep_pattern = None
    if grep:
        try:
            grep_pattern = re.compile(grep, re.IGNORECASE)
        except re.error as e:
            typer.echo(f"Error: Invalid regex pattern: {e}", err=True)
            raise typer.Exit(code=1)

    # Get min level
    min_level = level.value if level else None

    # Display settings
    show_colors = not no_color
    compact = not verbose

    async def run():
        # If multiple files, show all of them
        if len(log_files) > 1 and not follow:
            for log_file in log_files[:-1]:
                typer.echo(f"--- {log_file} ---", err=True)
                await tail_file(
                    log_file,
                    n_lines=tail,
                    min_level=min_level,
                    grep_pattern=grep_pattern,
                    show_colors=show_colors,
                    compact=compact,
                    show_extras=extras,
                )

        # Always process the last (most recent) file
        target_file = log_files[-1]
        if len(log_files) > 1:
            typer.echo(f"--- {target_file} ---", err=True)

        last_pos = await tail_file(
            target_file,
            n_lines=tail,
            min_level=min_level,
            grep_pattern=grep_pattern,
            show_colors=show_colors,
            compact=compact,
            show_extras=extras,
        )

        # Follow mode
        if follow:
            await follow_file(
                target_file,
                start_position=last_pos,
                min_level=min_level,
                grep_pattern=grep_pattern,
                show_colors=show_colors,
                compact=compact,
                show_extras=extras,
            )

    asyncio.run(run())


if __name__ == "__main__":
    app()
