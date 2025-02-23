import asyncio
import json
import os
import re
from datetime import datetime, timezone
from typing import List, Optional

import aiofiles
import typer
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

app = typer.Typer()
REGEX_FILTER = re.compile(
    r"(\U0001F4B0.*Forwarded.*✅|\U0001F4B5.*Received|⚡️.*Sent.*✅)"
)


async def process_log_lines(line: str, regex_filter: re.Pattern):
    global REGEX_FILTER
    try:
        log_entry = json.loads(line)
        message = log_entry.get("message", "N/A")
        if re.search(
            REGEX_FILTER,
            message,
        ) or (log_entry.get("payment") and log_entry.get("payment").get("route_str")):
            timestamp = datetime.fromisoformat(
                log_entry.get("timestamp", "N/A")
            ).replace(tzinfo=timezone.utc)
            module = log_entry.get("module", "N/A")
            line_number = log_entry.get("line", "N/A")
            local_timestamp = timestamp.astimezone()
            print(f"{local_timestamp} {module:>16} {line_number:>5} - {message}")
    except json.JSONDecodeError:
        print(f"Failed to decode JSON: {line}")


class LogFileHandler(FileSystemEventHandler):
    def __init__(self, log_file_path, loop):
        self.log_file_path = log_file_path
        self.loop = loop
        self.last_position = 0

    async def process_log_file(self):
        async with aiofiles.open(self.log_file_path, "r") as log_file:
            await log_file.seek(self.last_position)
            async for line in log_file:
                await process_log_lines(line, REGEX_FILTER)

            self.last_position = await log_file.tell()

    def on_modified(self, event):
        if event.src_path == self.log_file_path:
            asyncio.run_coroutine_threadsafe(self.process_log_file(), self.loop)


async def tail_log_file(log_file_path: str, lines: int):
    async with aiofiles.open(log_file_path, "r") as log_file:
        await log_file.seek(0, os.SEEK_END)
        file_size = await log_file.tell()
        block_size = 1024
        blocks: List[str] = []
        while file_size > 0 and len(blocks) < lines:
            file_size -= block_size
            if file_size < 0:
                file_size = 0
            await log_file.seek(file_size)
            blocks.append(await log_file.read(block_size))
        content = "".join(reversed(blocks)).splitlines()[-lines:]

        global REGEX_FILTER
        for line in content:
            await process_log_lines(line, REGEX_FILTER)


async def watch_log_file(log_file_path: str):
    loop = asyncio.get_event_loop()
    event_handler = LogFileHandler(log_file_path, loop)
    observer = Observer()
    observer.schedule(event_handler, os.path.dirname(log_file_path), recursive=False)
    observer.start()

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def get_log_files_in_order(log_dir: str) -> List[str]:
    log_files = sorted(
        [
            f
            for f in os.listdir(log_dir)
            if f.endswith(".jsonl") or re.match(r".*\.jsonl\.\d+$", f)
        ],
        key=lambda x: (len(x.split(".")), x),
        reverse=True,
    )
    return [os.path.join(log_dir, f) for f in log_files]


@app.command()
def main(
    log_file_path: str,
    follow: bool = typer.Option(
        False, "-f", help="Follow the log file for new entries"
    ),
    tail: Optional[int] = typer.Option(
        None, "--tail", help="Show the most recent N lines"
    ),
):
    if not os.path.exists(log_file_path):
        print(f"Log file not found: {log_file_path}")
        raise typer.Exit(code=1)

    if os.path.isdir(log_file_path):
        log_files = get_log_files_in_order(log_file_path)
        if not log_files:
            print(f"No log files found in directory: {log_file_path}")
            raise typer.Exit(code=1)
    else:
        log_files = [log_file_path]

    print(f"Log files: {log_files}")

    if tail:
        for log_file in log_files:
            print(f"Tail of {log_file} {"-"*80}")
            asyncio.run(tail_log_file(log_file, tail))

    if follow:
        asyncio.run(watch_log_file(log_files[-1]))

    if not follow and not tail:
        for log_file in log_files:
            print(f"Dump of {log_file} {"-"*80}")
            asyncio.run(tail_log_file(log_file, 10000))
        print(f"Tail of {log_files[-1]} {"-"*80}")
        asyncio.run(watch_log_file(log_files[-1]))


if __name__ == "__main__":
    app()
