import asyncio
import os
import re
import select
import subprocess
import threading
import time  # Optional, for adding a startup delay if needed
from datetime import datetime, timedelta
from queue import Queue

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig

"""
    1. Add a Session-Scoped Event Loop
Create a conftest.py in your tests directory to override the
fixture with session scope:
"""


@pytest.fixture(scope="session")
def event_loop():
    """Create single event loop for entire session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def full_stack_setup():
    print("Current working directory:", os.getcwd())

    # Queues to store output from processes
    output_queues = {"hive": Queue(), "db": Queue(), "lnd": Queue()}

    # Flag to indicate when to stop reading
    stop_reading = threading.Event()

    # Function to continuously read from a process output to avoid buffer filling up
    def read_output(proc, name, queue):
        while not stop_reading.is_set():
            # Read from stdout
            rlist, _, _ = select.select([proc.stdout, proc.stderr], [], [], 0.1)
            for stream in rlist:
                line = stream.readline()
                if line:
                    queue.put(line)
                    print(f"{name.upper()}: {line.strip()}")

    ic = InternalConfig(config_filename="config/devhive.config.yaml")
    # Start processes
    processes = {
        "hive": subprocess.Popen(
            [
                "python",
                "src/hive_monitor_v2.py",
                "--config-filename",
                "config/devhive.config.yaml",
                "--start-block",
                "-1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        ),
        "db": subprocess.Popen(
            ["python", "src/db_monitor.py", "--config", "config/devhive.config.yaml", "--resume"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        ),
        "lnd": subprocess.Popen(
            ["python", "src/lnd_monitor_v2.py", "--config", "config/devhive.config.yaml"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        ),
    }

    # Start threads to read process output
    reader_threads = {}
    for name, proc in processes.items():
        thread = threading.Thread(
            target=read_output, args=(proc, name, output_queues[name]), daemon=True
        )
        thread.start()
        reader_threads[name] = thread

    # Patterns to detect when each service is ready
    ready_patterns = {
        "hive": re.compile(r"🐝 ✅ Hive Monitor v2: 🐝\. Version:"),
        "db": re.compile(r"🏆 ✅ Database Monitor App\. Started\. Version:"),
        "lnd": re.compile(r"⛱️ ✅ LND gRPC client started\. Monitoring node:"),
    }

    ready_services = set()
    start_time = datetime.now()
    timeout = timedelta(seconds=60)

    print("Waiting for services to become ready...")

    # Monitor the output queues for ready signals
    while len(ready_services) < 3 and datetime.now() - start_time < timeout:
        for name, queue in output_queues.items():
            if name not in ready_services:
                # Process all queued output
                while not queue.empty():
                    line = queue.get()
                    if ready_patterns[name].search(line):
                        ready_services.add(name)
                        print(f"✅ {name.capitalize()} monitor is ready")

        # Check if any process exited prematurely
        for name, proc in processes.items():
            if proc.poll() is not None:
                print(f"WARNING: {name} process exited with code {proc.poll()}")

        # Small sleep to avoid tight loop
        if len(ready_services) < 3:
            time.sleep(0.5)

    if len(ready_services) < 3:
        missing = set(["hive", "db", "lnd"]) - ready_services
        print(f"Warning: Timed out waiting for services: {missing}")
    else:
        print("All services are ready! ✅")

    # Additional delay for final initialization
    time.sleep(2)

    # Yield control to test functions
    yield

    # Stop reading output
    stop_reading.set()

    # Terminate processes
    for name, proc in processes.items():
        print(f"Terminating {name} monitor...")
        try:
            proc.terminate()
            try:
                proc.wait(timeout=10)  # Shorter initial timeout
            except subprocess.TimeoutExpired:
                print(f"Process {name} did not terminate with SIGTERM, sending SIGKILL...")
                proc.kill()
                proc.wait(timeout=5)
        except Exception as e:
            print(f"Error cleaning up {name} process: {e}")

    print("All processes terminated.")
