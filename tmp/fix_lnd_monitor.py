"""Fix the misplaced logger.info block in lnd_monitor_v2.py"""

import sys

filepath = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/src/lnd_monitor_v2.py"

with open(filepath, "r") as f:
    lines = f.readlines()

# Find the misplaced logger.info block (between try/except)
# It starts with '            logger.info(\n' containing "LND gRPC client started"
# and is currently between the try:.../wait_for close paren and except asyncio.TimeoutError

# Find the logger.info line with "LND gRPC client started" after line 1300
logger_start = None
for i in range(1300, len(lines)):
    if (
        "logger.info(" in lines[i]
        and i + 1 < len(lines)
        and "LND gRPC client started" in lines[i + 1]
    ):
        logger_start = i
        break

if logger_start is None:
    print("ERROR: Could not find the logger.info block with 'LND gRPC client started'")
    sys.exit(1)

print(f"Found logger.info block starting at line {logger_start + 1}")

# The block is 5 lines: logger.info(, f-string1, f-string2, extra=, )
logger_block = lines[logger_start : logger_start + 5]
print("Block content:")
for line in logger_block:
    print(f"  {repr(line)}")

# Find startup_complete_event.set() after line 1300
startup_line = None
for i in range(1300, len(lines)):
    if "startup_complete_event.set()" in lines[i]:
        startup_line = i
        break

if startup_line is None:
    print("ERROR: Could not find startup_complete_event.set()")
    sys.exit(1)

print(f"Found startup_complete_event.set() at line {startup_line + 1}")

# Verify the logger block is AFTER startup_complete_event (it's misplaced)
if logger_start < startup_line:
    print("Logger block is already before startup_complete_event - nothing to fix?")
    sys.exit(0)

# Remove the misplaced block
del lines[logger_start : logger_start + 5]
print(f"Removed logger block from lines {logger_start + 1}-{logger_start + 5}")

# Now insert it before startup_complete_event.set()
# startup_line index hasn't changed since we removed lines AFTER it
for i, line in enumerate(logger_block):
    lines.insert(startup_line + i, line)
print(f"Inserted logger block before line {startup_line + 1}")

# Write back
with open(filepath, "w") as f:
    f.writelines(lines)

print("\nDone! Verifying result:")
with open(filepath, "r") as f:
    lines = f.readlines()

# Show the region around the fix
for i in range(startup_line - 2, startup_line + 20):
    print(f"{i + 1}: {lines[i].rstrip()}")
