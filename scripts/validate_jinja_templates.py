#!/usr/bin/env python3
"""Validate Jinja templates under admin/templates by compiling them.
Exit code 1 on first failure.
"""

import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError

TEMPLATES_DIR = Path("src/v4vapp_backend_v2/admin/templates")

if not TEMPLATES_DIR.exists():
    print("Templates directory not found, skipping")
    sys.exit(0)

env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))

failed = False
for p in TEMPLATES_DIR.rglob("*.html"):
    name = str(p.relative_to(TEMPLATES_DIR))
    try:
        src = p.read_text(encoding="utf-8")
        # parse to catch syntax errors
        env.parse(src)
    except TemplateSyntaxError as e:
        failed = True
        print(f"TemplateSyntaxError in {name}: {e}")
    except Exception as e:
        failed = True
        print(f"Error compiling {name}: {e}")

if failed:
    sys.exit(1)

print("All Jinja templates compiled successfully.")
