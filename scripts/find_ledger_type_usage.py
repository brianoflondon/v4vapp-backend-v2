#!/usr/bin/env python3
"""
Scan the repository for usages of members of LedgerType (both as attribute references
like `LedgerType.FOO` and by value strings like `'foo_value'`).

Usage:
    python scripts/find_ledger_type_usage.py [--json OUT]

Outputs a human-readable report to stdout and optionally JSON to a file.
"""
from __future__ import annotations

import ast
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

# Ensure the repository root is on sys.path so `src` can be imported
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import LedgerType from the package (now that src/ is on the path)
try:
    from src.v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
except Exception as e:  # pragma: no cover - helps running from different CWD
    print("Error importing LedgerType even after adjusting sys.path; run this script from the repository root.")
    raise

ROOT = PROJECT_ROOT
# Default excluded directories. Tests are excluded by default; pass --include-tests to include them.
EXCLUDE_DIRS = {".venv", "d_vol", "htmlcov", "docs", "tmp", "node_modules", "tests"}  # 'tests' excluded unless requested

@dataclass
class Occurrence:
    path: str
    line_no: int
    line: str


def walk_py_files(root: str, include_tests: bool = False):
    """Yield Python file paths under `root`.

    By default, the `tests` directory is excluded. Set `include_tests=True` to include it.
    """
    for dirpath, dirnames, filenames in os.walk(root):
        # determine per-walk excluded dirs so we can honor --include-tests without mutating the global
        excluded = set(EXCLUDE_DIRS)
        if include_tests and "tests" in excluded:
            excluded.remove("tests")
        dirnames[:] = [d for d in dirnames if d not in excluded and not d.startswith(".")]
        for fn in filenames:
            if fn.endswith(".py"):
                yield os.path.join(dirpath, fn)


def find_usages(include_tests: bool = False, match_values: bool = False) -> Dict[str, List[Occurrence]]:
    """Find usages of LedgerType members.

    By default this is strict: only attribute-based references are counted (e.g.,
    `LedgerType.CONV_HIVE_TO_KEEPSATS`). If `match_values=True`, literal string
    occurrences of the enum values (e.g., `'limit_order_cancelled'`) are also
    matched via AST Constant nodes (less strict and prone to false positives).
    """
    members = list(LedgerType)
    member_names = {m.name: m for m in members}
    member_values = {m.value: m for m in members}

    usages: Dict[str, List[Occurrence]] = defaultdict(list)

    def get_attr_chain(node: ast.AST) -> List[str]:
        """Return a list of names for an attribute chain, e.g.
        for `a.b.c` return ['a','b','c'].
        """
        names: List[str] = []
        cur = node
        while isinstance(cur, ast.Attribute):
            names.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            names.append(cur.id)
        # names were collected from the rightmost attribute inward; reverse to make left-to-right
        return list(reversed(names))

    ledger_file = os.path.abspath(os.path.join(ROOT, "src", "v4vapp_backend_v2", "accounting", "ledger_type_class.py"))

    for path in walk_py_files(ROOT, include_tests=include_tests):
        # skip the file that defines LedgerType itself
        if os.path.abspath(path) == ledger_file:
            continue
        try:
            with open(path, "r", encoding="utf-8") as fh:
                text = fh.read()
                # keep lines for quick snippet extraction
                lines = text.splitlines()
                try:
                    tree = ast.parse(text, filename=path)
                except SyntaxError:
                    # skip files that can't be parsed (e.g., generated code)
                    continue
                for node in ast.walk(tree):
                    # strict: look for Attribute chains that include LedgerType followed by a member name
                    if isinstance(node, ast.Attribute):
                        chain = get_attr_chain(node)
                        # find 'LedgerType' in chain and ensure next item is a known member
                        for idx, name in enumerate(chain[:-1]):
                            if name == "LedgerType":
                                candidate = chain[idx + 1]
                                if candidate in member_names:
                                    lineno = getattr(node, "lineno", 0)
                                    snippet = lines[lineno - 1].rstrip() if lineno and lineno <= len(lines) else ""
                                    usages[candidate].append(Occurrence(path, lineno, snippet))
                    # optional: literal string matches via AST Constant nodes (safer than regex over raw lines)
                    if match_values:
                        if isinstance(node, ast.Constant) and isinstance(node.value, str):
                            val = node.value
                            if val in member_values:
                                member_name = member_values[val].name
                                lineno = getattr(node, "lineno", 0)
                                snippet = lines[lineno - 1].rstrip() if lineno and lineno <= len(lines) else ""
                                usages[member_name].append(Occurrence(path, lineno, snippet))
        except Exception as e:
            print(f"Could not read {path}: {e}", file=sys.stderr)
    return usages


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Find usages of LedgerType members across the codebase")
    parser.add_argument("--json", nargs="?", const="ledger_type_usages.json", help="Write JSON report to FILE (optional)")
    parser.add_argument("--include-tests", action="store_true", help="Include the 'tests' directory in the scan (default: excluded)")
    parser.add_argument("--match-values", action="store_true", help="Also match literal enum value strings (less strict) via AST string nodes")
    args = parser.parse_args()

    usages = find_usages(include_tests=args.include_tests, match_values=args.match_values)
    all_names = [m.name for m in LedgerType]

    unused = [n for n in all_names if n not in usages or len(usages[n]) == 0]

    tests_str = "including tests" if args.include_tests else "excluding tests"
    match_str = "including literal value matches" if args.match_values else "strict (LedgerType attributes only)"
    header_suffix = f" ({tests_str}; {match_str})"
    print(f"LedgerType usage report{header_suffix}")
    print("=======================\n")
    print(f"Total LedgerType members: {len(all_names)}")
    print(f"Used members: {len(all_names) - len(unused)}")
    print(f"Unused members: {len(unused)}\n")

    if unused:
        print("Members with NO observed usages:")
        for n in unused:
            print(f"  - {n}")
        print()

    print("Usages (sample, truncated to first 20 occurrences per member):\n")
    for name in all_names:
        occs = usages.get(name, [])
        print(f"{name} ({len(occs)} occurrences):")
        for o in occs[:20]:
            print(f"  {o.path}:{o.line_no}: {o.line}")
        if len(occs) > 20:
            print(f"  ... and {len(occs) - 20} more occurrences")
        print()

    # optional JSON output
    if args.json is not None:
        out_path = args.json or "ledger_type_usages.json"
        payload = {n: [{"path": o.path, "line": o.line_no, "snippet": o.line} for o in usages.get(n, [])] for n in all_names}
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print(f"Wrote JSON report to {out_path}")


if __name__ == "__main__":
    main()
