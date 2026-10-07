#!/usr/bin/env python3
"""Count dead-root activation skips in the last 24 h (smarty-dev#6062). Read-only.

Usage: dead_root_skips_24h.py [MESH_ROOT_OR_JSONL ...]

Each argument is a Fabric mesh root (its metrics/dead-root-skips.jsonl is read) or a JSONL
file. Without arguments: $PI_FABRIC_MESH_ROOT (os.pathsep-separated roots) if set, else
~/.pi/fabric/mesh and ./.pi/fabric/mesh. Missing files count as zero; malformed lines are
ignored. The last line printed is the number.
"""
import json
import os
import sys
import time
from datetime import datetime

WINDOW_S = 24 * 60 * 60
NAME = os.path.join("metrics", "dead-root-skips.jsonl")


def sources(argv):
    roots = argv or [p for p in os.environ.get("PI_FABRIC_MESH_ROOT", "").split(os.pathsep) if p] or [
        os.path.expanduser("~/.pi/fabric/mesh"),
        os.path.join(os.getcwd(), ".pi", "fabric", "mesh"),
    ]
    seen = []
    for root in roots:
        path = root if root.endswith(".jsonl") else os.path.join(root, NAME)
        path = os.path.realpath(os.path.expanduser(path))
        if path not in seen:
            seen.append(path)
    return seen


def stamp(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def count(path, since, until):
    total = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    at = stamp(json.loads(line).get("at"))
                except (ValueError, AttributeError):
                    continue
                if at is not None and since <= at <= until:
                    total += 1
    except OSError:
        return 0
    return total


def main(argv):
    now = time.time()
    paths = sources(argv)
    total = 0
    for path in paths:
        n = count(path, now - WINDOW_S, now + 60)
        total += n
        print(f"{path}: {n}", file=sys.stderr)
    print(total)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
