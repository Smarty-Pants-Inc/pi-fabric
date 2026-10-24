#!/usr/bin/env python3
"""Derive NATS HTTP monitoring endpoints from a run's TLS server list."""
from __future__ import annotations

import ipaddress
import re
import sys
from urllib.parse import urlsplit


def _valid_dns_name(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return False
    except ValueError:
        pass
    normalized = host[:-1] if host.endswith(".") else host
    labels = normalized.split(".")
    return bool(normalized) and all(
        0 < len(label) <= 63 and re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", label)
        for label in labels
    )


def derive_monitor_urls(server_list: str) -> tuple[str, str, str]:
    servers = [item.strip() for item in server_list.split(",")]
    if not servers or any(not item for item in servers):
        raise ValueError("NATS server list contains an empty URL")
    first_host: str | None = None
    for value in servers:
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError as error:
            raise ValueError("NATS server list contains an invalid URL") from error
        host = parsed.hostname
        if (parsed.scheme != "tls" or not host or port is None or not 1 <= port <= 65535
                or parsed.username is not None or parsed.password is not None
                or parsed.path or parsed.query or parsed.fragment or not _valid_dns_name(host)):
            raise ValueError("NATS servers must be TLS URLs with DNS names and no path or credentials")
        if first_host is None:
            first_host = host.rstrip(".").lower()
    assert first_host is not None
    origin = f"http://{first_host}:8222"
    return f"{origin}/connz", f"{origin}/varz", f"{origin}/jsz?streams=1"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: monitor-urls.py TLS_SERVER[,TLS_SERVER...]", file=sys.stderr)
        return 2
    try:
        urls = derive_monitor_urls(argv[1])
    except ValueError:
        print("cannot derive NATS monitor endpoints from the server list", file=sys.stderr)
        return 2
    print("\t".join(urls))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
