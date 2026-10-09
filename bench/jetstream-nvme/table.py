#!/usr/bin/env python3
"""results.jsonl -> markdown table (case x backend x concurrency)."""
import json, sys

rows = [json.loads(l) for f in sys.argv[1:] for l in open(f) if l.startswith("{")]
case_name = {"kv": "single put", "row": "single put", "pub": "stream publish 1 KiB",
             "batch": "3-key batch", "tx3": "3-key batch",
             "abatch": "3-key batch, NATS atomic"}
order = {"single put": 0, "3-key batch": 1, "3-key batch, NATS atomic": 2, "stream publish 1 KiB": 3}
border = ["nats-always", "sqlite1c-FULL", "sqlite-FULL", "nats-default", "sqlite1c-NORMAL", "sqlite-NORMAL"]
rows.sort(key=lambda r: (order[case_name[r["case"]]], r["c"], border.index(r["backend"])))
print("| case | backend | c | ops/s | p50 ms | p99 ms | p99.9 ms | max ms | writer ops min/max |")
print("|---|---|--:|--:|--:|--:|--:|--:|--:|")
for r in rows:
    mx = f'{r["max_ms"]:.1f}' if "max_ms" in r else ""
    pw = f'{r["per_writer_min"]}/{r["per_writer_max"]}' if "per_writer_min" in r else ""
    print(f'| {case_name[r["case"]]} | {r["backend"]} | {r["c"]} | {r["ops_s"]:,.0f} | '
          f'{r["p50_ms"]:.3f} | {r["p99_ms"]:.3f} | {r["p999_ms"]:.3f} | {mx} | {pw} |')
