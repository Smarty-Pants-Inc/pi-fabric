#!/usr/bin/env bash
# Second pass: NATS 2.12 atomic batch (abatch) and the corrected SQLite matrix
# (fresh value bytes per put). Sequential; run from the temp dir.
set -euo pipefail
W=${WARM:-5}; D=${DUR:-30}
mkdir -p sq3
for c in 1 8 32; do
  ./natsbench -url nats://127.0.0.1:14301 -label nats-always -case abatch -c $c -warm ${W}s -dur ${D}s
  ./natsbench -url nats://127.0.0.1:14302 -label nats-default -case abatch -c $c -warm ${W}s -dur ${D}s
done
for c in 1 8 32; do for k in row tx3; do for s in FULL NORMAL; do
  node bench/sqlitebench.mjs load sq3/m-$s-$k-$c.db $s $k $c $W $D
  node bench/sqlitebench.mjs load1c sq3/q-$s-$k-$c.db $s $k $c $W $D
done; done; done
