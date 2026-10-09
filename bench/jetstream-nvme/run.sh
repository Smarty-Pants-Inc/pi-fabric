#!/usr/bin/env bash
# Full matrix, sequential (one workload on the disk at a time). Run from the temp dir.
# Each SQLite run gets a fresh db file (sq2/<mode>-<sync>-<case>-<c>.db).
set -euo pipefail
W=${WARM:-5}; D=${DUR:-30}
mkdir -p sq2
for c in 1 8 32; do for cs in kv pub batch; do
  ./natsbench -url nats://127.0.0.1:14301 -label nats-always -case $cs -c $c -warm ${W}s -dur ${D}s
  ./natsbench -url nats://127.0.0.1:14302 -label nats-default -case $cs -c $c -warm ${W}s -dur ${D}s
done; done
for c in 1 8 32; do for k in row tx3; do for s in FULL NORMAL; do
  node bench/sqlitebench.mjs load sq2/m-$s-$k-$c.db $s $k $c $W $D
  node bench/sqlitebench.mjs load1c sq2/q-$s-$k-$c.db $s $k $c $W $D
done; done; done
