#!/usr/bin/env bash
# Resume of run2.sh after the open() race fix (busy_timeout before journal_mode).
set -euo pipefail
W=${WARM:-5}; D=${DUR:-30}
run() { node bench/sqlitebench.mjs load sq3/m-$1-$2-$3.db $1 $2 $3 $W $D; node bench/sqlitebench.mjs load1c sq3/q-$1-$2-$3.db $1 $2 $3 $W $D; }
run NORMAL tx3 8
for k in row tx3; do for s in FULL NORMAL; do run $s $k 32; done; done
