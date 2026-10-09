#!/usr/bin/env bash
# kill -9 tests: 3 rounds each, 8 writers, unique keys, verify every acked write after restart.
set -euo pipefail
echo '# nats sync_interval=always'
./natsbench -mode kill -server ./nats-server -conf killtest.conf -url nats://127.0.0.1:14303 -rounds 3 -c 8
echo '# sqlite FULL (writer process killed)'
node bench/sqlitebench.mjs kill sq3/kill-full.db 3 8
echo '# control: nats default sync (process kill only, page cache survives)'
./natsbench -mode kill -server ./nats-server -conf killtest-default.conf -url nats://127.0.0.1:14304 -rounds 3 -c 8
