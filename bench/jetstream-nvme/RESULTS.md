# JetStream vs SQLite WAL durability bench on local NVMe (smarty-dev#7504)

Benchmark only. Run on Intel 1 on 2026-10-09, about 10:30–11:35Z. Lead: fabric-store.

## Answer

- **NATS can keep "fsync before ack"** (`sync_interval: always`), and kill -9 lost **0** acked writes
  in 3 of 3 rounds. SQLite `synchronous=FULL` also lost 0 in 3 of 3.
- **Single puts and stream publishes cost the same as SQLite FULL.** Both are bound by one device
  flush per acked write: about 800 ops/s on this disk, whatever the concurrency. The raw floor
  (append 200 B + `fdatasync`) is 836/s at p50 1.19 ms. Latency then grows linearly with the
  writer count: p50 is about 1.2 / 9.5 / 38 ms at 1 / 8 / 32 writers. Neither backend does group
  commit, so more writers only add queueing.
- **3-key batches cost NATS about 3x.** Each JetStream message gets its own fsync, so a batch of 3
  puts costs 3 flushes: about 270 batches/s against about 800/s for one SQLite 3-row transaction.
  The p50 at 32 writers is 113 ms (NATS) against 41 ms (SQLite). The NATS 2.12 atomic batch publish
  (ADR-50, `allow_atomic`) does not help. It gives about 240 batches/s with `always`, and with the
  default sync it is about 10x slower than plain publishes.
- **Non-durable comparison:** with the default `sync_interval` (2 min), NATS gives 24k–210k ops/s
  at sub-ms p50. That is 30–260x faster than `always`, but an acked write survives only a process
  crash, not a power loss or kernel crash.
- **SQLite with N connections starves writers.** With `busy_timeout`, writers that lose the lock
  sleep in the busy handler. At 8 and 32 writers, some writers completed 0 ops in 30 s and the
  maximum wait was 27–30 s. Total throughput stays the same. The fair design is one connection with
  an in-process queue (`sqlite1c` rows below): same throughput, a bounded tail.

## Table

ops/s counts acked operations completed inside the 30 s window, after a 5 s warm-up. For a batch,
ops/s is batches/s (multiply by 3 for puts/s). The latency is ack latency from issue to the last ack
(or COMMIT).

Backends:
- `nats-always`: JetStream file store, `sync_interval: always`.
- `nats-default`: the default sync (2 min).
- `sqlite-*`: N worker threads, one connection each, `busy_timeout=30000`. "writer ops min/max"
  shows starvation. The latency samples include every op that overlaps the window, so a starved
  writer's whole wait counts.
- `sqlite1c-*`: one connection in one process, with N async callers queued on it.

Cases:
- single put: NATS KV `Put` (200 B, random key of 10k), or a SQLite `INSERT … ON CONFLICT DO
  UPDATE` autocommit.
- 3-key batch: 3 async KV publishes acked together, or one `BEGIN IMMEDIATE` + 3 upserts + `COMMIT`.
- NATS atomic: the same 3 puts as one ADR-50 atomic batch. The commit ack is checked for
  `count == 3`.
- stream publish: `js.Publish` of 1 KiB with an ack. This case has no SQLite equivalent.

| case | backend | c | ops/s | p50 ms | p99 ms | p99.9 ms | max ms | writer ops min/max |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| single put | nats-always | 1 | 759 | 1.248 | 1.923 | 4.418 |  |  |
| single put | sqlite1c-FULL | 1 | 853 | 1.108 | 2.114 | 4.612 | 12.7 |  |
| single put | sqlite-FULL | 1 | 856 | 1.105 | 2.294 | 4.313 | 32.0 | 25691/25691 |
| single put | nats-default | 1 | 24,057 | 0.042 | 0.103 | 0.238 |  |  |
| single put | sqlite1c-NORMAL | 1 | 68,724 | 0.006 | 0.018 | 4.077 | 63.4 |  |
| single put | sqlite-NORMAL | 1 | 88,987 | 0.004 | 0.012 | 3.909 | 214.3 | 2670645/2670645 |
| single put | nats-always | 8 | 829 | 9.489 | 17.346 | 20.921 |  |  |
| single put | sqlite1c-FULL | 8 | 809 | 9.607 | 14.534 | 21.608 | 47.5 |  |
| single put | sqlite-FULL | 8 | 854 | 1.112 | 1.915 | 8.992 | 27251.8 | 0/14824 |
| single put | nats-default | 8 | 91,887 | 0.083 | 0.180 | 1.159 |  |  |
| single put | sqlite1c-NORMAL | 8 | 95,197 | 0.036 | 0.159 | 5.658 | 51.1 |  |
| single put | sqlite-NORMAL | 8 | 199,818 | 0.004 | 0.011 | 2.420 | 11840.3 | 413492/1010870 |
| single put | nats-always | 32 | 791 | 38.223 | 90.279 | 109.516 |  |  |
| single put | sqlite1c-FULL | 32 | 809 | 38.702 | 50.410 | 94.046 | 95.0 |  |
| single put | sqlite-FULL | 32 | 832 | 1.183 | 3.446 | 13741.635 | 29758.0 | 0/4101 |
| single put | nats-default | 32 | 124,157 | 0.226 | 1.040 | 3.523 |  |  |
| single put | sqlite1c-NORMAL | 32 | 93,301 | 0.145 | 5.627 | 6.944 | 21.7 |  |
| single put | sqlite-NORMAL | 32 | 189,314 | 0.004 | 0.015 | 2.923 | 10437.5 | 73500/322224 |
| 3-key batch | nats-always | 1 | 271 | 3.522 | 6.233 | 14.003 |  |  |
| 3-key batch | sqlite1c-FULL | 1 | 801 | 1.198 | 2.322 | 6.396 | 24.4 |  |
| 3-key batch | sqlite-FULL | 1 | 846 | 1.124 | 2.217 | 4.741 | 19.5 | 25384/25384 |
| 3-key batch | nats-default | 1 | 16,296 | 0.060 | 0.195 | 0.473 |  |  |
| 3-key batch | sqlite1c-NORMAL | 1 | 35,916 | 0.010 | 0.028 | 4.400 | 34.4 |  |
| 3-key batch | sqlite-NORMAL | 1 | 37,186 | 0.009 | 0.025 | 4.370 | 31.4 | 1119256/1119256 |
| 3-key batch | nats-always | 8 | 257 | 28.459 | 82.671 | 103.054 |  |  |
| 3-key batch | sqlite1c-FULL | 8 | 820 | 9.403 | 15.604 | 21.535 | 41.5 |  |
| 3-key batch | sqlite-FULL | 8 | 786 | 1.200 | 3.487 | 2535.350 | 26769.3 | 874/7240 |
| 3-key batch | nats-default | 8 | 45,202 | 0.162 | 0.645 | 2.923 |  |  |
| 3-key batch | sqlite1c-NORMAL | 8 | 35,809 | 0.076 | 5.461 | 6.824 | 90.7 |  |
| 3-key batch | sqlite-NORMAL | 8 | 74,220 | 0.010 | 0.043 | 5.267 | 7134.7 | 191880/394329 |
| 3-key batch | nats-always | 32 | 280 | 113.321 | 166.649 | 223.049 |  |  |
| 3-key batch | sqlite1c-FULL | 32 | 766 | 40.929 | 53.609 | 57.890 | 58.6 |  |
| 3-key batch | sqlite-FULL | 32 | 808 | 1.195 | 4.783 | 12240.810 | 28178.6 | 7/3039 |
| 3-key batch | nats-default | 32 | 51,949 | 0.548 | 2.046 | 4.468 |  |  |
| 3-key batch | sqlite1c-NORMAL | 32 | 36,615 | 0.304 | 6.259 | 11.164 | 36.2 |  |
| 3-key batch | sqlite-NORMAL | 32 | 79,500 | 0.010 | 0.037 | 6.972 | 13840.1 | 26005/115573 |
| 3-key batch, NATS atomic | nats-always | 1 | 236 | 3.995 | 6.701 | 15.279 |  |  |
| 3-key batch, NATS atomic | nats-default | 1 | 2,218 | 0.396 | 1.699 | 2.379 |  |  |
| 3-key batch, NATS atomic | nats-always | 8 | 250 | 31.413 | 55.331 | 67.558 |  |  |
| 3-key batch, NATS atomic | nats-default | 8 | 3,515 | 2.149 | 8.873 | 13.209 |  |  |
| 3-key batch, NATS atomic | nats-always | 32 | 243 | 130.450 | 229.428 | 266.097 |  |  |
| 3-key batch, NATS atomic | nats-default | 32 | 4,632 | 6.181 | 17.312 | 26.179 |  |  |
| stream publish 1 KiB | nats-always | 1 | 792 | 1.204 | 2.224 | 5.078 |  |  |
| stream publish 1 KiB | nats-default | 1 | 22,851 | 0.039 | 0.163 | 1.159 |  |  |
| stream publish 1 KiB | nats-always | 8 | 815 | 9.658 | 17.824 | 23.664 |  |  |
| stream publish 1 KiB | nats-default | 8 | 119,215 | 0.063 | 0.153 | 0.290 |  |  |
| stream publish 1 KiB | nats-always | 32 | 813 | 39.060 | 68.221 | 88.146 |  |  |
| stream publish 1 KiB | nats-default | 32 | 210,317 | 0.141 | 0.418 | 1.269 |  |  |

Disk floor (`fsyncprobe.py`, append 200 B + `fdatasync`, n=5000): 836/s, p50 1.193 ms,
p99 1.592 ms, p99.9 3.611 ms.

## kill -9 (fsync on every write)

The test uses 8 writers and unique keys, each with a fresh 200-byte value. A key counts as acked
only after the client received the ack (NATS) or the autocommit returned (SQLite). After 3–7 s
the test sends SIGKILL, restarts the backend, reads every acked key of all rounds so far, and
compares the values. Raw output: `raw/kill.jsonl`.

| backend | round | acked (cumulative) | missing | wrong value | restart → ready |
|---|--:|--:|--:|--:|--:|
| nats `sync_interval: always` (nats-server SIGKILL) | 1 | 3,200 | **0** | 0 | 10 ms |
| | 2 | 6,628 | **0** | 0 | 27 ms |
| | 3 | 11,497 | **0** | 0 | 30 ms |
| sqlite WAL `synchronous=FULL` (writer process SIGKILL) | 1 | 2,534 | **0** | 0 | 0.9 ms reopen + first read, `integrity_check` ok |
| | 2 | 5,260 | **0** | 0 | 0.5 ms, ok |
| | 3 | 9,426 | **0** | 0 | 0.8 ms, ok |
| control: nats default sync | 1–3 | 375k / 757k / 1,161k | 0 | 0 | 259 / 486 / 744 ms |

"Ready" for NATS means the time from exec to the first successful `kv.Status` (the TCP listen
came 1 ms earlier). The store holds a few more values than acked writes (for example 11,504 against
11,497). Those are writes in flight at the kill whose ack never arrived. That is expected and not a
loss.

**Limit: kill -9 does not test fsync.** The page cache survives a process kill, so the
default-sync control also lost 0. Only a power cut or a kernel crash tells `always` from the
default. This bench does not test that. The durability claim for `always` rests on its per-write
fsync, which the 1-flush-per-write throughput above shows. Restart time with the default sync grows
with the unflushed store size (about 0.25 s per 400k messages here), because the server rebuilds
its indexes.

## Host, disk and versions

- Host: Intel 1 (`intel1`), Intel Core i9-14900K, `nproc` 32, 184 GiB RAM, kernel
  7.0.0-38-generic. The host was shared with other fleet work (load average about 5).
- Disk: the store and SQLite dbs are in `/home/paul/b7504-nvme.*` on `/dev/nvme0n1p2`, ext4
  `rw,relatime`. The default `mktemp -d` resolves to a tmpfs (`/run/user/1000/...`), so I used
  `mktemp -d -p $HOME` instead.
- `lsblk -d -o NAME,MODEL,ROTA`: `nvme0n1 Samsung SSD 990 EVO Plus 4TB 0` (also `nvme1n1`, same
  model, not used). Firmware 2B2QKXG7. `queue/write_cache` = `write back`, `fua` = 1. This is a
  consumer drive with no power-loss protection, so each fsync is a real flush of about 1.1 ms. On a
  drive with power-loss protection, both durable backends would be much faster, and by the same
  factor.
- nats-server **v2.12.15** linux-amd64, the latest v2.12 release (v2.14.8 and v2.15.1 also exist).
  The tar.gz sha256 is `58ab8131f819263897e2fc38e503e1b755b63bcafeecc7d12582487df48ca15e`, which
  matches the release `SHA256SUMS` (`sha256sum -c` OK). The binary was built with go1.25.12, git
  8460a42.
- Client: nats.go **v1.54.0**, Go 1.26.8.
- SQLite: `node:sqlite` in Node **v24.19.0**, SQLite **3.53.3** (`DEFAULT_WAL_SYNCHRONOUS=2`),
  WAL mode, `wal_autocheckpoint` 1000.
- The `/varz` output confirmed `sync_always: true` on the durable server.

## Method notes and pitfalls found

- The first SQLite pass was **invalid**. The bench wrote the same 200 bytes on every upsert. SQLite
  skips the page write, and so the WAL fsync, when an upsert stores identical bytes. Once the 10k
  keys filled up, "FULL" reached 400k ops/s. I fixed this with fresh bytes per put; the raw file
  is kept as `raw/run1-all-INVALID-sqlite-rows.jsonl`, and only its NATS rows are used. A store
  that rewrites unchanged values gets this "free" in production too, but that is not the case this
  bench measures.
- A run with `busy_timeout` set after `PRAGMA journal_mode=WAL` failed on open with
  SQLITE_BUSY_RECOVERY. Set `busy_timeout` first.
- Each case ran once for 30 s; there were no repeats. The fsync-bound numbers sit within ±10% of
  the disk floor. The NORMAL and default-sync numbers depend on the CPU and vary more.
- NATS writers use one connection each. The batch case uses `PublishAsync` ×3 and waits for all 3
  acks.

## Scripts (this directory)

- `natsbench/main.go`: the NATS load (`-case kv|pub|batch|abatch`) and the kill -9 harness
  (`-mode kill`).
- `sqlitebench.mjs`: the SQLite load (`load`, `load1c`) and the kill harness (`kill`).
- `run.sh`: pass 1 (the NATS rows). `run2.sh` and `run3.sh`: the atomic batch and the corrected
  SQLite rows. `killtests.sh`: the kill -9 tests. `table.py`: builds the table. `fsyncprobe.py`:
  the disk floor.
- `*.conf`: the server configs. `store_dir` points at the deleted temp dir; edit it before reuse.
- `raw/`: the JSONL output. Table inputs: `results-nats.jsonl`, `r2nats.jsonl`, `r2sq.jsonl` and
  `results3.jsonl`.

Reproduce from a work dir on NVMe:

1. Put `nats-server` and the `natsbench` binary in the dir (`cd natsbench && go build -o ../natsbench .`).
2. Copy this directory to `./bench`.
3. Start `nats-server -c always.conf` and `nats-server -c default.conf`.
4. Run `bash bench/run.sh`, then `bash bench/run2.sh`, then `bash bench/killtests.sh`.
