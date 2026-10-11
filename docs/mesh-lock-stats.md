# Mesh lock stats

`fabric-mesh-lock-stats` shows how busy a mesh root's lock (`<mesh>/.lock`) is, who holds it
and who waits for it, summed over every process on that root (smarty-dev#6477, lane L8).

## What is recorded

Every `MeshStore` acquisition of the mesh lock records, by caller class:

| Class | Store entry point |
|---|---|
| `publish` | `publish` |
| `put/delete` | `put`, `delete` (each optimistic retry is its own acquisition) |
| `writeBatch` | `writeBatch` (participant publication, control plane, reapers, lifecycle) |
| `heartbeat/confirm` | `confirmWritable` (the participant heartbeat's empty acquisition) |
| `custody` | `exclusive` (file custody, sweeps, resident persistence and retry waits) |
| `bridge` | `publishBatch` (bridge imports) and the bridge presence mirror's `writeBatch` (an explicit `lockClass: "bridge"` option, never the caller identity) |
| `other` | anything else |

For each class and wall-clock minute: acquisitions, wait time (request to custody) and hold time
(custody to release) as sums, maxima and histograms (bounds 1, 2, 5, 10, 20, 50, 100, 250, 500,
1000, 2500, 5000, 10000 ms), full-budget **timeouts** and failed bounded **tries** (registry-fenced
`withTryLock` scopes or an explicit zero-wait `exclusive`, which fail by design while the lock is
busy). An ordinary write whose remaining budget ran out before custody is a timeout, not a try.

Each process writes `<mesh>/lock-stats/<host>-<pid>.json` on the next acquisition after a minute
boundary, at exit, or on an explicit flush. A quiet process keeps its final samples in memory
until one of those events; recording never schedules an idle timer. The file holds at most the last 60 complete minutes (the longest
`--minutes` window) plus the current one and is replaced by an atomic
rename, without fsync and without any lock. The temporary is a unique name created exclusively and
never through a symlink (mode 0600). A `lock-stats` directory that is a symlink, not a directory,
owned by another user or group/other-writable disables recording for that root, with one line in
the profile's private `fabric-lock-stats.log` (`$PI_CODING_AGENT_DIR`, default `~/.pi/agent`).
Files untouched for 24 hours are pruned during a real flush, at most hourly; a quiet root is not
polled for pruning. Nothing runs before a process's first acquisition. Set `PI_FABRIC_LOCK_STATS=0` to turn recording off for a
process; test runs set it, and the lock-stats suites opt back in.

## Usage

```sh
fabric-mesh-lock-stats [--mesh DIR] [--minutes N] [--top K] [--json] [--max-busy PCT] [--max-timeouts N]
```

The mesh root resolves as for `fabric-participants`. The window is the last N complete minutes
(default 10, at most 60). Busy % is the summed hold time over the window's wall time. For a stage
gate such as `fabric-mesh-lock-stats --minutes 60 --max-busy 30 --max-timeouts 0` (busy < 30%, no
timeouts): `--max-busy PCT` exits 3 when busy % is at or above PCT, and `--max-timeouts N` exits 3
when there are more than N timeouts.

The reader takes regular files only (no symlinks), at most 1 MiB each and 4096 files, and uses a
file only when every counter is a non-negative integer, every duration finite and non-negative,
every histogram 14 buckets long and every class known. Other files are ignored with a warning on
stderr; with a gate flag they also exit 3, so a gate never passes on files it could not read.
Host and writer labels are printed with control characters escaped.

The file's optional `writer` object records the script basename (`argv1`),
`PI_FABRIC_AGENT_NAME` (`agentName`), `PI_FABRIC_ACTOR_ID` (`actorId`),
`PI_FABRIC_ACTOR_NAME` (`actorName`), `PI_FABRIC_ROLE` (`role`), `SMARTY_ROLE`
(`smartyRole`), and the parent PID (`ppid`). These facts are captured once at the
recorder's first acquisition and retained on every rewrite, so an exited writer
stays attributable without a process scan. Unset or blank strings are omitted;
strings are trimmed and limited to 128 characters. Files from older writers
remain valid without this object. Malformed writer metadata fails a gate like
malformed counters. The top-pids table shows a compact `writer` column; `--json`
keeps the separate facts in `pids[].writer`.

## Lock events (`lock-stats/events.jsonl`)

The minute files say how busy the lock was, not which hold made a waiter give up. For that, the two
parties report it themselves at the source (smarty-dev#9290), so a watcher such as commswatch tails a
file instead of sampling `.lock` every 50 ms. Both locks are covered: the mesh `.lock` (`store:
"mesh-lock"`) and the SQLite state writer lock (`store: "sqlite"`, `BEGIN IMMEDIATE` through
`COMMIT`/`ROLLBACK`, including the R20 write fence).

Every process on a root appends to `<mesh>/lock-stats/events.jsonl`. This is a local file append,
never a mesh publish: publishing would need the very lock that is contended. One JSON object per line:

```json
{"v":1,"at":<epoch ms>,"kind":"held"|"wait-timeout","store":"sqlite"|"mesh-lock","class":<MeshLockClass>,
 "heldMs":<n, for held>,"waitedMs":<n, for wait-timeout>,
 "holder":{"pid":<n|null>,"process":<string|null>,"op":<MeshLockClass|null>,"host":<string>},
 "waiter":{"pid":<n>,"process":<string>,"op":<MeshLockClass>}}
```

- `held`: written by the holder when it releases after holding the lock **longer than** the
  threshold, `lockHeldEventMs` (a `MeshStore`, `MeshLock` or `SqliteStateStore` option; default
  1000 ms; `0` disables `held` events). `holder` is the process itself, with `op` its own class.
  There is no `waitedMs` and no `waiter`.
- `wait-timeout`: written by the waiter when a full-budget acquisition gives up with
  `FABRIC_MESH_LOCK_TIMEOUT` (`FABRIC_MESH_STATE_BUSY` at the bridge). There is no `heldMs`.
  `holder` names the holder as far as the lock's own record shows it: for `.lock`, the pid in its
  owner record; for SQLite, which keeps no owner record, the process holding the WAL write-lock byte
  (shm byte 120) in the kernel's lock table (Linux `/proc/locks`). Unreadable (another platform, a
  release in between, no single holder) means `pid` and `process` are `null`. `holder.op` is always
  `null` here: neither record carries the holder's class. Bounded tries (registry-fenced
  `withTryLock` scopes, zero-wait callers, the single-attempt R20 fence) fail by design while the
  lock is busy; they stay `tries` in the minute files and emit no event.
- `class` (and `waiter.op`) is the caller class from the table above. SQLite writes use the same
  classes: `put`/`delete` `put/delete`, `writeBatch` `writeBatch` (or `bridge`), `confirmWritable`
  `heartbeat/confirm`, `exclusive` `custody`, the write fence, `retire` and imports `other`.
- `process` is the short process name (`/proc/PID/comm`, else `process.title` for the process
  itself), never the command line, which may hold secrets. `host` is the sanitized host label used
  in the minute files' names. `heldMs` and `waitedMs` are whole milliseconds.

Each line is written with `O_APPEND` and a single `write()` of at most 4096 bytes (`PIPE_BUF`), so
lines from concurrent processes never interleave; process names are truncated to keep a line under
that. Before an append that would take the file past 1 MiB, it is renamed to `events.jsonl.1`
(replacing the previous one; one old file is kept). The file is created mode 0600, never through a
symlink, in the same private `lock-stats` directory as the minute files (same safety checks), and
the minute-file reader and pruner ignore it. An I/O error is never thrown into, and never waits in,
the lock path: it is counted and reported at most once a minute in the profile's private
`fabric-lock-stats.log`. `PI_FABRIC_LOCK_STATS=0` turns events off together with the minute files.

To consume it, tail `events.jsonl` by name, not by descriptor: when its inode changes or its size
drops below the read offset, finish reading `events.jsonl.1` from the old offset if needed, then
reopen `events.jsonl` from offset 0. Skip a line that is not valid JSON or has another `v`.

`PI_FABRIC_COMMIT_STATS` (commits and bytes by key family) and `PI_FABRIC_COMMIT_TRACE` (keys and
callers) complement this view.
