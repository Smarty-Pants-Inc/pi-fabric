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

`PI_FABRIC_COMMIT_STATS` (commits and bytes by key family) and `PI_FABRIC_COMMIT_TRACE` (keys and
callers) complement this view.
