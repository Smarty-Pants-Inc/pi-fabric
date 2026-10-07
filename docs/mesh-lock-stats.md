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
| `bridge` | `publishBatch` (bridge imports) and the bridge presence mirror's `writeBatch` |
| `other` | anything else |

For each class and wall-clock minute: acquisitions, wait time (request to custody) and hold time
(custody to release) as sums, maxima and histograms (bounds 1, 2, 5, 10, 20, 50, 100, 250, 500,
1000, 2500, 5000, 10000 ms), full-budget **timeouts** and failed bounded **tries** (registry-fenced
or zero-wait callers, which fail by design while the lock is busy).

Each process writes `<mesh>/lock-stats/<host>-<pid>.json` just after every minute in which it took
the lock, and at exit. The file holds at most the last 60 minutes and is replaced by an atomic
rename, without fsync and without any lock. Files untouched for 24 hours are pruned. Nothing runs
before a process's first acquisition. Set `PI_FABRIC_LOCK_STATS=0` to turn recording off for a
process; test runs set it, and the lock-stats suites opt back in.

## Usage

```sh
fabric-mesh-lock-stats [--mesh DIR] [--minutes N] [--top K] [--json] [--max-busy PCT] [--max-timeouts N]
```

The mesh root resolves as for `fabric-participants`. The window is the last N complete minutes
(default 10, at most 60). Busy % is the summed hold time over the window's wall time. `--max-busy`
and `--max-timeouts` exit 3 when exceeded, for a stage gate such as
`fabric-mesh-lock-stats --minutes 60 --max-busy 30 --max-timeouts 0`.

`PI_FABRIC_COMMIT_STATS` (commits and bytes by key family) and `PI_FABRIC_COMMIT_TRACE` (keys and
callers) complement this view.
