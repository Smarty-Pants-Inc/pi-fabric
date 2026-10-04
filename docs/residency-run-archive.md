# Resident runs: bounded references and reversible legacy archives

The 50 ms request/claim loop does not prepare retention references. Independent
100 ms maintenance ticks advance one persistent ownership cursor, at most 64
handles/runs and 2 ms per tick. Snapshots invalidate on the managed generation or
`runs/` directory identity/mtime/ctime; otherwise they refresh no more often than
60 seconds after completion. Incomplete, unreadable, overflowing or time-limited
proofs veto collection with `*`. No prefix restart, no unbounded snapshot copy.
The cached set is never collection authority: expiring an exchange performs a
fresh targeted writer/tree proof (including indexed actor runs) within its own
2-ms/64-run budget. In-place nested/status changes are therefore fenced even
without a run-set mtime change. Regular compaction/deletion retains the existing
native worker-exit, descendant, result preservation and actor-reference fences.
A unit interrupted after reference preparation/recovery can retry on two later
ticks; permanently oversized units are skipped without starving later entries. A legacy archive is a separate,
reversible policy, **not** permission to weaken native exit/deletion guards.

## Policy

```json
{
  "retention": {
    "legacyRunArchiveEnabled": true,
    "legacyRunArchiveAgeMs": 172800000
  }
}
```

Default age is 48 hours from **finishedAt**, not directory mtime. Normalized ages
range from one hour to 365 days. `legacyRunArchiveEnabled: false` disables new
archival (including rechecking a changed policy before publication/deletion).
The host shares its live policy overlay with the archive worker.

An independent 250 ms background tick discovers at most 32 runs/20 ms, yielding
between asynchronous filesystem operations. Only one compression child runs at
a time. Host shutdown joins that child and its current slice.

All of the following are required:

- Owned private run directories, ordinary owned single-link allowlisted files.
- Legacy root has **no** worker sessionId/PID or processStartTime. Status is
  completed, failed or stopped, and a valid finishedAt is older than the age.
- Every nested run has the same terminal/age proof; any saved descendant PID is
  confirmed absent. Unknown status, transports, metadata, links, unresolved
  worker markers and unknown contents veto. Oversized trees are skipped.
- No deliveries/follow-ups remain; queued results, completion-recipient markers
  and cleanup obligations are not inferred delivered from terminal status.
- No live manager handle, actor latest/in-flight run, removal marker, unjoined
  legacy resident admission handle, pending request/processing or delivery-outbox.
  Reference generations are checked before staging and after compression/append.
- **Complete Linux /proc visibility**: no process descriptor, cwd/root or memory
  mapping references the run tree. Permission errors are uncertainty, not proof
  of absence. The process census is rechecked for newly born/reused PIDs. A
  1-second/32,768-entry proof limit yields a veto, not partial authorization.
  Unsupported platforms and restricted /proc skip automatically.

`archive-retention.json` records checked/archived/skipped counts, incomplete
process proofs and the latest error. A failed global visibility preflight defers
legacy discovery for 60 seconds, avoiding fruitless per-run tree scans. It is
only a veto; no successful preflight is cached as authorization.
Restricted multi-user hosts may therefore
archive nothing: do not pretend that looking only at this user's PIDs proves
that no privileged process has a file open. An appropriately privileged,
independently reviewed process-proof service is future work, not a bypass here.

## Bundle format and restore

Eligible directories are atomically moved from `runs/` into a private
`archive/.staging-*` directory. `tar` compresses the entire byte-preserved tree
outside the Node event loop. The fragment is appended and fsynced to the one UTC
daily `archive/YYYY-MM-DD.tar.gz` bundle, followed by a fsynced manifest line per
run in `YYYY-MM-DD.manifest.jsonl`. The archive directory is fsynced before the
staged source is removed. Process, tree and custody proofs are checked again.
Manifest records include run ID/status/finishedAt, archive time and the gzip
member offset/length. No archive TTL/deletion is imposed.

The daily file concatenates complete gzip-compressed tar members. GNU tar's
`--ignore-zeros` is **required** to read all slices (not just the first member):

```sh
# Stop the resident first; restore into an empty temporary directory to inspect.
mkdir -m 700 restored-runs
tar --ignore-zeros -xzf /path/to/residency/archive/2026-10-04.tar.gz -C restored-runs
# Inspect, then move selected ID directories back to residency/runs/.
# Do not overwrite an existing live run with the same ID.
```

Or list: `tar --ignore-zeros -tzf ...tar.gz`. A normal `tar -xzf` without
`--ignore-zeros` may silently restore only the first slice. File contents are
unchanged; tar preserves file names and mode/mtime. Restore is operator-driven.

## Interrupted slices

Any failure retains the original directory bytes in `.staging-*`; no automatic
cleanup discards these recovery sources. `pending.json` records the intended
bundle, append offset/length and IDs; the complete compressed fragment remains
alongside them. On an interrupted append, stop the host and inspect the manifest
and journal. Copy all artifacts first. A corrupt uncommitted bundle tail can be
trimmed to the journal's **offset** before retrying a restore of committed members.
If the bundle/manifest committed but source removal did not, both copies exist;
choose one, do not restore duplicate IDs over existing directories. Recovery of
staged source directories can simply move each ID back to an empty `runs/` slot.
The worker never automatically resumes an ambiguous interrupted transaction.
