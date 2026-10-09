# Resident runs: bounded references and permanent legacy retirement

## Scope

Windows: unchanged from main in this PR; follow-up Smarty-Pants-Inc/smarty-dev#5132.

All PR #481 behavior changes below are POSIX-only, controlled by the shared
`retentionV2Enabled()` platform gate. Windows uses main 9387af87's original
request-poll maintenance, uncached ownership scan, synchronous full-result
recovery, streaming run collection and compaction proof ordering. It gets no new
maintenance/retirement timer, polling count/tree/fuel/retry bound, custody hook,
resampling or audit output. Existing main Windows event retention and request
expiry remain active and unchanged. Compatibility policy keys are accepted but
do not activate V2 on Windows. Forced-win32 tests compare tails and counts
against the pinned main implementation; they are not native Windows CI evidence.

## POSIX bounded references

PR #704 replaces the former 50 ms request/claim poll and 100 ms idle maintenance
poll with request/configuration events, a 60-second reconciliation, and bounded
100 ms one-shot continuations only while the collector has scan debt. The
ownership cursor remains bounded to at most 64 handles/runs and 2 ms per slice. Native custody changes and `runs/` directory
changes queue a delta pass, never restart an in-flight historical cursor. An
identity watermark avoids repeating historical status/tree reads on ordinary
run creation or UI progress. Periodic revalidation starts at a factory boundary,
not halfway through a suspended walk; snapshots refresh no more often than
60 seconds after completion. Incomplete, unreadable, overflowing or time-limited
proofs veto collection with `*`. No prefix restart, no unbounded snapshot copy.
The cached set is never collection authority: expiring an exchange performs a
fresh targeted writer/tree proof (including indexed actor runs) within its own
2-ms/64-run budget. In-place nested/status changes are therefore fenced even
without a run-set mtime change. Regular native compaction/deletion retains the
existing worker-exit, descendant, result preservation and actor-reference guards.
The resident collector also retains every still-managed run tree until the
manager releases its custody, even after checked worker exit: exchange debt can
clear first, but deleting that tree would destroy the next fresh exit proof for
a deferred stopped-actor exchange. The separate legacy policy below grants
**no deletion authority**.
An interrupted proof resumes at its failed predicate, not at the initial status
read. Each predicate can retry on two later ticks; timed-out units veto the
current snapshot and retry on the next completed delta pass rather than becoming
60-second cached ownership facts. Oversized tree-proof units are retained without
starving later entries.
Pending full-result custody records have **no** 1-MiB protocol cutoff: recovery
retains its nested DFS continuation and reads one source in 64-KiB chunks across
count/time slices. Parsing and atomic sink discharge use a one-record slow path.
Source identity/length/timestamp changes veto publication; successful settlement
and shutdown sinks discharge only their own exact outcomes.

### Native event collection: bounded progress, not repeated deadline aborts

Already bounded event tails return before mutation-only exit/removability walks;
reading that suffix is not collection authority. Actual replacements retain both
fresh native-exit/tree proofs, result/allowlist guards and latest-run generation
checks. This removes four redundant status reads per no-op compaction, keeping
historical prefix reads within the eight-read activity-test bound even when a
native directory cursor visits that prefix during the run phase.

A resident run transaction is a soft 5-ms slice boundary: the collector stops
**between** complete transactions. Recovery and registry preparation remain
resumable. A transaction has a 64-entry tree cap, fresh size checks at mutation
boundaries and 4,096 predicate checks of fuel; oversized/changed/unknown trees
still veto. A slow three-file run must not restart its fresh safety proof on every
5-ms tick and remain over its byte cap forever. Status, reply and saved results
remain unchanged; the event cap and the shared atomic-write retry path are not
relaxed. These compaction optimizations are POSIX-only; Windows retains main's
existing compaction code path unchanged.

## Scope cut: POSIX-only retirement, no automatic legacy source deletion

Linux can keep a writable file reference in an **unreceived `SCM_RIGHTS` message**
after the sender closes its descriptor. No task's fd table, cwd/root or maps need
show that file. A stable, completely visible `/proc` census therefore cannot
prove absence of writable file custody. Compressing a snapshot and then deleting
the source can lose later writes through that queued descriptor.

On POSIX, this policy does **not compress, copy, unlink, recursively remove, or
expire** legacy source trees. It only moves them with same-filesystem `rename(2)` into
`runs-retired/YYYY-MM-DD/slice-*/RUN_ID/`, inside the residency root. Rename keeps
the directory and file inodes linked. A hidden descriptor received later still
writes into the moved file; the retained bytes are not a stale compressed copy.
There is no `/proc` census, mount/namespace completeness proof, compression child,
bundle append, manifest or deletion transaction in this worker anymore.

The hot `runs/` directory shrinks, reducing startup and sweep work. **Total disk
usage does not shrink.** Deletion is cut from this PR: a follow-up needs an
independently reviewed opaque-custody proof or an owner-run offline step that
actually discharges outstanding custody. More `/proc` visibility is not that proof.

## Policy and live writers

```json
{
  "retention": {
    "legacyRunArchiveEnabled": true,
    "legacyRunArchiveAgeMs": 172800000
  }
}
```

The legacy public class and policy names are retained for compatibility; their
meaning is now rename-only retirement. Default age is 48 hours from **finishedAt**,
not directory mtime. Normalized ages range from one hour to 365 days. The worker
shares the host's live overlay; `legacyRunArchiveEnabled: false` disables moves.

An independent 250 ms background tick discovers at most 32 runs/20 ms, yielding
between asynchronous filesystem operations. Only one slice runs at a time.
Shutdown joins the current slice. Discovery holds a directory cursor, not a
snapshot of all historical run names. Empty slices may remain; they hold no run
bytes and are never automatically removed.

Rename preserves descriptor writes, but a **known live writer that reopens by
original path** must not lose that path. These guards remain:

- Owned private, unaliased source/destination directories and ordinary owned,
  single-link allowlisted files. Unsupported filesystem moves fail, never copy.
- The legacy root has **no** worker sessionId/PID or processStartTime, and a valid
  completed/failed/stopped status with sufficiently old finishedAt.
- Every nested run needs terminal/age evidence; any saved descendant PID must be
  absent (a zombie leader is not inferred dead). Unknown status, transports,
  metadata, links, unresolved workers, unknown contents and oversized trees veto.
- Pending deliveries/follow-ups, results, completion recipients and cleanup
  obligations veto. Terminal status alone does not infer publication.
- Manager custody, actor latest/in-flight run, removal markers, unjoined resident
  admission handles, pending requests/processing and delivery-outbox veto.
- Fresh tree identity and reference-generation checks bracket the move. If known
  custody appears after rename, move back without overwriting an existing run ID.
  A conflicting ID or namespace fault leaves the moved bytes retained and reports
  an error rather than destroying either copy.

The guards are conservative protection for known live/path-based owners, **not**
a claim that all potential writers are discoverable. Unknown descriptor custody
is safe for bytes because the inode remains linked.

**All retention V2 behavior is POSIX-only in this PR. On `win32`, the host
never starts retirement or V2 maintenance.** Direct retirement calls are silent
no-ops before opening a run cursor, checking candidates, creating a destination
or moving/deleting bytes. Main had no retirement audit, so none is emitted.

The Unix group/other permission-bit privacy checks stay intact on POSIX; native
Windows mode bits and `mkdir({ mode: 0o700 })` are not that privacy proof. Neither
a rejected Windows rename nor a simulated platform provides ownership evidence.
The compatibility policy keys remain accepted on Windows, but enabling them does
not enable retention V2 there. Main's native event compaction and acknowledged
request expiry remain cross-platform and unchanged on Windows. No legacy run is automatically
deleted on any platform.

Follow-up: Smarty-Pants-Inc/smarty-dev#5132 (owner fabric-v2). It tracks the
actor-specific bounded custody index and the entire Windows V2 half: ownership/
privacy evidence, polling/recovery bounds, tail compaction and native Windows
retention verification. Local simulation does not substitute for native CI.

Directory fsync is performed on POSIX. `archive-retention.json`
keeps checked/archived/skipped counters and the latest error; the historical
`archived` label now counts moved runs.

## Inspect and restore

Retired runs are ordinary directories. No tar tooling or decompression is needed.
Stop the owning resident and inspect the selected retained tree, then move it back
into an **empty** original slot. Never overwrite a live run with the same ID.
For example, in the stopped resident's root:

```sh
# Inspect the exact dated slice and ID first.
ls runs-retired/2026-10-04/slice-EXAMPLE/RUN_ID/
# Verify runs/RUN_ID does not exist; restore on the SAME filesystem.
test ! -e runs/RUN_ID && mv -T runs-retired/2026-10-04/slice-EXAMPLE/RUN_ID runs/RUN_ID
```

The `mv -T` example uses GNU coreutils; on other platforms use an equivalent
non-overwriting directory rename while the owner is stopped. Unique private
slice directories keep repeated run IDs from overwriting previously retired
bytes. A partial failure or restart needs no compressed-tail recovery: each
completed rename already leaves a complete ordinary source directory. The next
collector scans only remaining hot runs and never deletes older slices.

Any pre-existing `archive/` bundles or `.staging-*` bytes from the older branch
implementation are untouched. Inspect/recover those manually; this worker
neither resumes nor cleans up that superseded compression protocol.
