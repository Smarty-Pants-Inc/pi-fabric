# Stop/remove a dead Main's actor or retire its offline residency

Run on the resident's machine, as its OS user, from any Fabric session or shell:

```sh
fabric-actors stop --resident <directory-or-prefix> --actor <id-or-name> --dry-run
# After the manual check below, use the exact rootId printed by dry-run:
fabric-actors stop --resident <directory-or-prefix> --actor <id-or-name> --confirm-dead-root <rootId>
fabric-actors remove --resident <directory-or-prefix> --actor <id-or-name> --confirm-dead-root <rootId> \
  --main-stopped --evidence-file <herdr-and-ps-check.txt>
```

## Retire an offline residency of a dead Main (smarty-dev#20687)

Use this **Linux-only, manual, offline** operation when the root Main and its resident
are already gone but the retained residency keeps attracting wake attempts or
`ResidentWakeConfigMismatch` alarms. It does not stop or signal a running launcher,
Pi, actor or worker. It is not automatic retirement, actor adoption, disk reclamation
or release handover. For a live resident, use its owning Main and the appropriate
[drain operation](resident-drain.md); do not use retirement to force it offline.

After the manual Main/session/process check below, run on the resident's machine,
as the OS user who owns its files:

```sh
fabric-actors retire-residency --resident <directory-or-unique-hex-prefix> \
  --mesh-root <mesh-directory> --confirm-dead-root <exact-rootId> \
  --main-stopped --evidence-file <herdr-and-ps-check.txt>
# Add --dry-run to inspect the same checks and the planned actions without changing files.
```

`--main-stopped` remains an **operator attestation**, not automatic death detection.
Both execution and dry-run require the exact root confirmation, attestation and
nonempty evidence (`--evidence <text>` also works, with the same 64-KiB cap). The
same strict root lease, participant freshness, local Main PID/birth identity and
ownership checks as offline `remove` apply. A missing Main identity, foreign-host
record, malformed custody data, a retained wake actor without a matching registry
identity, or unsupported kernel fence refuses; confirmation
cannot override it. A clean resident may have withdrawn `owner.json`; its absence
is allowed, but `host.lock` must still contain a proven-dead PID.

It refuses a live owner/host-lock/Main-generation PID, held startup/host/wake/intent/handover or Main
publication fence, a blocked lock waiter, live recorded launcher, or retained
watchdog custody. Pending requests, processing, delivery-outbox entries and
unconfirmed runs/tasks also refuse. It reads both configured actor registries and
their on-disk queue checkpoints without starting a host or restoring an actor.
Queued, parked, overflow, settled-window, pending delivery outcomes, dead-letter
work and stop archives refuse with `FABRIC_ACTOR_QUEUED_WORK` and counts (nonempty
dead-letter and stop-archive files are counted as files, not guessed item totals). This
offline check is conservative: all nonempty dead-letter files and stop archives,
and any in-flight/preparing/removing actor, refuse; the check never guesses whether
recovery is complete. Resolve/adopt that work first. There is **no `--drop-queued`
or `--actor` option** for residency retirement. Neither actor registries nor actor
directories outside the residency are changed.

On success the result prints `retired:true`, `archive` and `auditFile`. ONE
same-filesystem directory rename moves the selected residency to
`<mesh>/residency-retired/<hash>.<time>.<uuid>/tree`; an exclusively reserved
parent prevents overwrite or merging. EXDEV refuses, with no copy fallback.
`operator.json` beside the tree keeps the evidence, operator identity, root,
time, tool observations and exact plan. The source inode and every original
file/byte stay linked in that tree, including nested `runs-retired/`, config,
owner/custody records and **all `*.jsonl` traces**. No original residency or actor
file is deleted, truncated, compressed or rewritten. Disk usage does not shrink. New empty fence files may
be created; their inodes are retained, including the inert Main publication fence
outside the moved tree. A refusal after creating an audit/fence directory keeps
those added files too; it never cleans up by deleting data.

The destination is **outside** `<mesh>/residency/`, so ordinary publication,
dormant-control and retry discovery no longer find its config/wake routes. No
wake journal or archived mesh event is edited by this command. Existing retry
maintenance can acknowledge its now-absent path through its normal path; the
moved failure receipts and traces remain untouched. This is not a tombstone:
do not restart/resume the old Main or explicitly start its former config. A new
explicit start can create a new residency for that root.

`--dry-run` prints `retired:false` and `plan` with the exact proposed rename,
audit path, paths execution would create, and `delete:[]`. It probes only existing
lock files and creates no files or directories. It reports current refusals just
as execution does; it does not reserve the destination or promise the same
random/time suffix on a later invocation. Check the JSON exit status before
assuming retirement happened. If a namespace-sync failure occurs after rename,
the error names the retained tree and audit; inspect them before restarting
the old root.

To inspect, use the printed archive path directly. Any restore is a separate
manual maintenance action: first establish that all owners remain stopped, then
rename `tree` back on the **same filesystem** into an absent original slot. Never
overwrite a new/live residency. Restoring routing metadata re-enables wakes.
There is no automatic restore or archive deletion operation.

## Stop and remove never drop queued work silently (smarty-dev#16416)

`stop` refuses an actor that still holds work: queued, parked, overflow, pending settled-window or dead-letter
work. The error names each count, and nothing changes. An activation in flight alone never refuses a stop: the run is
stopped, and its item, unless a caller's ask is waiting on it, is archived as below. To stop it anyway, add
`--drop-queued`. The work is then **moved**, not deleted, into one archive file in the actor's directory,
`<actor dir>/stopped-queues/<stamp>-<sha256-12>.jsonl`.
The printed result carries `actor.stoppedQueue` with the archive path, its full sha256 and the counts. The actor's
message log gets a `MOVED-TO <path>` line with the same sha256. To restore the work, recreate the actor and call
`agents.requeueArchived({ id: <new actor>, archive: <path> })` from its root's Main (see agents.md). The new
actor may have the same name: the path still finds the archive after the removal moved it to session-archive/.
`remove` refuses an actor that has stop archives or held dead letters, or live work its removal would drop:
queued, parked or overflow items, a callerless item in flight or a pending settled window (a caller's ask in
flight does not refuse it: the caller hears the abort). The refusal comes before any effect: the actor keeps
running with its queue and run, nothing is archived, and the error starts with `FABRIC_ACTOR_QUEUED_WORK` (stdout
gets `{"ok":false,"errorCode":"FABRIC_ACTOR_QUEUED_WORK","queuedWork":{...}}` with the counts).
`remove --drop-queued` (or `agents.remove({ id, dropQueued: true })`) first archives that live work into
`stopped-queues/` as `stop --drop-queued` does, then removes the actor and keeps its archives under
`<mesh root>/session-archive/`.

## Offline remove (dead resident host): `--main-stopped` is an operator attestation (smarty-dev#7817)

When the root's resident host is dead, `remove` runs offline in the CLI and also needs `--main-stopped` with `--evidence <text>` or `--evidence-file <path>` (text, capped at
64 KiB). **`--main-stopped` is an OPERATOR ATTESTATION, not a machine proof.** Chief-of-staff accepted it on
2026-10-09 for the operator of the dead Main's own fleet, or Light for the #7231 waves. The automatic proof is
**smarty-dev#7956**. The evidence (the Herdr pane/agent listing and the process check for that Main's session) is
kept as `operatorAttestation`, with the operator (`$USER`, `PI_FABRIC_AGENT_NAME` or session id), the root id and
the time. The tool adds its own observation at removal time (`toolEvidence`): every root participant record
(pid, host, release, last seen), a `/proc/<pid>` check of each pid on this host with its start time against
the record, the owner lease state, and the time. The record is archived as `<id>.operator.json` (in
`SHA256SUMS`, beside the registry row) and kept in the removal record.

The actor is never deleted: its directory is moved by ONE `rename(2)` into a fresh, exclusive archive directory
`<residency>/archives/<id>.<time>.<random>/tree` (created by `mkdir`, so an existing name refuses; never
overwritten or merged). `rename` follows no link; the moved entry must be the pinned directory (same inode, this
user), or it is moved back and the removal refuses. A cross-filesystem archive (EXDEV) refuses; there is no copy.
Deleting archives is left to retention (smarty-dev#7916). A live resident's operator `remove` path also holds
the root's Main publication fence across its final liveness check and actor-tree deletion (smarty-dev#8090).
It does not require `--main-stopped`. A normal removal by the root's own Main uses the same fence without
the dead-Main liveness check.

With `--main-stopped` the tool verifies every root participant record's Main process (pid, host, start time):
the pid must be gone from this host, or reused with a different start time. A record without that identity,
from another host, or a root with no participant record at all refuses as "identity unavailable"; the
automatic proof for roots whose records are gone is smarty-dev#7956. Offline removal holds the root's resident
startup claim (`host-fence-establish.lock`) and `host.lock` throughout, so no resident host starts meanwhile.
The offline remover holds the root's Main publication fence (an exclusive flock on
`main-publication-fences/<sha256(root)>.json`) across its checks and every destructive step: the root Main
does not publish its participant while it is held. The kernel drops it when the remover dies; there is no
time-based expiry. The live host's final operator check refuses a fresh, reloading, invalid or unreadable root
participant record. Accepted operator removals keep this requirement through pending runs and host restarts.
Where the kernel fence is unavailable, deletion refuses and keeps the actor tree; there is no PID or clock fallback.

The attestation never overrides an observation: a live root lease, a fresh, reloading or unreadable root participant, or a participant process alive on this host refuses. When
the resident host itself is dead, `remove` runs offline under its `host.lock` fence (dead holders, a claimable
flock, no waiter), re-checking before each destructive step. Where file ownership cannot be proven
(no `getuid`, Windows) remove refuses (smarty-dev#7858).

A bare directory prefix searches `$PI_FABRIC_MESH_ROOT/residency`; alternatively
pass `--mesh-root`. Without either, the default is
`$PI_FABRIC_PROJECT_ROOT/.pi/fabric/mesh` (or the current directory).
Explicit paths must be direct children of that residency directory; selector and
channel symlinks are refused. The resident directory and its channel files must
be owned by the invoking OS user: on POSIX by uid; on Windows by owner SID
(queried with PowerShell `Get-Acl`). If the Windows owner SID cannot be read,
or any owner differs from the current user's SID, the command refuses before it
sends a request. Ambiguous resident prefixes and actor names are
refused. Actor IDs and names are exact and scoped to the selected host's root,
not the caller's root.

## Explicit operator confirmation, not automatic death detection

`--dry-run` requires no confirmation and changes no actor state. If a confirmation
is supplied, it must still match the root exactly. Dry-run prints the
selected root ID, Main session ID, last recorded lease time (`lastLeaseTime`,
Unix milliseconds, or `null` when no lease is recorded), lease expiry and whether
a lease is live. Missing or mismatched confirmation refuses stop/remove and
prints the same evidence and manual-check instructions.

Before confirming, inspect **the root's Main session** and **its last lease
time**, and run **`herdr agent list` / `ps`** on the resident's machine to confirm
that no live Main serves that root. Do not restart/resume that Main during the
operation. An absent or expired lease is **not** proof that Main is dead.

Stop/remove require `--confirm-dead-root <rootId>` equal to the selected
resident's root ID exactly. **A wrong confirmation can interrupt a live Main's
actor.** This is accepted operator responsibility; spelling out the root ID
makes the choice explicit, so death is never silently inferred from partial
process evidence. There is no automatic live-Main census, startup marker,
platform split, or bypass flag. Automated detection/adoption is follow-up work
under **smarty-dev#5919**, not a safety claim of this command.

The resident executor reads the current root host lease strictly and uncached,
including immediately before mutation. An **unexpired root host lease always
refuses mutation, even with matching confirmation**; unreadable or invalid
current lease data also refuses. Shared root host lease publication is checked
as well. Dry-run reports a live lease without mutating so the operator can
inspect it. Confirmation cannot override these lease checks.

Stop cancels queued work and drains the actor's run, retaining its stopped
registry record. Remove first performs the same terminal stop/drain (so a
progressed worker cannot detach on caller abort), then uses the host's normal
removal path, including custody, participant cleanup and registry revocation.
It may return a `pending` state while the existing host finisher completes
cleanup. Other actors and the resident host are not stopped. No actor/registry
files are edited by the CLI, and it never starts or upgrades a resident host.

This requires a resident binary advertising `operatorActor`. Older binaries
are refused before a request is dispatched; no unsafe legacy command fallback
is used. Existing dead-root resident binaries need an explicit deployment of a
compatible host before this operator path is available.
