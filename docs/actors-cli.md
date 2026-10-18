# Stop or remove a dead Main's durable actor

Run on the resident's machine, as its OS user, from any Fabric session or shell:

```sh
fabric-actors stop --resident <directory-or-prefix> --actor <id-or-name> --dry-run
# After the manual check below, use the exact rootId printed by dry-run:
fabric-actors stop --resident <directory-or-prefix> --actor <id-or-name> --confirm-dead-root <rootId>
fabric-actors remove --resident <directory-or-prefix> --actor <id-or-name> --confirm-dead-root <rootId> \
  --main-stopped --evidence-file <herdr-and-ps-check.txt>
```

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

## Recover a close-timeout actor run

A close-timeout recovery receipt is not permission to delete custody. The restored activation must match the saved run and answer; its normal actor delivery and queue acknowledgement are still the only point that settles custody. New runs use the host-recorded process-scope path. A legacy run without that receipt requires a machine-proven absent cgroup scope and this operator command:

```sh
fabric-actors recover-close-timeout --run-root <resident-root>/runs --run-id <32-hex-run-id> \
  --scope-unit <fabric-scope-XXXXXX.scope> --authorized-by <org-or-lead-name> \
  --authority-ref "<approved-issue-or-comment-URL>"
```

Use `--authorized-by` and `--authority-ref` only after the owning Main no longer resolves in the resident's participant directory, every recorded Main process is proven gone, and its participant heartbeat is older than the runtime's normal stale bound. The receipt stores the operator, authority reference and the tool's participant/lease/process snapshot. Missing, unreadable, remote-only or fresh owner state refuses. If the Main is live, the CLI is owner-only: invoke it from that owning Main; operator authority cannot override a live or unknown root. Do not infer death from stderr, event logs or a hand-written PID check. The receipt is one-shot and does not itself remove `unresolved-worker.json`.

When the old resident is still running code that cannot read the receipt, create the receipt first, then restart that exact resident under the installed release. There is no host-level `fabric-actors stop` operation. The following bounded sequence signals only the launcher incarnation recorded by the resident, waits for both launcher and child owner to exit, then starts the installed package's new launcher in wake mode. It deliberately refuses if the old owner lacks the launcher birth identity; do not substitute a PID-only `kill`.

```sh
set -euo pipefail
ROOT=$(dirname "$(realpath -- "<resident-root>/runs")")
OWNER="$ROOT/owner.json"
node --input-type=module - "$OWNER" <<'NODE'
import fs from 'node:fs';
const ownerFile = process.argv[2];
const owner = JSON.parse(fs.readFileSync(ownerFile, 'utf8'));
const launcher = owner.handover?.launcher;
if (!Number.isSafeInteger(launcher?.pid) || typeof launcher?.processStartTime !== 'string' ||
    !/^[0-9]+$/.test(launcher.processStartTime) || !Number.isSafeInteger(owner.pid) ||
    typeof owner.processStartTime !== 'string' || !/^[0-9]+$/.test(owner.processStartTime)) {
  throw new Error('No exact launcher and owner birth identities; safe restart is unavailable');
}
function birth(pid) {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/);
    return fields[19];
  } catch (error) {
    if (error.code === 'ENOENT') return undefined;
    throw error;
  }
}
if (birth(launcher.pid) !== launcher.processStartTime || birth(owner.pid) !== owner.processStartTime) {
  throw new Error('Resident process identity changed; no signal sent');
}
process.kill(launcher.pid, 'SIGTERM');
const deadline = Date.now() + 30_000;
while (Date.now() < deadline &&
    (birth(launcher.pid) === launcher.processStartTime || birth(owner.pid) === owner.processStartTime)) {
  await new Promise(resolve => setTimeout(resolve, 100));
}
if (birth(launcher.pid) === launcher.processStartTime || birth(owner.pid) === owner.processStartTime) {
  throw new Error('Resident did not provide a bounded native exit; do not start a second host');
}
NODE
ACTORS=$(realpath -- "$(command -v fabric-actors)")
RELEASE=$(dirname "$(dirname "$ACTORS")")
node "$RELEASE/dist/residency/launcher.js" --config "$ROOT/config.json" --wake
```

Run the restart on the resident machine as the same OS user. Confirm the installed `fabric-actors` resolves to the intended new release before starting. If owner.json has no exact launcher identity, the process does not exit within 30 seconds, or the launcher rejects the existing config, stop and escalate instead of signaling another PID or starting a second host. A resident host on the new release reads the durable receipt during restore; queued activations continue only after the saved answer is delivered and acknowledged. Refs Smarty-Pants-Inc/smarty-dev#4313.
