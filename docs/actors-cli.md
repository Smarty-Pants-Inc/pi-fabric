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
Deleting archives is left to retention (smarty-dev#7916). A live resident's own `remove` path is unchanged
(no `--main-stopped`; smarty-dev#8090).

With `--main-stopped` the tool verifies every root participant record's Main process (pid, host, start time):
the pid must be gone from this host, or reused with a different start time. A record without that identity,
from another host, or a root with no participant record at all refuses as "identity unavailable"; the
automatic proof for roots whose records are gone is smarty-dev#7956. Offline removal holds the root's resident
startup claim (`host-fence-establish.lock`) and `host.lock` throughout, so no resident host starts meanwhile.
The offline remover holds the root's Main publication fence (an exclusive flock on
`main-publication-fences/<sha256(root)>.json`) across its checks and every destructive step: the root Main
does not publish its participant while it is held. The kernel drops it when the remover dies; there is no
time-based expiry. The live host path's equivalent is smarty-dev#8090.

The attestation never overrides an observation: a live root lease, a fresh, reloading or unreadable root participant, or a participant process alive on this host refuses. When
the resident host itself is dead, `remove` runs offline under its `host.lock` fence (dead holders, a claimable
flock, no waiter), re-checking before each destructive step. Where file ownership cannot be proven
(no `getuid`, Windows) remove refuses (smarty-dev#7858).

A bare directory prefix searches `$PI_FABRIC_MESH_ROOT/residency`; alternatively
pass `--mesh-root`. Without either, the default is
`$PI_FABRIC_PROJECT_ROOT/.pi/fabric/mesh` (or the current directory).
Explicit paths must be direct children of that residency directory; selector and
channel symlinks are refused. Ambiguous resident prefixes and actor names are
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
