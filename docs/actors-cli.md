# Stop, remove or adopt a dead Main's durable actor

Run on the resident's machine, as its OS user, from any Fabric session or shell:

```sh
fabric-actors stop --resident <directory-or-prefix> --actor <id-or-name> --dry-run
# After the manual check below, use the exact rootId printed by dry-run:
fabric-actors stop --resident <directory-or-prefix> --actor <id-or-name> --confirm-dead-root <rootId>
fabric-actors remove --resident <directory-or-prefix> --actor <id-or-name> --confirm-dead-root <rootId>
```

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
makes the choice explicit rather than silently inferring death from partial
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

## Adopt a dead root's durable actor into a live root (smarty-dev#5919)

```sh
fabric-actors adopt --resident <dead-directory-or-prefix> --actor <id-or-name> \
  --into <live-rootId-or-resident> --dry-run
fabric-actors adopt --resident <dead-directory-or-prefix> --actor <id-or-name> \
  --into <live-rootId-or-resident> --confirm-dead-root <deadRootId>
```

Adoption moves the actor's custody to the live root's resident host and keeps
its id, name, instructions, model, topics, message history and queued mailbox
work; it does not replay old mesh events (future-only delivery). The dead root
gets exactly the stop/remove checks above: `--confirm-dead-root` must equal its
root ID and an unexpired (or unreadable) root lease always refuses. `--into`
must be another root in the same mesh whose resident host is live; its executor
refuses unless that root has a live root lease and a live root participant
(a live Main). Actors of another project are refused.

Both hosts' request channels are used. If the dead root's resident host still
runs, it first releases the actor (`releaseActor`: ends its run, parks queued
work in its queue file, flushes the row and withdraws its presence). If it is
down, the CLI proves it is not running and holds its `host-fence-establish.lock`
and `host.lock` for the whole operation, so it cannot restart while its registry
is edited offline. The live host (`adoptActor`) then moves the row under the
registry locks and loads the actor.

The move is two-phase with an intent file at
`<live residency>/adoptions/<actorId>.json`: intent, copy of the actor
directory (session scope only), remove from the dead root's registry, add to the
live root's registry, cleanup. The first registry write is the commit point.
Recovery runs at live-host start and before every adopt: an uncommitted move
rolls back, a crash between remove and add rolls forward from the intent
snapshot, a committed one finishes cleanup. A crash never leaves the actor lost
or in two registries. A released actor whose adoption was rolled back stays
released (not running) until the adopt is retried.
