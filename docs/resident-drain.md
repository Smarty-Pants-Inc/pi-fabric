# Resident drain preflight

`fabric-resident-drain` is a **fail-closed preflight**, not a working legacy
release migration. It selects one resident, checks its expected release and
native identity, and prints one JSON refusal receipt with a nonzero exit status.
It never stops an actor or host, dispatches a resident request, starts a target,
changes a cursor, removes a custody marker, or changes the resident's files.

```sh
fabric-resident-drain \
  --resident <resident-directory-or-unique-hex-prefix> \
  --mesh-root <absolute-mesh-root> \
  --expected-release <absolute-old-package-root> \
  --target-release <absolute-new-package-root>
```

All four arguments are required. The mesh root has no implicit fleet default.
Release arguments name retained package directories, not abbreviated Git SHAs or
mutable profile selectors. An expected-release mismatch or unverifiable owner
refuses before any transition. An older host without launch attestation also
refuses; its immutable extension path can identify a mismatch but cannot prove
launcher custody.

Even an attested current host refuses. No supported resident operation closes
all admission while preserving accepted queues and outcomes:

- `setActivationFilter` skips matching events; it is not a queue-preserving hold.
  Matching mesh events can be consumed, not retained behind the cursor.
  Direct calls are not covered by that filter.
- `pauseForRelease()` is an internal actor gate, not an operator request. The
  retained release transaction also needs separate request, control and lifecycle
  gates and checked publication barriers.
- `releaseChange` unconditionally defers before arming those gates. Its successful
  request exchange does not mean the release changed.
- `fabric-actors stop` is terminal cancellation: it drops queued work. It must
  not be used as an installer drain.

Persistent host close retains committed actor queues better than terminal stop,
but an external quiet-state scan followed by shutdown can race a new admission.
A clean direct-child exit or empty process group cannot prove an escaped helper
has exited. These are the same boundaries that keep automatic release following
disabled; this command does not bypass them.

## Read-back and rollback

The receipt reports a refusal in `preflight`, with the observed resident identity,
a reason, elapsed time, and rollback status `not-needed`: no transition was
attempted. Read-back of the owner and actor queue should still show the old
serving generation. The native regression checks this with acknowledged queued
work and then observes that work complete exactly once on the old release.

Do not launch the old release as a rollback while any target may still execute.
A future cooperative drain may roll back only after it proves target startup
admitted no work **and** the complete target attempt exited. After target work
admission, a read-back failure needs an operator; it cannot authorize a competing
old resident.

The remaining implementation is a preserving resident-owned admission/drain
endpoint plus complete attempt custody and exit receipts. It cannot be retrofitted
into an already-loaded legacy host by running this CLI. Until those prerequisites
exist, leave the old resident serving and arrange any manual maintenance window
through its owner; a quiet window alone is not a machine-verified drain.
