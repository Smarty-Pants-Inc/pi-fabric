# Cooperative resident drain

`fabric-resident-drain` negotiates a **reversible hold and checkpoint** with a
compatible loaded resident. It is not a working release migration. A healthy
operation holds admission, lets accepted work finish naturally, checkpoints and
reads back custody, then resumes the same old resident. It returns nonzero JSON
`containment-unavailable`: complete attempt exit and rollback remain disabled.
It never stops an actor or host, starts a target, or removes a custody marker.

Legacy or uncertain roots retain the read-only preflight: they refuse without
request dispatch or changes to resident files. New source cannot retrofit the
endpoint or containment into an already-loaded legacy host.

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
refuses before any transition. An older host without the immutable birth-bound
launcher identity also refuses; its extension path can identify a mismatch but
cannot prove launcher custody.

A cooperative host must advertise `fabric-resident-drain-1` and `releaseDrain`.
The CLI validates its immutable launch snapshot, target artifact and unchanged
topology, plus the existing live owning Main process, session and generation.
It never creates a Main attestation or starts a host to satisfy these checks.
A finite timeout resumes A without aborting accepted work; unreadable or
uncorrelated terminal evidence reports `needs-operator`, not successful service.

These legacy operations are not substitutes for cooperative PREPARE:

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

Legacy refusal reports `preflight`, `unchanged:true` and rollback `not-needed`.
A negotiated operation reports its resident-generated ID, held/checkpointed
state, actual resume and checked read-back. `containment-unavailable` is partial
progress, never release success. It has `unchanged:false` because PREPARE changed
admission briefly, and rollback `not-attempted` because no target started.

Public JSON contains only allowlisted evidence. Paths are relative to the explicit
mesh root. Authentication tokens use short SHA-256 fingerprints; private config,
Main nonce and complete checkpoint contents are not printed. Durable root-local
receipts retain private correlation data and must not be copied into public logs.
Native regressions check the same serving owner, unchanged accepted retry evidence
and exactly-once work completion after resume; concurrent input stays behind its
cursor while held.

Do not launch the old release as a rollback while any target may still execute.
A future cooperative drain may roll back only after it proves target startup
admitted no work **and** the complete target attempt exited. After target work
admission, a read-back failure needs an operator; it cannot authorize a competing
old resident.

The remaining dependency is complete attempt-owned containment and exit receipts
(smarty-dev#11962). The frozen containment interface defaults to refusal. A real
adapter must run in launcher custody with its retained native handles; neither
the CLI nor the endpoint reopens a scope path from `owner.json`. The launcher alone
writes the immutable `attempt-launch-<attemptId>.json` beside `owner.json`, before
releasing the host. Its full receipt must match the launch environment and native
IPC receipt field for field. Readers use an owned, no-follow descriptor boundary;
a missing or different receipt never grants containment. A free lock, missing
owner or direct-child exit cannot authorize B or an A fallback. Leave
legacy residents serving and arrange any manual maintenance through their owner;
a quiet window alone is not a machine-verified drain.
