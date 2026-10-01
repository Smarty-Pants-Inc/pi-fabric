# Resident release following (#2615)

A live Main chooses the release it actually loaded; the **existing detached
launcher** owns the irreversible handover. This is not a new daemon, a fleet
scanner or an installer migration.

## Protocol

1. Each Main runtime publishes a birth-bound, nonce-qualified intent. Native
   `/reload` activation reconciles an already-owned root, including while its
   successor is staged; it never creates an empty host merely to follow a release.
2. A captures its actual effective launch specification. A and B pin real release
   paths, the whole package-local JS closure, resolved runtime/binaries and all
   configuration fields. Selected live model/kernel overlays belong only to A.
   Storage topology must remain identical and both releases advertise
   `fabric-resident-1` (a compatibility promise, not a schema-migration engine).
3. A reversibly gates file/control/lifecycle ingress and both actor scopes. Runs,
   caller-bound continuations and finalizers finish naturally. Queues and registry
   checkpoint before cursors; result publication, completion delivery and control
   ACKs join before release. Untouched backlog retains its retry budget.
4. Under the root transaction flock, A's existing launcher validates its own
   child PID/birth/token and the exact plan, pins A/B in memory and writes an
   immutable custody receipt **before A exits**. Cancellation and custody share
   one hard-link CAS. An unrelated launcher cannot consume the plan.
5. Only receipt-bound, drained A closes and releases the stable `host.lock`
   inode. The launcher starts B once. B acquires that fence, restores actors but
   gates business work, and runs an isolated real-worker startup probe. No prompt
   or inference is sent; the worker requires B's nonce-bound loaded-extension
   activation ACK and a correlated Pi RPC startup response.
6. Before durable terminal success, a dead/obsolete Main or failed B causes
   birth-validated cleanup of that launcher's owned child and observed
   descendants. TERM is not exit proof: confirm their exit and the same free
   fence before the single supervised A fallback. A uses its frozen specification,
   never desired `config.json`. After success, B publishes one `host.reloaded`
   `{old, new, transaction}` event and resumes the preserved backlog.

## Bounds and safe failures

Prepare cancels after 120 seconds without cutting a run. Each staged startup has
30 seconds; owned cleanup allows 5 seconds TERM then 5 seconds KILL. The retry
record is persistent per root/target artifact, so reloads and config changes do
not create loops. No manual retry-reset API is introduced in this change.

Unproved capability, artifact, storage boundary, launcher custody or birth/fence
identity means retain A. Unknown termination/fence or failed A recovery is an
explicit blocked transaction, never permission to spawn a duplicate. An
indeterminate `host.reloaded` publication is not retried or followed by business
activation (exactly-once recovery needs a separate durable event outbox). Once a
terminal write was attempted, an error may follow an already-visible rename: the
launcher retains B rather than risking cutting newly admitted work for rollback.
It reports terminal uncertainty; storage-fault recovery is not fabricated success.

The first transition **requires a capable A host and launcher**. Installed B71
predates this protocol: loading B in Main cannot retrofit custody into that live
launcher. It stays stale-but-served with an explicit deferred diagnostic; use the
existing installer drain/exit proof for initial capability rollout and rollback.
No automatic non-Linux or mixed-legacy takeover is added.

Assumptions: trusted same-user private root and retained immutable artifacts,
healthy compatible storage, a surviving launcher, and usable known-good A.
Same-user malicious artifact rewrites, machine/storage/controller loss and fleet
activation coverage are outside this transaction's recovery guarantee. A
Sol-max process-relaunch security review and fleet activation audit are required
before rollout. Native-Pi proof controllers and immutable release copies are
retained in the task evidence, not in `.local/` or the package.
