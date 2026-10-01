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
   configuration fields. Bundled Pi cold-start attestation uses the birth-checked
   launcher's generic runtime; successors use their custody-pinned runtime, not
   Pi's own executable path. Selected live model/kernel overlays belong only to A.
   Storage topology must remain identical and both releases advertise
   `fabric-resident-1` (a compatibility promise, not a schema-migration engine).
3. A reversibly gates file/control/lifecycle ingress and both actor scopes. Runs,
   caller-bound continuations and finalizers finish naturally. Queues and registry
   checkpoint before cursors; result publication, completion delivery and control
   ACKs join before release. A non-destructive AgentManager receipt checks
   pending/unregistered transports and tracked/preserved/nested run trees;
   terminal UI failure never proves worker exit. Unknown identities or exit
   cancel preparation without stopping work. Delivered lifecycle cursor/once
   deletion obligations must have confirmed storage receipts, never replayed
   operations. Untouched backlog retains its retry budget.
4. Under the root transaction flock, A's existing launcher validates its own
   child PID/birth/token and the exact plan, pins A/B in memory and writes an
   immutable custody receipt **before A exits**. Cancellation and custody share
   one hard-link CAS. An unrelated launcher cannot consume the plan.
5. Only receipt-bound, drained A closes and releases the stable `host.lock`
   inode. The launcher starts B once. B acquires that fence, restores actors but
   gates business work, and runs an isolated real-worker startup probe. No prompt
   or inference is sent; the worker requires B's nonce-bound loaded-extension
   activation ACK and a correlated Pi RPC startup response.
6. If Main is lost before B is attempted (or B validation fails before launch),
   the launcher can start one supervised A fallback from its frozen specification,
   never desired `config.json`. **Once B launch is attempted, a failed B blocks
   automatic fallback.** Birth-validated cleanup stops only observed processes;
   sampled ancestry, direct-child exit and a free host fence do not prove that
   reparented helpers exited. No second generation may start on that evidence.
   After success, B publishes one `host.reloaded` `{old, new, transaction}` event
   and resumes the preserved backlog.

## Deliberate scope cuts / containment follow-up

Automatic fallback after an attempted B is deferred until a separate change
provides an attempt-owned membership boundary established before any child can
escape (for example, an appropriately delegated cgroup) and checked whole-
attempt exit receipts. Tracking request for the repository owner: **file a
containment follow-up under #2615 before re-enabling failed-B fallback**. This
review task has no GitHub-write authority, so it does not invent an issue number.
The blocked transaction retains its retry suppression; ordinary reload cannot
reset it. Operators must use the existing explicit drain/exit-proof route.
Preserved runs with unknown external transport identity also defer release.
An indeterminate once-subscription deletion with no surviving confirmed receipt
keeps A; a missing entry alone is not durable proof.

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
launcher retains the owned generation (B or recovered A) rather than risking
cutting newly admitted work for rollback or overwriting a visible terminal state.
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
