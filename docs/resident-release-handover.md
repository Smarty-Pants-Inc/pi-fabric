# Resident release negotiation (#2615)

A live Main chooses the release it loaded. The existing detached launcher may
own a staged irreversible `releaseChange` handover; the supported cooperative
Main stop-then-start path instead uses the existing in-process drain endpoint
and ordinary lease-fenced `ensureHost`. This is not a new daemon, fleet scanner
or installer migration.

## Current staged policy: defer before A exits

Staged `releaseChange` handover and its fallback are **disabled**, including on
protocol-capable Linux hosts. The launcher samples ancestry but has no
attempt-owned membership boundary or complete exit receipts, so it cannot safely
promise A recovery after failed B or Main loss during staged B startup. A protocol
ABI and a free `host.lock` are not sufficient recovery capability. This does not
disable the separate Main stop-then-start path below: it requires exact A exit
proof before ordinary B launch and makes no staged A-fallback promise.

For staged `releaseChange`, `assertAutomaticReleaseRecovery()` is an
unconditional, non-configurable gate:

- Under staged `releaseChange`, A records the authorized target intent and
  synchronously cancels it **before pausing its backlog, yielding to the
  launcher, accepting custody or exiting**.
  The original A owner, admitted runs, actor identities and acknowledged queued
  inputs continue serving. Main shutdown/reload cannot turn deferral into outage.
- The launcher independently cancels a staged custody request under its
  transaction lock, before the positive custody CAS/receipt, target-attempt
  suppression or B spawn. A observes that cancellation and resumes a reversible
  prepare.
- There is no environment/configuration opt-in. Tests of the retained staged
  transaction primitives explicitly mock the recovery assertion; these are not
  evidence that staged release following is enabled or accepted.

This replaces round 2's post-B `blocked` scope cut for staged B attempts. We do
**not** accept service loss on healthy storage with usable A, claim complete
descendant cleanup, or claim the old failed-B recovery matrix applies. B failure
and Main loss *after B spawn* remain unreachable on the staged path because no
staged B attempt is authorized.

## Supported Main graceful stop-then-start

When a Main starts on B and sees a live, compatible resident A for the same root,
it uses the existing `releaseDrain(exit:true)` endpoint. The Main retries only
correlated, read-only precommit busy refusals, leaving A serving while accepted
work finishes naturally; missing or uncorrelated evidence and postcommit
uncertainty are not retried. After the exact durable
`phase:"exited", outcome:"exited"` receipt confirms checkpoint read-back with
admission held, the host has written `reason:"stopped"` for A's exact generation,
A's process birth is gone, and A's host lease is released, Main uses ordinary
`ensureHost` on B and checks that B's release is serving. Queues and traces are
retained. No actor terminal stop, external signal, new state or protocol, or
staged rollback is involved.

This covers Main-initiated switches, with this Main's ordinary file commands
queued behind reconciliation. An independent CLI/proxy writer racing the
committed drain can leave A held with `needs-operator`; Main diagnoses it with
an explicit operator recovery command. It does not retry or launch a competitor. Queues and traces remain retained. This independent-writer case is
outside the automatic path.

Old residents lacking cooperative drain remain diagnosed and serving; Main does
not force-stop them. The [`fabric-resident-drain` CLI](resident-drain.md) default
mode still holds/checkpoints then resumes A with `containment-unavailable`; its
`--exit` success proves only A's stopped exit, not a B launch or serving release.

For staged launcher cleanup, shutdown tracks its directly spawned child
through native process exit.
A missing Linux birth observation cannot suppress native TERM/KILL, and inherited
pipes cannot substitute for an exit receipt. Repeated shutdown signals share one
cleanup pass; the finalizer joins observed cleanup even after the direct child
exits. TERM and KILL each have a five-second wait. This hardening applies on
Windows too, where descendant sampling is unavailable. Native-child receipts
prove only that child exited. They do not establish attempt membership, prove
helper exit or authorize fallback. Detached/reparented helpers and supervisor
crash containment remain part of #3103's boundary work.

## Retained protocol primitives

1. Each Main runtime publishes a birth-bound, nonce-qualified intent. Native
   `/reload` activation reconciles an already-owned root, including while an
   older transaction is staged; it never cold-starts an empty root merely to
   follow a release. Disposal revokes the old nonce.
2. A and B pin real release paths, the whole package-local JS closure, resolved
   runtime/binaries and configuration snapshots. The initiating Main resolves
   its generic JavaScript runtime via `resolveScriptRuntime` before constructing
   B's spec. A bundled Pi executable is never substituted as a script interpreter.
   Cold host attestation uses the birth/kernel-checked parent launcher runtime;
   successors use their custody-pinned runtime. Live model/kernel overlays belong
   to A; storage topology must remain identical.
3. The staged transaction has reversible file/control/lifecycle and both-scope
   actor gates. Runs, continuations and finalizers finish naturally. Queues and
   registry checkpoint before cursors; result/delivery/ACK publication must have
   checked receipts. A terminal UI status never proves worker exit. Delivered
   lifecycle cursor/once-delete obligations repair storage receipts, not accepted
   operation execution. Untouched backlog keeps its retry budget.
4. The retained custody mechanism binds the exact child PID/birth/token and plan
   under the root lock; cancellation/custody share one immutable hard-link CAS.
   It cannot override the current unconditional recovery gate.
5. Staged startup probes require the real worker's nonce-bound extension ACK and
   correlated Pi RPC response, without inference. Terminal publication uncertainty
   retains the owned generation and does not cut possibly admitted business
   work. `host.reloaded` publication uncertainty never ungates or replays business
   work. None of these primitives supplies containment or enables a staged B attempt.

## External transport scope cut (security round 2 F1)

The tmux/screen adapters still conflate a failed CLI/socket query with an absent
session. They have no checked exit contract. Release quiescence does not treat
`false` as a receipt: it rejects any tracked or unregistered tmux/screen
transport **without issuing a liveness query**. Thus a failed or hung query cannot
permit custody or defeat the reversible drain bound.

Close, explicit cleanup, per-manager collection and resident/orphan/nested-tree
retention preserve their worker directories. Requested stops do not prove exit;
automatic relaunch is disabled for these adapters too. This intentionally retains
completed external-pane runs as well until a checked exit contract exists.
Operators must confirm external workers are gone before manual file removal.
Other transport liveness calls during release/exit observation are raced against
an explicit deadline; a failed/hung observation records an unresolved worker and
vetoes custody. No destructive close is used as release proof.

## Frozen absence and assumptions

Missing optional binaries, including absolute paths, become a stable absent
sentinel inside the sealed package JS closure. Reconstructing a pinned spec is
idempotent. Installing a binary at the original missing absolute path cannot
change the selected runner; creating a sentinel JS file invalidates the closure
hash. External binary/dependency immutability remains an assumption, not binary
content authentication by the package JS digest.

The reversible drain bound remains 120 seconds; transport observations cannot
extend it. Retained staged startup/observed-process cleanup bounds are 30 seconds
and 5 seconds TERM + 5 seconds KILL; observed cleanup is never complete membership
proof or fallback authorization. Storage/identity/publication uncertainty remains
fail-closed. No retry-reset interface is added.

## Follow-up issues for the repository owner (not filed by this task)

**Complete staged release handover and A-fallback recovery (#2615 follow-up).**
Establish membership before any child can escape (for example, a delegated
cgroup), retain a supervisor until every member has a checked exit receipt, then
prove one coherent A fallback after failed staged B startup and Main death, with
the same actor IDs, acknowledged queued IDs and no overlapping service. Remove
the unconditional gate only with independent security review and real native-Pi
published A/B acceptance. Until then staged B attempts remain disabled; where
cooperative stop-then-start is unavailable, A remains stale-but-served.

**Add checked tmux/screen worker exit receipts.** Distinguish definitive absence
from CLI/socket failure; bound and cancel every query; persist enough identity
for safe retention after manager death. Re-enable release, collection and retry
only after failed-query/hung-query/live-terminal-pane regressions prove safety.

Installed B71 still requires installer drain for initial capability rollout. This
change does not authorize that install, fleet activation or a claim that #2615's
published-pair/post-install audit is complete. Required Ubuntu/Windows CI,
Astra review, independent security pass and owner hold/queue gates remain.
