# Session recovery

## Dead Main completion delivery

Fabric retains terminal child outcomes under the shared mesh root in
`agent-completions/`. Workers receive the original Main's host-created return
address before launch, so ordinary detached tasks and foreground handoff results
can finish after the Main process disappears. Resident hosts also retain outcomes;
new Mains import authenticated resident delivery envelopes written by B72.

A pending result goes to the original Main while it is live. Otherwise, only a
newer live **native** Main with the same canonical runtime project root, execution
cwd, participant name, and fleet role is a successor. Reload leases count as live;
non-interactive roots and mesh-bridge mirrors cannot adopt results. If several
successors exist, the newest wins (root ID breaks timestamp ties). An already-live
claimant retains its unconsumed admission; a dead claimant can be replaced.

Consumed envelopes are moved, without rewriting their bodies, to
`agent-completions/archive/<sha256(run-id)>.json` only after their shared
consumption receipt is durably written. Receipts remain in `receipts/` as replay
fences. A crash between the receipt and rename leaves a live envelope; the next
owner drain validates the receipt, skips delivery, and retries the rename.
Concurrent renames are idempotent: a losing `ENOENT` means another consumer
already moved the source. Unreceipted envelopes remain live.

Idle discovery lists only the live directory and `attempts/`, never `archive/`
or the historical receipt directory. It does not fsync or rewrite existing
receipts during drain. Explicit receipt-write retries still confirm durability
before archiving. Archived bodies remain available to targeted status/wait by
run ID after restart, with the same exact root/session access checks. Archive
retention is separate from live discovery; this change does not delete history
or transfer claim-cleanup authority to another Main.

The run ID is the stable completion key. Mesh CAS claims arbitrate competing
Mains, and durable shared receipts suppress replay to subsequent successors.
Inbox delivery is receipted after Pi's session entries contain the completion
carrier, not merely after an in-memory steer is accepted. Explicit successful
wait/status consumption also receipts the outcome. Legacy resident metadata
receipts remain authoritative. Inbox receipts require the matching session JSONL
file/header and successful file and namespace durability barriers; hosts without
a verifiable persisted carrier retain the pending outcome and do not receipt
send admission.

Without a successor, results remain pending. `agents.list` includes pending
outcomes and `agents.status` exposes
`completionDelivery: { status: "undelivered", addressedTo }`. Recovered inbox
summaries are marked `re-delivered from dead Main session <id>`; full results
remain readable with `agents.wait`/`agents.status` after notification. Reading
another lane's pending status does not acknowledge it. Completion recovery
never grants cleanup or mutation authority over the predecessor's runs.

This requires shared persistent mesh storage. Pre-upgrade ordinary workers have
no launch return-address manifest; the legacy import path covers resident
completions, not arbitrary old temporary worker directories. It does not restart
work, make session-scoped tasks survive graceful owner cancellation, or rotate
Mains. A storage-write failure retains the worker status and reports a warning.

## Native compaction recovery and best-effort mesh alarm

Every Fabric-loaded Pi session, including a leaf task agent that has never called
a Fabric tool, observes `session_compact_failed`. Native Pi owns two transient
summary retries shared across the summaries and one coalesced fallback operation.
Fabric does not call `ctx.compact`, schedule another compaction, or signal retry
completion/idle. Pi's terminal session error is visible through its native path,
independently of any Fabric event.

By default, `compaction.primaryTimeoutMs` is `300000` ms (5 minutes) and
`compaction.fallbackTimeoutMs` is `240000` ms (4 minutes), for a nine-minute
combined timeout budget. Both settings accept positive milliseconds.
`compaction.fallbackModel` defaults to
`cliproxyapi-anthropic/claude-opus-5-5`; `compaction.maxSummaryTokens` defaults to
`8000`. A summary-token cap is not a latency guarantee or a promise of exact
provider tokenization.

For a genuine non-benign terminal failure, Fabric attempts the best-effort mesh
event with topic and kind `fabric.session.compaction_failed`. Its data is
`{ session, windowPercent, reason, error, fabricAttempts: 0 }`; `windowPercent`
is null when unavailable and `error` is a detached preview capped at 4000
characters. There is no `firstError` or `attempts` field. The event may be absent
when mesh/runtime/configuration is unavailable or publication fails; callback
resolution is not a subscriber receipt or wake proof. This optional publication
is separate from Pi's native terminal session error.

Aborted events/signals, disabled handling, stale contexts, and known no-op
outcomes (`Already compacted`, `Nothing to compact (session too small)`, and
`Compaction cancelled`) are not reported. Repeated failures are deduplicated per
session while the incident context remains live; a replacement context may
report after the old one is invalidated. Successful `session_compact`, session
start, and shutdown reset the incident latch.
