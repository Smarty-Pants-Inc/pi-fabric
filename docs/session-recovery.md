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

The run ID is the stable completion key. Mesh CAS claims arbitrate competing
Mains, and durable shared receipts suppress replay to subsequent successors.
Inbox delivery is receipted after Pi's session entries contain the completion
carrier, not merely after an in-memory steer is accepted. Explicit successful
wait/status consumption also receipts the outcome. Legacy resident metadata
receipts remain authoritative. Inbox receipts require the matching session JSONL
file/header and successful file and namespace durability barriers; hosts without
a verifiable persisted carrier retain the pending outcome; send admission
cannot be receipted.

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

## Compaction retry and mesh alarm

Every Fabric-loaded Pi session, including a leaf task agent that has never called
a Fabric tool, observes `session_compact_failed`. A genuine terminal failure
schedules **one** public `ctx.compact` retry with the original custom instructions.
It runs only after Pi is idle: native `compact` aborts active operations, so Fabric
never awaits or dispatches the retry inside the emitting failure hook. Owner
aborts, extension vetoes, and exact no-op outcomes do not retry or alarm. Shutdown
and session replacement cancel deferred work.

A second failure emits a mesh event with topic and kind
`fabric.session.compaction_failed`. Data includes `session`, `windowPercent`
(nullable when unknown), `reason`, `error`, `firstError`, and `attempts: 2`.
The producer identity identifies Main or task agent. Fabric initializes its mesh
only on actual failure if not already active; registration/idle remain lazy.

Pi 0.87.1 exposes the necessary failure event and public retry API: no Pi-side
change is required for this retry/alarm. The retry is a manual compaction, not a
resumption of an interrupted automatic inference turn. Pi's internal transient
summarization retry policy remains independent; Fabric retries one *terminal
compaction operation*, not each provider stream error. Idle-window alarms and
mechanical Main rotation are separate owner work, not implemented here.
