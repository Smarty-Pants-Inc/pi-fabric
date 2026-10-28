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

## Portable interrupted-turn recovery building blocks (L7 B3)

These building blocks are **not an installed automatic startup path**. Provider
registration does not activate them, and they do not qualify cross-host recovery
or the 120-second target. Ordinary local startup and its predecessor-process
proof remain unchanged.

`src/recovery/l7-interfaces.ts` defines the B2 identity, B4 effect and B5 input
ports. Extensions can publish their independently owned providers during factory
registration with the public `registerSessionRecoveryAdapters(pi, parts)` export.
Discovery uses Pi's existing in-process event bus; it does not create another
journal, authority or scheduler. An absent composition is inert. Partial or
conflicting compositions are explicit errors, not invented no-op implementations.

A host startup transaction can call `prepareSessionRecovery` from
`src/recovery/coordinator.ts` while ordinary native input, model work and Fabric
inbox drains are stopped:

1. B2 validates the exact retained session file, REQUIRED state roots, session-scoped
   transport identity, predecessor receipt bytes/hash and native old-writer exclusion.
   A different boot or an absent discovery row alone is not writer exclusion.
2. The caller's `activateSuccessor` callback must acquire the actual owner lease.
   `FabricState.setSessionRecovery` carries the validated activation to the runtime
   **before bootstrap**; `ensure` awaits NATS lease/view activation, and
   `assertSessionRecoveryActive` checks that ownership still holds before admission.
3. B5 supplies complete accepted-input, settlement and durable explicit-cancel history.
   A different-boot open input with known no-cancel history is interrupted; clean,
   completed, cancelled and same-boot inputs are not automatically continued. Missing,
   corrupt or unknown history is uncertainty. Classification gates continuation, not
   settlement: before returning for any non-interrupted classification, the coordinator
   still calls B4's existing `settlePendingIntents` port for already-issued intents. B4
   records committed/not-committed/uncertain outcomes without restarting unissued slots;
   uncertainty rejects rather than returning silent success. These settlement-only paths
   do not read/materialize tool results, install provider context, create a continuation
   marker or call B5 admission. An existing marker's generation is reused and its result
   receipts/admission are untouched. Unconfigured ordinary startup remains inert.
4. B4 settles saved intents by stable effect keys and resolves exact original native
   results. `readToolResult(effect)` returns `{ message, source: 'native-entry' |
   'adapter-original', postHook: boolean }`. Prefer `native-entry` with `postHook: true`;
   other native-entry flags block. For `adapter-original`, the caller must supply
   `SessionRecoveryOptions.transformToolResult(message, effect)`, the same current live
   strict `tool_result` transformation path, with original call/input resolution. It is
   always invoked, even if the adapter claims `postHook: true`. Missing callbacks and
   thrown errors block without raw fallback; errors propagate unchanged. This coordinator
   does not implement a hook runner. Ordinary Pi observer dispatch that swallows handler
   errors does not satisfy this strict port. The callback may not change call ID, tool
   name or timestamp, or turn an original error into success. Raw messages are detached
   before callbacks, and materialized results are detached and deeply frozen afterward.

   Duplicate `(toolCallId, key)` rows block, but consistent same-key physical aliases are
   valid. Same-key aliases must agree on outcome and every field of their materialized
   messages except **only the top-level `toolCallId` and `timestamp`**, which correlate
   physical aliases/attempts rather than the logical effect result. Content, tool name,
   error flags, details, nested IDs/timestamps and every other field are still checked.
   Full per-alias hashes therefore legitimately differ: each full message includes its
   exact alias ID **and original native timestamp**. Strict transformation must still
   preserve both; neither field is removed from a materialized message or its hash.
   Every alias is read/checked, including
   aliases not visible to the provider. Only exact original assistant call IDs present in
   `context.sessionManager.buildSessionProjection().messages` become provider results;
   this is Pi's active-branch, compaction-aware projection, not all session entries.
   No provider suffix normalization (for example `call|fc_item` to `call`) is performed.
   An outer `fabric_exec` result may cover multiple distinct settled nested keys, but
   invisible nested aliases never become invented results or revive an unsettled outer
   assistant. The existing `repairRecoveryContext` checks exact call/tool-name binding
   and whole-assistant settlement before any receipt is written. The interrupted
   JavaScript program is never replayed.
5. A durable continuation marker retains the original input, recovery generation and
   exact continuation text before B5 admission. Retrying after enqueue without a caller
   receipt uses that same generation and admission. Changed/corrupt markers or a changed
   admission ID are errors. Its retained format-1 recovery receipt now also contains
   `resultReceipts: Array<{ toolCallId, source, resultBytesHash }>` as a monotonic retained
   union of materialized calls, sorted lexicographically by exact ID. A continuation can
   progress, issue a distinct new call and crash again: its newly settled/materialized
   call is added to this same marker without changing the original input, generation,
   continuation text or admission ID. The expanded union is durable (file/directory
   barriers) **before** context installation or the next B5 admission, and checked again
   before admission. Any previously pinned call returned by B4 must reproduce the retained
   hash **and source** exactly, even if it is no longer in the active provider view;
   changed provenance blocks without overwriting any pin or persisting partial additions.
   Historical receipts are never erased merely because compaction, branching, settlement
   or the provider view no longer returns their calls. Retaining a receipt does **not**
   resurrect that call into provider context: only current exact native calls materialize.
   Existing format-1 markers without this field are upgraded additively and durably after
   successful settlement, before admission. This uses the existing coordinator receipt,
   not another ledger or B6's native carrier-commit evidence.

   The exact B3/B4 result hash serialization is SHA-256, lower-case hex, of the UTF-8
   bytes from JavaScript `JSON.stringify(materializedMessage)` with no replacer, spacing,
   key sorting or trailing newline. It covers the complete native `role: 'toolResult'`
   message, including call ID, timestamp, error flag and all serialized optional fields,
   **not** the read envelope, store entry framing, or just text content. Materialization
   snapshots that JSON as a detached plain message and hashes the same representation
   used in context. For `native-entry`, B3 compares any supplied B4 `resultBytesHash`
   (required for committed effects) to that exact hash. B4 must conform to this native
   message serialization; arbitrary store-record bytes are a different object. For
   `adapter-original`, the B4 pre-hook hash is not compared with post-hook bytes; B3
   records its own post-hook hash plus source instead. B6 still returns its exact stored
   bytes without reserialization (see below).

   Cancellation is reread immediately before admission; B5 must serialize the final
   cancellation/admission race.
6. The synchronous pre-admission callback installs and validates the provider view with
   `repairRecoveryContext`. It replaces aborted/synthetic results with retained truthful
   native results, preserves original IDs/arguments, and suppresses duplicates without
   rewriting raw session history. Interrupted assistant projections carry only the
   original complete tool calls, not partial text or provider reasoning/signatures.
   Every call in a revived aborted/error assistant must have a settled result. Missing
   original calls, conflicting results or incomplete interrupted assistants are explicit
   uncertainty, not fabricated success.

The optional NATS `startupSuccessor.verifyPredecessor(snapshot, signal)` receives
both the frozen parsed predecessor receipt and its exact original UTF-8 bytes under
receipt custody, including explicit absence. The caller supplies B2 validation.
Acquisition waits only for the exact predecessor and requires an authoritative KV
read showing actual absence; a watch event or elapsed TTL is not permission to
replace an entry. One total TTL plus safety budget is 17 seconds. Admission remains
create-only, and the successor receipt is reserved durably before the put/PubAck.
Foreign ownership, renewal, receipt replacement, timeout or competing acquisition
refuses that single attempt. The ordinary five-second composition timeout remains
unchanged when the portable verifier is absent.

### Native integration still required

B5 `admitContinuation` is a **durable storage-only obligation** while native input is
fenced. Its receipt is not a native carrier commit. B5 owns subsequent native drain
and restart lookup, with B6's post-append durable receipt; B3 must not call
`sendMessage` or replay a whole tool program as a substitute.

`RecoveryDurabilityAdapter` in `l7-interfaces.ts` is B6's bounded post-append port
for `control-seen` and native `carrier` records. It preserves the original absolute
`deadlineMs` and optional abort signal. A successful receipt means the record is
**written and fsynced** (or the store's equivalent commit), never merely enqueued.
Repeated `(sessionId, deliveryId, recordKind)` requests return the same sequence
without appending twice. The closed failure reasons are `deadline`, `aborted`,
`store-unavailable`, `superseded` and `not-found`. This defines the provider contract;
it does not implement B6 storage or turn a pre-append `message_end` into a commit.
Beside the barrier, `readDurableEntry({ sessionId, deliveryId, recordKind })` returns
`{ found: true, seq: number, bytes: Uint8Array }` or `{ found: false, reason:
'not-found' | 'store-unavailable' | 'superseded' }`. `bytes` are the exact retained
UTF-8 serialized native/store bytes after hooks, not parsed/reserialized JSON. There
are no deadline/signal or other new arguments on this companion read. This type-only
port does not implement a store reader or establish native post-hook durability.

The inspected Pi 1.2.0 runtime does not expose an atomic, fail-closed startup
recovery transaction/fence handoff. Cold startup has no `session_ready`; replacement
`session_start` is already fenced, and `ctx.fenceInputs()` cannot adopt that fence.
`session_ready` is notification-only and is not awaited. Attempting keyed native
submission while fenced can cache a rejected request key, so retrying it after
release is not a drain protocol. Ordinary observer errors are swallowed, making a
thrown startup/context handler insufficient as an admission veto. Durable explicit
cancel-before-apply across RPC, TUI and extension paths also requires its native
owner's seam.

Consequently no automatic extension startup/context hook is installed here. The
host/B5 integration must supply that transaction, durable cancel history and the B6
carrier receipt before dispatch. B2 must also address a foreign crash-held receipt
custody lock with validated native exclusion; this implementation does not force
reclaim such a lock from a new boot assertion. Unit tests use explicit adapters and
broker fixtures; actual provider conversion is exercised, but neither those tests
nor the pure coordinator prove native kill/restart, physical-host exclusion or
zero lost/duplicate effects. Those remain integration qualification work.
