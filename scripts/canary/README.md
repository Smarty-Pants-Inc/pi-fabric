# Frozen-v2 canary plans and recorded evidence

These are plain Node ESM tools with **no production imports**. Run from the package root with Node 24+. They do not install dependencies, start processes, control units, load credentials, connect to brokers, write evidence files, or delete/purge anything. The CLI only reads explicitly supplied JSON and writes JSON to stdout. All examples below are **probe inputs**, not live receipts or E2E evidence.

Binding references: frozen contract §§0, 3.4, 6, 8.3, 10.1–10.3, 11, the latest `CONTRACT-v2-ERRATA.md` (E1–E3, E5–E10), and **lead-accepted `CONTRACT-v2-E4.md`**. E4 supersedes the older proposed/pending labels and frozen completion-transport text. Errata wins: **WA=ryzen2, WB=ryzen4; dedicated messaging server=intel1, v2.11.17**, `sync_interval: always`, R1 **non-HA**. Intel 1 is not an agent host. The event bridge stays up; no events-port or fleet-cutover claim is made.

**E4 scope:** each host's headless Main spawns process tasks on its **own host**. F1/F2/F4 cross-host flows are messaging only, not remote spawn. F3 (parent offline at local child exit, T-B5) runs on **both WA and WB**, under both file and NATS modes. Completions use the producer and exact parent's shared **host mesh root**, unchanged in both modes: there is no completion control operation, `deliverCompletion` sink, carrier envelope, PubAck/owner ACK, transport retry/deadline, `control-seen` claim or transport rollback. `completion:<runId>` is reserved for the unbuilt F-XSPAWN follow-on, not a canary dedupe key. Native dedupe remains the run id and its receipt fence.

## CLI

```sh
node scripts/canary/p95.mjs                         # plan, all four N values
node scripts/canary/p95.mjs --dry-run --n 1,4
node scripts/canary/p95.mjs --input p95.json         # recorded analysis, NOT live execution
node scripts/canary/p95.mjs --input p95.json --n 16  # subset, not full rising-N acceptance
node scripts/canary/lock-attribution.mjs            # collection plan only
node scripts/canary/lock-attribution.mjs --input locks.json
node scripts/canary/rollback.mjs --host ryzen2 --runtimes session:a,runtime:b
node scripts/canary/rollback.mjs --replay rollback-calls.json
```

Each supports `--help`. Exit codes: **0** plan or successful recorded evaluation; **1** failed/blocked gate or unresolved rollback; **2** malformed data/usage. Exit 0 is **not acceptance**. P95 reports `metricVerdict` separately; a numeric pass is `verdict: "probe-only"`, `evidenceKind: "recorded-probe-only"`, `e2eProven: false`, `canaryLatencyClaimEligible: false`. Injected/fake adapters cannot promote these fields to real E2E proof. Rollback replay similarly carries `recorded-probe-only` and never claims E2E proof. Independent live receipt review, useful model work and all B1–B5 fault checks remain required by velocity-lead.

## P95 input: `fabric-canary-p95/v1`

Top-level fields:

- `schema`, `mode: "headless"`, `hosts: {WA:"ryzen2", WB:"ryzen4"}`.
- `server: {host:"intel1", version:"2.11.17", syncInterval:"always", replicas:1}`.
- `preconditions`: keys `P-HL`, `P-SRV`, `P-UP`, `P-ACL`, **each** `{passed:true, receiptId:"stable-evidence-reference"}`. Booleans alone do not pass. The referenced receipts must establish RPC Mains on both hosts, approved host credentials/private CA/server durability, the candidate artifact and host-wide NATS switch/no non-canary work, and ACL reachability. This tool does not authenticate those documents or retrieve secrets.
- `lockEvidence`: the complete lock evidence object described below, **not** a caller-supplied `pass` boolean. T-LOCK missing/failing/incomplete makes the latency gate **blocked**, never weaker.
- `steps`: one object for each requested N in `{1,4,16,64}`. Default requires all four; duplicate steps are invalid.

Each step has `n`, `durationMs:600000`, `ratePerSenderHz:1`, `healthy:true`, `complete:true`, `kvWrites` (nonnegative integer counting successful presence **and** lease writes), `kvWindowMs:600000`, `kvComplete:true`, `senders`, `recipients`, `samples`.

There must be exactly N distinct sending processes, 1–16 of which are real agents; the rest are model-free Fabric control-plane sender processes. One sender entry:

```json
{
  "id": "sender-a", "host": "ryzen2", "kind": "real-agent", "agentKind": "main",
  "runtimeId": "session:a", "senderReceiptId": "probe:sender-ready",
  "process": {"pid": 1001, "pidStartTime": "12345", "bootId": "probe-boot-a"},
  "clockId": "probe:ryzen2:1001:12345:boot-a",
  "windowStartMonoNs": "1000000000", "windowEndMonoNs": "601000000000"
}
```

`kind` is `real-agent` or `model-free`; a real agent's `agentKind` is `main` or `process-task`, never a print root or a surrogate `session:<taskId>`. `runtimeId` is the actual sender runtime. Process identity and clock IDs must be distinct across the N senders. All senders must contribute exactly one command in **each** sender-relative one-second slot for 600 seconds; missing slots block, duplicate slots are invalid. Retries do not count as new latency samples.

Recipients are inventoried independently; a boolean `realRecipient` alone is insufficient:

```json
{
  "id": "main-b", "host": "ryzen4", "targetClass": "main", "nativeKind": "main",
  "runtimeId": "session:b", "receiptId": "probe:recipient-ready",
  "process": {"pid": 2001, "pidStartTime": "23456", "bootId": "probe-boot-b"}
}
```

`targetClass` is `main`, `task` or `actor`; corresponding `nativeKind` is `main`, `process-task` or `actor`. `runtimeId` is the **owner runtime hostId** that emits the ACK. The readiness receipt must join it to the actual native target/process. All samples must cross WA↔WB and use these real recipients. No real sender/recipient inventory means invalid/blocked, not a pass.

One sample (not a complete step):

```json
{
  "commandId": "probe:command:0", "senderId": "sender-a", "clockId": "probe:ryzen2:1001:12345:boot-a",
  "recipientId": "main-b", "ownerHost": "ryzen4", "targetClass": "main", "operation": "followUp",
  "realRecipient": true, "sendMonoNs": "1000000000",
  "pubAckMonoNs": "1020000000", "pubAckSequence": 1, "pubAckDurable": true,
  "ownerAckMonoNs": "1100000000", "outcome": "accepted", "ackAuthenticated": true,
  "ackCommandId": "probe:command:0", "ackSourceMachine": "ryzen4", "ackOwnerHostId": "session:b",
  "b3MonoNs": "1800000000", "b3ReceiptId": "probe:native-consumed:0"
}
```

- Stamp `sendMonoNs` immediately before the **actual sender** calls send/request. Stamp `ownerAckMonoNs` when that same sender receives the authenticated, correlated **owner ACK (B2)**, not command PubAck (B1). Authenticate source machine/owner runtime per P6 before recording. Never subtract remote wall clocks or timestamp a remote sender's RPC in the harness. `senderMonotonicNow()` exports `process.hrtime.bigint().toString()` for an adapter running in the sender process.
- All `*MonoNs` fields are nonnegative decimal strings in that sender's `clockId` domain. PubAck has its own durable sequence and timestamp. It is not required to arrive before the owner ACK at the sender.
- `outcome` is `accepted`, `rejected`, `timeout`, or `uncertain`. Rejections need the real correlated ACK; timeouts/uncertainty have absent/null `ownerAckMonoNs`. Never drop unsuccessful sends from the population. Missing PubAcks block; nonaccepted Main/task outcomes fail. No fallback/new-id retry.
- `b3MonoNs`/`b3ReceiptId` are optional, independent native consumption evidence. The timestamp is the **sender's observation** of that receipt (an upper-bound observation, not remote native-consumption time). Missing B3 is counted, never converted to zero; B3 is separately reported and **not claimed**. A stop may have no applicable B3 receipt. Completion consumption is separate, not a B3 command sample.
- Main/task operations are **`followUp`, `steer`, `stop` only**; coverage of each on both classes is required in every step. `task` means a real **process-task** target. A `completion` operation in `samples` is invalid, not silently excluded. Actor operations are `tell`, `ask`, `stop`; actor statistics (including an explicit empty population) are report-only. Stop is checked within its native target class.

Reports use nearest-rank p50/p95/p99, with `null` for an empty population. Each N reports Main/task/actor owner-ACK distributions, per-operation distributions/outcomes, separate PubAck and B3 distributions, KV writes/s and aggregate PubAck p95. Main/task aggregate **and** per-operation p95 for followUp/steer/stop must be strictly **<5000 ms**, also reporting the ≤15000 ms ceiling. PubAck p95 **≤250 ms**; a miss requires escalation, never changing durability to make it pass. A subset run sets `fullRisingN:false`.

### Separate completion timing (E4; report-only, no transport hop)

Each step may also include `completionSamples` (default `[]`), **outside** `samples`, the N-sender schedule, command coverage and all command/PubAck/B3 populations. One recorded native completion timing:

```json
{
  "runId":"probe:run:wa", "host":"ryzen2", "meshRoot":"/probe/ryzen2/mesh",
  "recipient":{"rootId":"probe:root:wa", "sessionId":"probe:parent:wa"},
  "recorderProcess":{"pid":3001, "pidStartTime":"34567", "bootId":"probe-boot-a"},
  "clockId":"probe:completion-clock:wa",
  "settledClockId":"probe:completion-clock:wa", "deliveredClockId":"probe:completion-clock:wa",
  "settledMonoNs":"1000000000", "deliveredMonoNs":"26000000000",
  "settlementReceiptId":"probe:manager-result:wa", "deliveredReceiptId":"probe:delivered:wa"
}
```

`host` and the absolute `meshRoot` identify the **same-host shared root of producer and parent**, not a remote route. `recipient` is the spawn-bound `{rootId,sessionId}`. Both monotonic stamps must record local **settle → exact-parent inbox `delivered()`** in the named recorder process's clock domain; both clock IDs must equal `clockId`. The receipt references must join those boundaries to the actual run and recipient. Never subtract clocks from different processes/hosts, substitute PubAck/owner ACK or archive time, or fabricate a zero for missing timing. If compatible stamps are unavailable, omit the timing. This evaluator checks the recorded shape, not the authenticity of those referenced receipts.

Duplicate `(host,meshRoot,runId)` timings, mixed clock IDs, missing native receipts, negative durations and completion transport fields (`commandId`, `operation`, carrier/ACK/`controlSeen` fields) are invalid. Per-step `completion` reports aggregate and `byHost` p50/p95/p99, `hostsMissing`, **`claimed:false, transportHop:false`**. Empty/missing populations remain explicit (`count:0`, percentiles `null`). Even a latency above 15 seconds does **not** affect the control metric gate; it is reported for independent review, never a transport claim. Top-level `completionClaimed:false` and all recorded-only restrictions remain. These timings do not prove F3/T-B4/T-B5.

E4's B4 exit evidence is the existing read-only persisted-exit check `runTreeResourceVeto(runDir, 0, undefined, true, true) === undefined`, not a new completion field or a settlement timestamp. A final-answer completion may precede native exit; permits stay fenced until exit is proven. F3/T-B5 live setup on each host includes a durable-residency process task and a session-residency process task; stop the parent before the durable task's final answer, with a SIGKILL variant. Live review must verify the admission-manifest recipient, one durable envelope while the parent is offline, restart with the same mesh root, rootId and sessionId, exact-once intake, real status/final-answer receipt, durable `delivered()` receipt **before** archive/claim retirement, second-restart nonreplay and wrong-parent exclusion, plus resident-host-after-save and parent/session-task orphan-attempt crash variants. NATS must contain **no** completion envelope or completion `control-seen` claim. The envelopes, attempts, receipts, archives and native `residency/completion-claims` stay unchanged; native completion claims are not `control-seen` claims.

Programmatic boundary: `collectP95(commandAdapter, context, n)` sends structured `p95.collect-step` requests only to the explicitly supplied function. `context` is the top-level object without steps. Precondition receipts and T-LOCK must pass before invocation. Each request includes N, the 600-second schedule, `claimedOperations:["followUp","steer","stop"]`, report-only local completion measurement, real sender/recipient requirements and no-new-id retry. The adapter must verify readiness **before sending**, own the actual sender processes, preserve receipts/ids, and return one step in the schema above. An invalid/failed step stops later steps. Each observation has a finite 12-minute deadline and an AbortSignal; cancellation does not revoke broker custody or prove a send failed. There is deliberately no CLI driver-module loader or implicit live execution path. A recorded fake function can exercise this API, but its output stays probe-only.

## Lock input: `fabric-canary-lock/v1`

Normalize recorded all-acquisition PID counters plus span evidence into:

```json
{
  "schema": "fabric-canary-lock/v1", "ownerInventoryComplete": true,
  "window": {"meshRoot": "/probe/global-mesh", "startAt": 1000, "endAt": 2000},
  "expectedProcesses": [{"machine":"ryzen2", "pid":1001, "pidStartTime":"12345", "bootId":"probe-boot-a"}],
  "records": [{
    "machine":"ryzen2", "pid":1001, "pidStartTime":"12345", "bootId":"probe-boot-a",
    "meshRoot":"/probe/global-mesh", "startAt":1000, "endAt":2000,
    "counterKind":"all-acquisitions", "complete":true, "appendErrors":0,
    "globalAcquisitions":0, "controlAcquisitions":0, "otherAcquisitions":0,
    "spans":[{
      "spanId":"delivery:1", "commandId":"probe:command:0", "targetClass":"main",
      "workload":"conformance", "startAt":1100, "endAt":1200,
      "deliveryThroughBrokerAck":true, "globalAcquisitions":0
    }]
  }]
}
```

This one-span example is intentionally **blocked**: complete evidence needs coverage of **main, task, stop** in **conformance, faults, canary** (**9** combinations across the records). E4 voids I-LOCK's **completion: `writeJsonAtomic`** control row: native completion settlement/consumption has no delivery→broker-ACK span and is **not** required as a control class. A `completion` control span is invalid; do not relabel native journal work as a control acquisition. Any acquisition outside the control spans must still be accounted for in `otherAcquisitions` (not omitted). Inventory every participating owner, including restarts distinguished by PID/start time/boot id; missing records, incomplete windows, partial delivery→broker-ACK spans or append errors block. The mesh root must be an absolute exact **global** root; owner-local `control-seen` locks do not count.

`globalAcquisitions = controlAcquisitions + otherAcquisitions`; attributed control spans must sum exactly to `controlAcquisitions`. **Any positive control acquisition fails T-LOCK**, even if waits were zero or latency happened to pass. Explicitly attributed unrelated activity is reported, not falsely blamed on handlers. Unclassified acquisitions cannot be omitted. These are normalized **window deltas**, not lifetime totals.

`scripts/benchmark-lock-stats.mjs` (lane source supplied by the task) demonstrates all-acquisition accounting and complete-minute PID stats. `scripts/probe-mesh-lock-events.mjs` describes thresholded held/timeout events. Those events alone cannot prove zero acquisitions: this evaluator requires all-acquisition counters and complete span attribution, never infers acquisition counts from waits, and does not run either destructive scratch benchmark. Recorded `pass` means the supplied evidence satisfies the zero gate; `e2eProven` remains false.

## Rollback plan and recorded adapter

`rollbackPlan({host,runtimeIds})` prints the §8.3 sequence. `runRollback(request, commandAdapter, {dryRun:true})` does **not call** the adapter. Only an explicit programmatic `{dryRun:false}` enables structured adapter calls; this CLI has **no live mode**. Adapter implementations are trusted code, not model-generated shell. They must implement only the named semantics, never deletion/purge/cleanup ACKs, honor AbortSignal/finite process deadlines and retain uncertain operation receipts.

Replay envelope:

```json
{
  "schema":"fabric-canary-rollback-calls/v1",
  "request":{"host":"ryzen2", "runtimeIds":["session:a"]},
  "calls":[
    {"action":{"kind":"pause-admission", "host":"ryzen2", "runtimeIds":["session:a"]},
     "result":{"paused":true, "spawnsRefused":true, "receiversDraining":true,
       "code":"FABRIC_TRANSPORT_DRAINING", "notRun":true, "persisted":false,
       "runtimeIds":["session:a"], "hostInventoryComplete":true}}
  ]
}
```

This abbreviated transcript deliberately blocks at the missing drain receipt. Every `calls[].action` must exactly match the generated request; extra/unconsumed calls on successful replay are invalid. Every action includes `host` and the full `runtimeIds`. Additional payloads and required receipt fields:

| Action | Additional request payload | Required result |
|---|---|---|
| `pause-admission` | none | As above: refuse spawns/new sends before persistence, but keep recipients draining; prove complete host inventory. |
| `drain` | `waitLimitMs:120000` | `elapsedMs` in 0..120000 and `states:[{runtimeId,pending,ackPending}]` for **every** runtime. All zero may return early; nonzero requires the full logical 120000-ms drain deadline. Adapter owns bounded waiting, not an unbounded polling loop. |
| `classify-retained` | `states` from drain | `complete:true, pausedInventoryStable:true, controlSeenStoreUnchanged:true, messages:[...]`. Inventory count per runtime must equal pending+ackPending, including ACK envelopes; if it cannot be joined consistently, stop and obtain a fresh verified inventory. No MSG.GET result is acked. |
| `relay-outcome-unknown` | `disposition`, `replyTo` | `id:"relay:<commandId>", persisted:true, sequence:positiveInteger`. Once per unique remaining command, not per duplicate envelope. |
| `flip-to-file` | `transport:"file", reloadAll:true, retainBridge:true` | `hostTransport:"file", fileRuntimeIds:[all ids], oldNatsWritersStopped:true, bridgeUp:true, filePresenceMirrored:true, eventTopicsUnaffected:true, fileAdmissionReady:true`. File admission may become ready **only after** old NATS writers are confirmed stopped. |
| `reconcile-command-ids` | `commands` from classification, `preserveControlSeen:true, maxSameKeyRetries:1, retainCompletions:true` | `controlSeenStoreUnchanged:true, completionJournalRetained:true, completionJournalUnchanged:true, results:[one per commandId]`. No `completionResendCarrier` field. |
| `retain-audit` | none | `streamsKept:true, consumersKept:true, fabricAdvKept:true, controlSeenKept:true, nativeCustodyKept:true, deletedOrPurged:false`. |

A retained message has `runtimeId`, positive `sequence`, and `kind:"command"|"ack"`. **Only control/ACK carrier envelopes belong here:** a completion operation or reserved `completion:<runId>` command key blocks classification before relay/flip. Native completion journals never enter this inventory or `control-seen`, and are never relayed, carrier-replayed or reconciled as transport commands. `retainCompletions:true` and `completionJournalUnchanged:true` mean rollback does not replace, migrate or carrier-replay the native same-root journal; a legacy `completionResendCarrier` receipt is rejected. Normal native settlement, restart drain, receipt writing, archival and claim retirement continue unchanged—this is not a freeze of journal contents. Commands additionally have `commandId`, `replyTo`, `claim:"absent"|"outcome"|"pending"`. `outcome` is present only with `claim:"outcome"`; optional `terminal:true` marks a terminal stored outcome. Classification is typed as **queued / admitted / unknown / terminal**. ACK envelopes are **retained-for-audit**, not turned into command relays. Duplicate command envelopes must agree on replyTo/classification/outcome.

Each command gets a typed disposition with its classification, `relayId:"relay:<commandId>"`, `code:"FABRIC_RELAY_DESTINATION_UNAVAILABLE"`, `outcome:"outcome-unknown"`, `retry:"same-key-once-after-flip"`. Even an admitted/terminal command remaining in the inbox receives that relay; the authoritative stored outcome is returned on reconciliation. No `notRun:true` assertion is invented for an uncertain effect.

Reconciliation requests carry `{commandId,replyTo,classification,storedOutcome?}`. Each result carries `commandId`, `sameKeyRetries` (0 or 1), `commandIdUnchanged:true`, `claimPreserved:true`, `effectCount` (0, 1, or null only for unresolved holds), and `disposition`:

- **admitted/terminal → `stored-outcome`**: `outcome` must exactly match `storedOutcome`; `handlerReexecuted:false`, nonnull `effectCount`, and `receiptId` required.
- **unknown → `held-unknown`**: no handler reexecution or retry while the claim lacks an outcome; retain for independent operator resolution. Never replace the claim to obtain a clean run.
- **queued → `admitted-once`**: one same-key retry, one effect, receipt, `handlerReexecuted:false`; or **`held-queued`** with zero retries/effects and `handlerReexecuted:false` for explicit nonexecution/continued hold.

Unresolved holds return `needs-operator` (exit 1), not a successful rollback proof. The function keeps a partial ordered audit on any failed/uncertain step and stops before subsequent actions. Never blindly replay a live partial rollback: the operator must inspect effects of the uncertain action first. Pause lasts through drain/classification; the compound flip adapter must confirm old NATS writers stopped **before** making file admission ready for reconciliation retries. Re-enabling NATS is a separate operator-controlled rollout, never an automatic recovery action. Streams, consumers, FABRIC_ADV, carrier-independent control-seen and all Main/task/actor native custody and consumption receipts remain intact. The same-root completion envelopes, attempts, receipts, archives and native completion claims are retained unchanged in both modes: E4 has **no completion transport rollback**.

## Verification boundary

These files, including the E4 amendment, were authored under a read/edit/write-only restriction: **no commands, tests, broker work or build were run**. `scripts/canary/canary-tools.test.mjs` contains recorded-tool tests (keep reason: evidence trust boundary and rollback data safety), not production-transport or E2E tests. It covers the amended collection/analysis boundary; separate, unclaimed completion timing; rejected completion control/lock/rollback records; nine-combination T-LOCK coverage; and full exact recorded rollback replays with queued/admitted/terminal/unknown dispositions, duplicate control envelopes, retained ACKs and unchanged completion journals on either host. Existing clock/provenance, drain, same-key, no-double-effect, old-writer, audit-retention and recorded-only guardrails remain.

Intended checks, **only when authorized**, are `node --test scripts/canary/canary-tools.test.mjs`, the three `--help`/plan commands, targeted recorded happy/negative probes, `node --check` on the three tool `.mjs` files, and the repository-required fresh `bun run build`. None is claimed green here. No production file, native journal implementation or hypothetical production transport driver is changed/added.
