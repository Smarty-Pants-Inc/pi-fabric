# NATS Path B switch and FILE rollback — operator DRAFT

**GATED DRAFT, not rollout approval. No live-fleet cutover has been made by this work.**
Do not use this page to authorize a fleet restart, install, broker change or cutover.
R1 must precede R2; independent acceptance and an approved operator window are still required.
There is no new product producer fence, drain-only boot mode or receipt-issuing CLI here.
The STOP-ALL procedure below is a checklist of required observables, **not runnable fleet commands**.
A deployment owner must supply and verify its existing stop/start controls and no-relaunch mechanism.

**Known RED: `PRODUCT-CLOSE-EPIPE-1` (`smarty-dev#14612`).** The retained R1 rerun reports
reconciled business results and a retained archive, but normal final FILE bridge/remote-agent
close failed with `write EPIPE`; the whole command is NOT a clean acceptance PASS.
Focused FILE cases reproduce this before business traffic as well as after followUp/steer.
`MeshBridge.stop` calls `RemoteBridgeSide.close`, which ends/destroys the pipes before the
agent necessarily processes EOF and disables its change-notification writer. Keep the failure;
do not catch/normalize EPIPE, substitute forced termination for graceful success, or rerun blindly.
The first R1 was also RED: export was refused and its full stopped native root was not retained.
Preserve that first failure separately; the later archive does not retroactively repair it.
No completed R2 PASS, independent R1 acceptance or fleet-capacity/bar claim is asserted here.

## Invariants and prerequisites

Path B changes the **control carrier**, not the keyed-state backend or event/schedule storage.
Use separate actual Fabric runtimes and roots: hub SQLite, work FILE, authenticated private TLS
JetStream **2.14.7**, with `sync_interval: always`. Rehearse on sanitized real-fleet copies,
never original capture roots, live roots or a shared production broker.
Within R1, NATS and restarted FILE must use the SAME actual roots and stable runtime host IDs.
A separate FILE-from-start fixture is a useful contrast, not a rollback of those NATS roots.

| Root | NATS effective settings | FILE rollback effective settings |
| --- | --- | --- |
| Hub | `transport=nats`, `hostTransport=nats`, `stateBackend=sqlite` | `transport=file`, `hostTransport=file`, `stateBackend=sqlite` |
| Work | `transport=nats`, `hostTransport=nats`, `stateBackend=file` | `transport=file`, `hostTransport=file`, `stateBackend=file` |

These are `mesh.*` fields. `mesh.hostTransport` is the host stamp in `<agentDir>/fabric.json`;
a project cannot set it. Inventory trusted project overrides of `mesh.transport`/`mesh.stateBackend`.
Read back effective settings, not only the edited file: `PI_FABRIC_MESH_TRANSPORT`,
`PI_FABRIC_MESH_HOST_TRANSPORT` and `PI_FABRIC_MESH_STATE_BACKEND` must be unset or match.
Configuration is activation-captured: editing it does not switch a running process.

Before either direction, the operator records:
- Every actual runtime host ID, machine, root, backend, agent directory, process generation,
  owner/sender role, native inbox stream/durable/filter and retained consumer identity.
- Main, worker and resident runtimes; publishing bridges AND their remote agents; automatic
  launch/recovery paths; required links and exact approved normal stop/start observables.
- The complete per-command manifest: logical/idempotency key, command/message identity,
  actual API outcome, admission, native stages and disposition, including unsuccessful items.
- Coherent private backups under stopped writers or an approved custody/filesystem snapshot.
  Preserve SQLite DB and any required WAL/SHM coherently; do not copy a live DB alone.
- Original journals, `control-seen`, root state, host IDs, bridge cursor, event/archive history,
  and separate `<agentDir>/fabric` native NATS transport/lease state. Retain NATS machine/stream
  configuration for historical checkpoint validation even after selecting FILE.

**Never restore an older snapshot as ordinary rollback.** It can erase accepted work.
Do not purge, delete or recreate streams/consumers, remove seen rows, synthesize presence,
change host IDs, or retry UNKNOWN work with fresh keys to manufacture a passing result.
Missing inventory, missing Consumer Info or an RPC error is **UNKNOWN, not zero**.

## Source rehearsal commands — R1 FIRST, R2 only after its acceptance barrier

These commands select existing source-integration cases; they are not fleet-management tools.
Prerequisites: Linux, dependencies, Node/Pi host environment and a caller-owned isolated test
environment/deletion guard; test-root retirement belongs only to that environment.
Set `PI_FABRIC_NATS_REHEARSAL=1` explicitly; NATS_BIN/PATH discovery alone must not admit the suite.
Capture native command exit, stdout, stderr and descendant terminal evidence securely.
`NATS_SWITCH_PROBE_MODULE` must resolve to the existing thin adapter shown below; otherwise R1/R2
can skip. A skip is not PASS. Verify the selected case actually executed.

For real-copy evidence, supply an authorized `NATS_SWITCH_SEED_MODULE` exporting BOTH
`seedSwitchFixture` AND `verifySwitchPopulation`; it may seed only the fixture-owned copies passed
to it. The seed must return a nonempty serializable object. The verifier receives that same object
and BOTH actual native route snapshots. Its positive matching captured counts, zero historical live
routes, exactly two fresh native routes per role and total counts must agree with those snapshots.
Invalid exports, seed results or population evidence fail closed; hook claims alone are insufficient.
Without a seed module, ordinary non-R2 cases may use fresh synthetic fixtures, NOT capture-backed
evidence. R2 skips BEFORE creating a fixture or starting a broker, with reason `not capture-backed`.
The public fixture's `residency.enabled:false` input is not a supported product disable guard;
prove actual resident inventory/state instead of relying on that field.

R1 selection (template; do not run until the gates above and evidence collector are ready):

```sh
PI_FABRIC_NATS_REHEARSAL=1 NATS_BIN="$NATS_BIN" NATS_EXPECTED_SERVER_VERSION=2.14.7 NATS_AUTH_REQUIRED=1 \
NATS_SWITCH_PROBE_MODULE="$PWD/tests/helpers/nats-switch-probe.ts" \
NATS_SWITCH_ARTIFACT_DIR="$REHEARSAL_ARTIFACTS" \
bunx vitest run tests/nats-path-b-switch-rollback.test.ts -t 'R1 exact STOPALL'
```

`NATS_BIN` is a caller-supplied absolute executable path; `REHEARSAL_ARTIFACTS` is a new, private,
caller-owned output directory, not a live native root. If using the seed hook, export its absolute
module path as `NATS_SWITCH_SEED_MODULE` before R1 and R2; it is REQUIRED for capture-backed R2.
No private path or credentials belong here.
Inspect R1's actual result and normal shutdown, retain failures and complete its independent
acceptance barrier before authorizing R2. Do not infer a green exit from a business JSON record.
R2 is a separate capture-backed native evidence case, not a substitute for R1 rollback.
Its seed module and population verifier are mandatory; a missing seed is a skip, never a capture PASS:

```sh
PI_FABRIC_NATS_REHEARSAL=1 NATS_BIN="$NATS_BIN" NATS_EXPECTED_SERVER_VERSION=2.14.7 NATS_AUTH_REQUIRED=1 \
NATS_SWITCH_PROBE_MODULE="$PWD/tests/helpers/nats-switch-probe.ts" \
NATS_SWITCH_ARTIFACT_DIR="$REHEARSAL_ARTIFACTS" \
bunx vitest run tests/nats-path-b-switch-rollback.test.ts -t 'R2 full capture-backed'
```

The local `producerFence`/`stopProducers`/`resumeProducers` IPC gate controls test submissions only;
it is **not a product feature or an atomic fleet fence**. The real receiver, journal and ACK paths run.
The fixture fails closed if residual 4a recovery is needed; it does not implement that branch.

## Targeted FILE close diagnostics — known RED, separate from R1/R2

Both no-business close and FILE followUp/steer close are expected RED (`smarty-dev#14612`) until fixed.
With opt-in, do not suppress EPIPE or skip assertions; the fix must make both GREEN before the gate may default-on.
```sh
PI_FABRIC_NATS_REHEARSAL=1 NATS_BIN="$NATS_BIN" NATS_EXPECTED_SERVER_VERSION=2.14.7 NATS_AUTH_REQUIRED=1 \
bunx vitest run tests/nats-path-b-switch-rollback.test.ts -t 'FILE close without business traffic counterexample|counterexample FILE from start links fresh native mirrors and routes actual file followUp/steer'
```

## NATS → FILE: exact STOP-ALL operator sequence

Maintain one external cooperative quiet/no-relaunch window through steps 1–6.
Quiet is not atomic: account for every already-started publication, retry, cancellation and outcome.
Required links run normally before STOP-ALL; a publishing bridge is never exempt by its label.

1. **Quiet new business producers; release held native work and settle accepted sends.**
   Keep owners, sender ACK readers and required bridges operating while held pre-journal ingress
   is released. Await the actual API results AND strict shared owner+sender all-stage evidence.
   Require native command PubAck, accepted business result, one real persistent Main admission,
   owner accepted-ACK PubAck, owner command ACK-of-ACK and sender ACK-consumption confirmation.
   Retain command/message correlation and reconcile the manifest, not just observation counts.
   Refusal, `notRun`, UNKNOWN, TERM and delivery exhaustion are not business PASS.

2. **Initial BOTH-role drain.** Query every retained owner AND sender native inbox consumer;
   require `num_pending=0` AND `num_ack_pending=0` for the exact inventoried stream/durable/filter.
   One host inbox filter covers commands and ACKs; a sender-only inbox still belongs here.
   Let real deliveries, retries and final writes settle; missing/changed consumers or errors fail.
   Consumer counters are delivery observations, not a publisher barrier or proof of admission.
   Stream message count need not be zero under Limits retention; never purge to make it zero.

3. **Complete STOP of ALL runtimes AND publishing bridges, including every remote agent.**
   Use inventoried existing controls; await joined normal shutdown, actual terminal native state
   and a complete descendant/publisher inventory. A stop request, actor halt, pause, UI idle or
   close reply alone is insufficient. Keep automatic relaunch and new starts suppressed.
   Only independently inventoried standalone nonpublishers can remain up; keep the broker
   available for read-only consumer inspection. Preserve shutdown errors as separate failures.

4. **Frozen ALL-consumer recheck while ALL publishers remain stopped.** Query ALL retained
   owner/sender inboxes again; require unchanged retained identity/filter and exact 0/0 everywhere.
   Reject missing, replaced, malformed or errored consumer information. No config flip yet.
   **4a — residual only:** start ONLY the runtime owning that residual consumer on NATS,
   attempt its native drain, complete its normal STOP, then repeat the ALL-stopped ALL-consumer
   check. Include new ACKs in other stopped sender inboxes; repeat this exact barrier per attempt.
   Do not silently start peers or bridges. If owner-only startup requires extra publishers,
   cannot drain, or cannot reconcile dispositions, retain NATS config/quiet and escalate no-go.
   Owner-only restart can refuse stale authority (`FABRIC_SENDER_UNVERIFIED`/`notRun`) or stay UNKNOWN.
   Draining an old ACK cannot resurrect the stopped sender's pending Promise or recover its caller.
   A clean `attempts:0/not-needed` branch proves NO residual-recovery execution; 0/0 is not recovery.
   Apply the conditional checkpoint rules below if such a native lifecycle path is involved.

5. **Paired FILE configuration write/readback, still under completed STOP-ALL.** Only after
   frozen global zeros and the required manifest/evidence checks, set BOTH `mesh.transport=file`
   and host `mesh.hostTransport=file` everywhere; reconcile trusted project/env overrides.
   Read back both settings and preserved SQLite/FILE backends, actual roots, stable host IDs,
   journals, seen stores, bridge cursor and native transport state. No snapshot restore.

6. **Restore essential links and FILE runtimes; do NOT resume business producers yet.**
   Start with the same preserved roots/IDs using inventoried methods and verify FILE activation.
   Runtime startup necessarily publishes native presence/leases: do not claim ALL publishers
   remain OFF during this warm-up. The maintained restriction now concerns business producers.
   Require every expected fresh FILE-native own host and cross-mirror in each required direction,
   linked bridges, unexpired leases and `mirror.observedAt` AFTER restart and fresh at readback.
   NATS presence lives in KV; absent FILE-native mirrors while NATS was active is expected,
   not a preflip failure. Never synthesize mirrors. Missing/stale post-restart mirrors means HOLD.
   Only after the mirror gate may the operator release business producers. Reconcile every
   preflip command against persistent native evidence and run BOTH real FILE `agents.followUp`
   and `agents.steer`; require accepted results, one admission each and both-role stage evidence.
   Mirror readiness alone proves neither old-result recovery nor successful custody release.

## Conditional paused custody checkpoint — not ordinary FILE startup/send/stop

Ordinary FILE startup, send, Main admission, graceful shutdown and operator business release
are NOT drain-receipt-gated. Separate native paused custody/dormancy/cooperative-release paths
invoke `FabricControlPlane.checkpointForRelease` and synchronous `releaseCheckpointReceipt`.
Receipt enforcement is per actual runtime `options.hostId` with retained recognized NATS-marked
seen rows, including completed historical rows; FILE checks local and own-host shared rows.
A sender-only ACK consumer may have no such rows and need no receipt, yet still MUST drain.

Observe BOTH public entries distinctly before initialization THROUGH joined shutdown/terminal exit.
Attest the actual `AgentsProvider.control` receiver via `componentCatalog` and registry provider
notifications, not a nonexistent runtime control getter or unrelated imported prototype.
Pass-through observation must preserve exact receiver, arguments, Promise/synchronous return and
native errors; do not async-wrap the synchronous readback or alter lifecycle behavior.
Observer fault, unattested receiver, unknown resident or incomplete terminal coverage is UNKNOWN,
not NONE. Entry counts are neither per-row assertion counts nor valid receipts/custody release.

`writeNatsDrainReceipt` exists in the built internal `dist/topology/nats-drain-receipt.js` entry,
but validates declarations and writes: it does NOT query the broker. There is no verified turnkey
safe issuer/CLI here. Admin `drain` reports one consumer only; it does not fence, consume, ACK,
settle business work, mint a receipt or certify the global inventory. Do not hand-author an envelope,
invent timestamps or use fixture IPC declarations as authenticated issuance.
If a required native checkpoint actually hits, stop/escalate to a separately authorized issuer lane.
That lane must bind actual host/configured stream, final retained writes (including stop/4a),
authenticated existing-consumer checks immediately before durable write/readback and the maintained
barrier. No receipt bypass or universal mint prerequisite is implied by ordinary FILE success.

## Evidence, timings and handback

Use shared `waitForControlProbeEvidence`/`collectControlProbeEvidence` with BOTH roles and the
correct carrier. Shared v2 `ownerAckAtNs`/`allStagesVerified` can select an ACK publication without
checking its accepted operation: independently require accepted API results, persistent native
admissions and full manifest reconciliation. Never treat publication alone as business acceptance.
FILE `inbox-ack-confirmed` is native reader **in-memory** advancement, NOT durable cursor/broker ACK.
Actors are extra and unexercised; this page claims only Main followUp/steer coverage.

Record quiet, held-work release/acceptance, initial zeros, stop start/completion, frozen recheck,
each 4a attempt, paired config readback, restart/ready, fresh mirror gate and business release.
Separate start-to-ready, ready-to-mirror warm-up and inclusive restart-to-mirror timing; measure
outage from quiet through business release and verification through final manifest/post-FILE checks.
Keep actual measurements in the private qualified evidence handoff: these are **PRE-PROJECTION**
copied-fixture observations, not fleet capacity, cross-host latency or a presence-projection bar.

Stopped native roots/journals/seen state and full reports are PRIVATE. The fixture creates its
archive only inside the current owned private TMPDIR; stdout emits ONLY its SHA-256 and local private
archive path, never base64/archive chunks, forensic state, arbitrary hook evidence or full IPC reports.
Forensic projections, full terminal metadata and complete R1/R2 reports stay in private artifact JSON.
The archive is real and locally readable at preservation time, but the test environment may retire
TMPDIR on exit: a printed path is NOT a promise of persistent retention. If audit requires retention
beyond that lifetime, obtain separate authorized custody before running; do not invent an export.
Existing retained private baselines remain immutable and do not use a new stdout decoder.
Before authorized audit-only extraction, verify the archive hash, paths, links/devices and size bounds;
extract ONLY into a new owned private temporary audit copy, NEVER into a live native root.
Do not copy/rename/symlink out to a refused destination, use alternate export APIs, clear deletion guards,
relax guards or paste opaque archives into model/public text. There is no opt-in to print archive bytes.
Keep artifact and cleanup errors separate; neither may mask the other or turn command RED green.

Source bindings: [switch controller](../tests/helpers/nats-switch-runtime.ts),
[case selections](../tests/nats-path-b-switch-rollback.test.ts), [thin adapter](../tests/helpers/nats-switch-probe.ts),
[shared probe](../tests/helpers/nats-path-b-probe.ts), [control checkpoints](../src/topology/control-plane.ts),
[receipt validator/writer](../src/topology/nats-drain-receipt.ts), [admin reports](../src/transport/nats/admin.ts),
[carrier config](../src/config.ts), [native bridge](../src/mesh/bridge.ts),
and [latency probe / metric contract](nats-path-b-latency-probe.md).
NATS references: [Consumer Info](https://docs.nats.io/reference/2.12/jetstream/api/consumer/info),
[acknowledgment](https://docs.nats.io/learn/jetstream/acknowledgment), [retention](https://docs.nats.io/learn/jetstream/retention-policies),
[headers](https://docs.nats.io/reference/2.12/jetstream/api/headers).
