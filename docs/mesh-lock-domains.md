# Mesh-lock domains: audit of every `<mesh>/.lock` call site

Plan: smarty-dev#6477, lane A1 (plan rules R3, R5, R7, R11, R20).
Tree: `main` at `31be825c`. Each `file:line` below was read in this tree. Paths are under `src/`.

## 0. Domains and counts

| Domain | Meaning after cutover |
|---|---|
| `state` | Moves into the `state.db` SQLite transaction (`BEGIN IMMEDIATE`). |
| `events` | Stays on `.lock`: publish, archive barriers, dedupe receipts, `publishBatch`. |
| `custody` | Moves to `custody.lock` (file custody: Main inbox, participant key-lock recovery). |
| `none` | Takes no lock itself. |

The lock primitive is `MeshStore.#withLock` (`mesh/store.ts:2389`). It creates `<mesh>/.lock/` with an `owner` file, uses the FIFO ticket directory `<parent>/.pi-fabric-mesh-lock-<uid>/<sha256(root)>/` (`mesh/lock-queue.ts:12`) and recovers stale holders (`#clearStaleLock`, `mesh/store.ts:2522`).

Call sites outside `mesh/store.ts` (104 call expressions, all verified):

| Entry point | Sites | `state` | `events` | `custody` | `none` |
|---|---|---|---|---|---|
| `put` | 31 | 31 | | | |
| `delete` | 14 | 14 | | | |
| `writeBatch` | 11 | 11 | | | |
| `confirmWritable` | 1 | 1 | | | |
| `publish` | 29 | | 29 | | |
| `publishBatch` | 1 | | 1 | | |
| `exclusive` | 15 | 4 | | 11 | |
| `withTryLock` | 2 | | | | 2 |
| **Total** | **104** | **61** | **30** | **11** | **2** |

Notes on the counts:

- The plan counts 13 `exclusive()` sites. The real count is **15 call expressions**: 13 lock bodies plus 2 forwarding lambdas in `topology/host-reaper.ts:161` and `:200`. These pass `mesh.exclusive` into the bodies at `topology/participant-files.ts:422` and `:237`.
- **11 of 15 `exclusive()` sites are `custody`.** Only 4 touch `state`. No `exclusive()` site is `events`.
- 2 `state` sites write only to a `control-seen` store (`topology/control-plane.ts:1209`, `:1264`). That store is a separate `MeshStore` with its own lock at `<mesh>/control-seen/<host-hash>/.lock` (`topology/control-plane.ts:386`). It is not `<mesh>/.lock`. R20 keeps it on `file`. `topology/control-plane.ts:341`/`:342` serve both stores.
- Two-domain sites: 6. See section 1.5.
- `custody.lock` (lane L5, #591) is **not in this tree**. There is no `custody-lock.ts`. All 11 `custody` sites still take `<mesh>/.lock`.

## 1. Call sites

### 1.1 Lock primitives in `mesh/store.ts`

| file:line | Entry point | Under the lock: reads | Under the lock: writes | Domain |
|---|---|---|---|---|
| `mesh/store.ts:1080` | `publish` → `#preparePublish` closure (`:925`) | `event-receipts/<h>.json` and `.pending.json`, `sequence`, tail of `events.jsonl`, archive head and lookup | see 1.2 | `events` |
| `mesh/store.ts:1105` | `publishBatch` (≤256 events, ≤50 ms) | same, for each event | same; then `#compactEventLog`, one `fsync` of `events.jsonl` | `events` |
| `mesh/store.ts:1806` | `#withWriteSnapshot` (`:1792`), no stat identity (fallback) | `state.json` (full parse) | `state.json` commit (1.3) | `state` |
| `mesh/store.ts:1816` | `#withWriteSnapshot`, optimistic path | `state.json` stat identity only (prepared off-lock) | `state.json` commit (1.3) | `state` |
| `mesh/store.ts:2037` | `exclusive(operation)` | caller-defined | caller-defined | per caller (1.4) |
| `mesh/store.ts:2047` | `confirmWritable(onAcquired)` | nothing | memory only: `#requireCanonicalRead`, cache age | `state` (witness, R13) |

`put` (`:1984`), `delete` (`:2055`) and `writeBatch` (`:2099`) all go through `#withWriteSnapshot`.

### 1.2 Event path under `.lock` (all `events`)

All of these run only inside the `publish`/`publishBatch` lock. None has its own entry point.

| file:line | Step | Files |
|---|---|---|
| `mesh/store.ts:2650` | `#repairEventLog`: cut a torn tail | `events.jsonl` (truncate) |
| `mesh/store.ts:1203` | `#recoverArchive`: reboot promote, catch-up (`:1226`) | `events.jsonl` append + `fdatasync`, `sequence`, archive files |
| `mesh/store.ts:840`, `:915` | `#settleDedupeIntent(s)`: dedupe recovery | `event-receipts/*.pending.json`, `*.json` (durable), archive lookup/commit |
| `mesh/store.ts:999` | `archive.installDigestRepair` | archive `DIGEST-REPAIR.json` |
| `mesh/store.ts:1039` | `archive.reserveLookup` (dedupe only) | archive lookup |
| `mesh/store.ts:1046`, `:1058` | `archive.begin` / `archive.commit` (barriers) | archive day file + `fdatasync`, sidecar |
| `mesh/store.ts:1002` | `data(createdAt)` stamp callback of the caller | caller-defined (see 3) |
| `mesh/store.ts:2619` | `#compactEventLog`: live compaction above the cap | `events.jsonl` (durable rewrite), `generation` |
| `mesh/store.ts:794` | `#confirmEventFile`: `fsync` for receipts and `durable` | `events.jsonl` |

### 1.3 State commit under `.lock` (all `state`)

| file:line | Step | Files | After cutover |
|---|---|---|---|
| `mesh/store.ts:2138` | `writeBatch` `prepare(view)` callback | caller-defined (see 3) | inside the transaction |
| `mesh/store.ts:2143`, `:2162` | `op.condition`, `op.value(now)` callbacks | caller-defined (see 3) | inside the transaction |
| `mesh/store.ts:1917` | rename staged `state.json` | `state.json` | SQLite rows |
| `mesh/store.ts:1928` | `appendStateJournal` | `state.read-journal.jsonl` | not needed (R7) |
| `mesh/store.ts:1930` | `#writeSignal` | `state.read-signal.json` (with witness) | not needed (R7) |
| `mesh/store.ts:1924` | `commitStats.record` | memory; flushed by a timer off-lock | unchanged |
| `mesh/store.ts:1936` | `PI_FABRIC_COMMIT_TRACE` append | trace file (diagnostic) | after `COMMIT` |
| `mesh/store.ts:2194` | `afterCommit(view)` callback | caller-defined (see 3) | after `COMMIT` (R11) |

### 1.4 `exclusive()` sites (15)

| # | file:line | Caller | Reads under the lock | Writes under the lock | Domain |
|---|---|---|---|---|---|
| X1 | `fabric-runtime-state.ts:654` | `initialize` → `registerMainInbox` (`topology/stall-alarms.ts:41`). Main only. | nothing | `main-followups/<session>.owner.json`, `main-followups/successors/<h>.json` (durable) | `custody` |
| X2 | `topology/stall-alarms.ts:51` | `recordMainSuccessor` | owner, successor files | successor file, owner file (durable) | `custody` |
| X3 | `topology/stall-alarms.ts:64` | `stageMainSuccessor` | owner file | `main-followups/rotations/<h>.json`, owner file | `custody` |
| X4 | `topology/stall-alarms.ts:77` | `confirmMainSuccessor` | rotation intent, owner, successor | successor file; unlink intent | `custody` |
| X5 | `topology/stall-alarms.ts:200` | `MainInboxMaintenance.run`: move a dead inbox item | owner, successor, route files; process liveness | `main-followups/routes/<h>.json`; Main journal `main-followups/<session>.json` | `custody` |
| X6 | `topology/stall-alarms.ts:238` | `MainInboxMaintenance.run`: recover a claim | route, owner, successor files; session receipts | route file; journal; `main.receiveInboxItem` (writes the Main journal, can release to Pi) | `custody` |
| X7 | `topology/stall-alarms.ts:270` | `MainInboxMaintenance.run`: mark claim done | route file | route file (`done`) | `custody` |
| X8 | `topology/participant-files.ts:237` | `recoverDeadKeyLock`. Callers: `withKeyLock` (`:200`) for every `writeParticipantFileIf`/`removeParticipantFileIf`; `prepareParticipantFileLock(s)` (`:251`, `:260`), from `actors/manager.ts:5291` | `participants/.locks/<h>/owner` | rename to `<h>.<uuid>.dead`, remove or restore | `custody` |
| X9 | `topology/participant-files.ts:422` | `sweepParticipantLockLeftovers` | `participants/.locks/` listing, mtimes | remove old `*.tmp`, `*.dead` | `custody` |
| X10 | `topology/host-reaper.ts:161` | `reapDeadHostRecords`: forwards to X9 | as X9 | as X9 | `custody` |
| X11 | `topology/host-reaper.ts:200` | `reapDeadHostRecords`: forwards to X8 via `removeParticipantFileIf` | as X8 | as X8 | `custody` |
| X12 | `topology/participant-directory.ts:1247` | `refreshRoutingView` (250 ms budget) | `state.json` (`assertMeshStateReadable`), participant list (state + `participants/*.json`) | memory only | `state` (read) |
| X13 | `residency/host.ts:329` | `waitForPublicationRetry`: acquire and release only | nothing | nothing | `state` (wait only) |
| X14 | `residency/host.ts:941` | `#queueDelivery` with a function `rootId` | state via `rootId()` (lineage proof) | `residency/.../outbox/<id>.json` (durable) | **`state` + `custody`** |
| X15 | `actors/manager.ts:5309` | `#confirmAdoption`, inside the actor registry lock | state: participant, legacy session, lineage closure (`#lineageMayBeAlive` → `topology/participant-directory.ts:1164`); `participants/<h>.json` presence | actor registry commit (`prepared.commit()`); participant key lock create/release (`withParticipantFileTryLock`) | **`state` + registry** |

X13 has no data access. It waits for a free window before a registry-fenced retry of the heartbeat. After cutover it must wait for the state write lock (or the R2 async ticket). If R2 gives an async wait, X13 can become `none`.

X14: in this tree no caller passes a function `rootId`. The three callers (`residency/host.ts:489`, `:521`, `:870`) pass a string or the default. The branch is dormant.

### 1.5 Two-domain sites and their split (R20)

R20: lock order is registries, then the state transaction. `.lock` inside a state transaction only in migration tools.

| Site | Domains | Today | Split |
|---|---|---|---|
| `mesh/bridge.ts:310` `publish`, `:315` `publishBatch` (data callback `:332` → `holds()` `:348`) | `events` + `state` read | The `data()` callback reads host and participant state fresh under `.lock` at commit. Comment `:329`: "Every state writer takes the same lock" (security F2: a native takeover before the commit refuses the event). | **Plan gap.** After cutover state writers do not take `.lock`, so the F2 fence breaks silently. Fix: take `.lock`, then `BEGIN IMMEDIATE` on `state.db`, run `holds()`, append the event, `ROLLBACK`, release `.lock`. This nests state inside `.lock` at run time. R20 must allow this order (events outer, state inner) for the bridge. Alternative: move the bridge ownership check into state with a fence on the next native write. |
| `mesh/bridge.ts:438` `#mirror` `writeBatch` | `state` + host-lease files | `prepare` reads `participants/*.json`; `afterCommit` (`:505`) writes or removes `host-leases/*` under the lock. | Read files before `BEGIN`, check their stamps again in the transaction. Lease writes go after `COMMIT` through the outbox (R11). |
| `topology/participant-directory.ts:1852` heartbeat `writeBatch` | `state` + host-lease file reads | `prepare` → `compactExpiredHostRecords` reads `host-leases/<h>` (`topology/host-record-compaction.ts:22`); `condition` reads it again. | File reads before `BEGIN`; check the lease stamp in the transaction. |
| `topology/host-reaper.ts:166` `writeBatch` | `state` + file reads | `prepare` and each `condition` read `host-leases/` (`:33`, `:88`). | As above. |
| X14 `residency/host.ts:941` | `state` + `custody` | Root selection reads lineage state; the durable outbox write is the irreversible effect. | `BEGIN IMMEDIATE`, select the root, `COMMIT` a selection row (or an outbox row in `state.db`); write the outbox file after `COMMIT`. Dormant: low priority. |
| X15 `actors/manager.ts:5309` | registry + `state` | Registry lock, then `.lock`, then the participant key try-lock; registry commit inside. | Registry lock, then `BEGIN IMMEDIATE` (read-only fence, serializes with `resumeLineage` at `topology/participant-directory.ts:1414`), then key try-lock, registry commit, `ROLLBACK`. This puts a file commit inside the state transaction. It is the registry's own atomic commit and writes no state, so it cannot go to an outbox. R11 needs this exception. |

### 1.6 `put`, `delete`, `writeBatch`, `confirmWritable`, `withTryLock` sites (state, none)

All `state` unless marked. "Key family" is the `state.json` key prefix.

| file:line | Entry point | Caller | Key family |
|---|---|---|---|
| `topology/stall-alarms.ts:110` | `writeBatch` (delete) | `absence` | `topology/root-absence/` |
| `topology/stall-alarms.ts:114` | `writeBatch` (`prepare`) | `absence` | `topology/root-absence/` |
| `topology/root-inbox.ts:380` | `put` | `RootInbox.#save` | `topology/inbox/` |
| `topology/control-plane.ts:341` | `put` | `#putFenced` (no consumption gate) | `topology/control-seen/` (mesh or seen store) |
| `topology/control-plane.ts:342` | `writeBatch` (`prepare`) | `#putFenced`; called at `:995`, `:1046` (mesh) and `:1060`, `:1171` (seen store) | `topology/control-seen/` |
| `topology/control-plane.ts:1209` | `put` | `#executeClaimedCommand` | `topology/control-seen/` (**seen store only**) |
| `topology/control-plane.ts:1264` | `writeBatch` | `#cleanupSeen` | `topology/control-seen/` (**seen store only**) |
| `topology/control-plane.ts:1318` | `writeBatch` | `#cleanupLegacySeen` | `topology/control-seen/` |
| `topology/participant-directory.ts:1414` | `delete` | `resumeLineage` | `topology/lineage-closures/` |
| `topology/participant-directory.ts:1424` | `put` | `closeLineage` | `topology/lineage-closures/` |
| `topology/participant-directory.ts:1454` | `delete` | `close` | `topology/participants/` |
| `topology/participant-directory.ts:1459` | `delete` | `close` | `sessions/` |
| `topology/participant-directory.ts:1467` | `delete` | `close` | `topology/hosts/` |
| `topology/participant-directory.ts:1816` | `confirmWritable` | `#refresh` (idle heartbeat witness) | none (lock only) |
| `topology/participant-directory.ts:1852` | `writeBatch` (`prepare`, `afterCommit`, `value` fn) | `#refresh` (heartbeat) | `topology/hosts/`, `topology/participants/`, `sessions/`, `actors/` (presence batch) |
| `topology/participant-directory.ts:1938` | `withTryLock` (0 ms) | `#renewActors` around `writeParticipantFileIf` | none: bounds X8 (`custody`) |
| `topology/participant-directory.ts:2077` | `put` | `#claimPeerSeq` | `topology/peer-seq` |
| `topology/host-reaper.ts:166` | `writeBatch` (`prepare`, `condition`) | `reapDeadHostRecords` | `topology/hosts/`, `topology/participants/`, `topology/inbox/`, `sessions/` |
| `state/store.ts:664`, `:797`, `:842`, `:885`, `:892`, `:1163`, `:1243`, `:1279` | `put` ×7, `delete` ×1 (`:892`) | `transition`, `advanceHead`, `goal`, `checkGoal` | `state/` (`state/current`, `state/goal`, `state/complexity/`) |
| `schema/controller.ts:167`, `:240`, `:247`, `:256`, `:373`, `:401`, `:438`, `:520`, `:528`, `:547`, `:567`, `:771` | `put` ×12 | `hypothesize`, `verify`, `commit`, `abort`, `endInvocation`, `#recordFailedOutcome` | `schema/workspace`, `schema/hypothesis/`, `schema/certificate/` |
| `mesh/bridge.ts:438` | `writeBatch` (`prepare`, `afterCommit`) | `#mirror` | `topology/hosts/`, `topology/participants/` (mirrors) |
| `residency/client.ts:1099`, `:1183`, `:1213` | `delete` ×3 | `#adoptCompletion`, `#deliver` | `residency/deliveries/` |
| `residency/host.ts:301` | `withTryLock` (50 ms) | `publishFenced` (registry → mesh) | none: bounds the heartbeat `writeBatch` and X8 |
| `residency/host.ts:972`, `:978` | `put` ×2 | `#flushDeliveries` | `residency/deliveries/` |
| `agents/completion-journal.ts:627` | `put` | `drain` | `residency/completion-claims/` |
| `agents/completion-journal.ts:675` | `delete` | `#retireClaim` | `residency/completion-claims/` |
| `lifecycle/broker.ts:148` | `put` | `subscribe` | `topology/subscriptions/` |
| `lifecycle/broker.ts:174` | `delete` | `unsubscribe` | `topology/subscriptions/` |
| `lifecycle/broker.ts:413` | `writeBatch` (`condition`) | `#confirmDelivered` | `topology/subscriptions/` |
| `lifecycle/broker.ts:415` | `delete` | `#confirmDelivered` | `topology/subscriptions/` |
| `lifecycle/broker.ts:434` | `put` | `#replace` | `topology/subscriptions/` |
| `lifecycle/broker.ts:438` | `writeBatch` (`value` fn) | `#replace` | `topology/subscriptions/` |
| `actors/presence-reaper.ts:75` | `writeBatch` | `reapDeadSessionPresence` | `actors/` |
| `actors/manager.ts:2256` | `delete` | `#finishCleanup` | `actors/` |
| `actors/manager.ts:4125`, `:4128` | `put`, `delete` | `#writePresenceNow` | `actors/` |
| `providers/mesh-provider.ts:288`, `:298` | `put`, `delete` | `invoke` (`fabric_exec` `mesh.put`/`mesh.delete`) | any user key |

`withTryLock` (`mesh/store.ts:2024`) takes no lock. It sets an async scope that caps every acquisition inside it. After cutover the same scope must cap `BEGIN IMMEDIATE` (R2 `busy_timeout` ≤5 ms) and `custody.lock`.

### 1.7 `publish` and `publishBatch` sites (all `events`)

| file:line | Caller | Topic / note |
|---|---|---|
| `fabric-runtime-state.ts:1213` | records `publisher` (from `records/service.ts:120`, `:127`) | records lag alarms |
| `fabric-runtime-state.ts:1772` | `publishOpsEvent` | `ops.fabric.*` |
| `fabric-runtime-state.ts:1782` | `#publishCompactEvent` | `fabric.compact` |
| `topology/stall-alarms.ts:138` | `rootPresenceAlarms` | `ops.owner`, dedupe |
| `topology/stall-alarms.ts:174` | `MainInboxMaintenance.run` | `ops.owner`, dedupe |
| `topology/stall-alarms.ts:267` | `MainInboxMaintenance.run` | `fleet.work.inbox-receipts`, dedupe |
| `topology/root-inbox.ts:221` | `wake` | notification |
| `topology/control-plane.ts:621` | `#requestAcceptance` | control command |
| `topology/control-plane.ts:791` | `#publishCancellation` | control cancel |
| `topology/control-plane.ts:1329` | `#publishAcknowledgement` | ACK; `data()` callback checks lease (memory) |
| `topology/participant-directory.ts:956`, `:977` | `#reportRootCollisions`, `#reportRefusal` | ops alarms |
| `state/store.ts:652`, `:703`, `:746`, `:1193`, `:1443`, `:1505` | `transition`, `checkGoal`, `verify` | state topic events |
| `schema/controller.ts:812` | `#publish` | schema topic |
| `mesh/bridge.ts:310` | `StoreBridgeSide.publish` | **two-domain** (1.5) |
| `mesh/bridge.ts:315` | `StoreBridgeSide.publishBatch` (only `publishBatch` caller) | **two-domain** (1.5) |
| `residency/host.ts:1153` | `#advanceRelease` | `host.reloaded` |
| `lifecycle/broker.ts:82` | `publish` | lifecycle events |
| `actors/manager.ts:1401`, `:1471`, `:1776`, `:3265`, `:3836`, `:4042` | session alarm, `steerRemote`, `#relayHostEvent`, activation alarm, dead letter, `#publishNotification` | actor events |
| `providers/mesh-provider.ts:216` | `invoke` (`fabric_exec` `mesh.publish`) | user topics |

The bridge also calls `target.publish`/`target.publishBatch` at `mesh/bridge.ts:1083`, `:1146`. On the local side these reach `:310`/`:315`. On the remote side they reach the remote bridge agent.

## 2. Operation census (R5)

The census should see `meshProtocol` from each process that does an operation before cutover. It is advisory only (smarty-dev#6982): it reports, it never blocks or permits a cutover, because pid-only lock evidence cannot be attributed safely. "Pi runtime" is `FabricRuntimeState` (`fabric-runtime-state.ts:607`). It runs in Main and in child agent and actor Pi sessions (`identity.kind` `agent`/`actor` get a `writeSignal`, `:605`).

| Operation | Domain | Main | Child / actor Pi session | Resident host | Bridge (`bin/mesh-bridge` run and agent) | CLIs in `bin/` |
|---|---|---|---|---|---|---|
| Heartbeat `writeBatch`, `confirmWritable` (`topology/participant-directory.ts:1816`, `:1852`) | `state` | yes | yes | yes (registry-fenced, `withTryLock` 50 ms) | no | no |
| Participant close and lineage (`:1414`–`:1467`), peer-seq (`:2077`) | `state` | yes | yes | yes | no | no |
| Dead-host reap (`topology/host-reaper.ts:166`) | `state` | yes | yes | no (`reapDeadHosts: false`, `residency/host.ts:334`) | no | no |
| Root absence (`topology/stall-alarms.ts:110`, `:114`) | `state` | yes | yes | yes (`presencePass`) | no | no |
| Root inbox (`topology/root-inbox.ts:380`) | `state` | yes (Main only) | no | no | no | no |
| Control claims (`topology/control-plane.ts:342`, `:1318`) | `state` | yes | yes | yes | no | no |
| Control-seen store (`:1209`, `:1264`) | own `.lock` | yes | yes | yes | no | no |
| `state/*` and `schema/*` (`state/store.ts`, `schema/controller.ts`) | `state` | yes | yes | no | no | no |
| `fabric_exec` `mesh.put`/`delete` (`providers/mesh-provider.ts`) | `state` | yes | yes | no | no | no |
| Actor presence (`actors/manager.ts:2256`, `:4125`, `:4128`), presence reap (`actors/presence-reaper.ts:75`) | `state` | yes | yes | yes | no | no |
| Lifecycle subscriptions (`lifecycle/broker.ts`) | `state` | yes | yes | yes | no | no |
| Resident deliveries put (`residency/host.ts:972`, `:978`) | `state` | no | no | yes | no | no |
| Resident deliveries delete, completion claims (`residency/client.ts`, `agents/completion-journal.ts`) | `state` | yes (owner of the persistent actor registry, `fabric-runtime-state.ts:1005`) | no | no | no | no |
| Bridge mirror (`mesh/bridge.ts:438`) | `state` | no | no | no | yes | no |
| Routing view (X12), publication wait (X13) | `state` | yes (X12) | yes (X12) | yes (X12, X13) | no | no |
| Delivery root selection (X14) | `state`+`custody` | no | no | yes (dormant) | no | no |
| Actor adoption (X15) | registry + `state` | yes (actor directory) | yes | yes | no | no |
| `publish` | `events` | yes | yes | yes | yes | no |
| `publishBatch` | `events` | no | no | no | yes | no |
| Main inbox custody (X1–X7) | `custody` | yes (Main only, `mainAgent.local`) | no | no | no | no |
| Participant key-lock recovery (X8–X11) | `custody` | yes | yes | yes | no | no |

CLIs in `bin/`:

| CLI | Takes `<mesh>/.lock` | Note |
|---|---|---|
| `mesh-bridge` | yes | `openBridgeStore` (`mesh-bridge.ts:62`). `--lock-protocol` defaults to `1`, independent of the config. The census must read the bridge flags. |
| `fabric-participants` | no write | `ReadOnlyMeshStore` (`participants-cli.ts:34`) refuses `publish`, `put`, `delete`, `writeBatch`, `confirmWritable`. It does not override `exclusive` or `withTryLock`; no code path in the CLI calls them. It reads `state.json` directly (`:125`). |
| `fabric-actors` | no | Uses `ResidentActorClient` requests (`actors-cli.ts:75`). |
| `fabric-judge`, `fabric-releases` | no | No mesh store. |

## 3. Callbacks with I/O or effects inside the lock (R11)

| file:line | Callback | I/O or effect | After cutover |
|---|---|---|---|
| `mesh/bridge.ts:505` | `#mirror` `afterCommit` | writes `host-leases/<h>.json` (`writeHostLease`), removes leases | effect: after `COMMIT`, idempotent outbox; re-check `view.get` from the committed rows |
| `topology/participant-directory.ts:1854` | heartbeat `afterCommit` | memory only (`committedAt`, `publication.committed()` → `actors/manager.ts:942`) | after `COMMIT`; no outbox needed |
| `topology/participant-directory.ts:1853` | heartbeat `prepare` → `compactExpiredHostRecords` | reads `host-leases/<h>.json` per expired host (`topology/host-record-compaction.ts:22`); `condition` reads it again | read: before `BEGIN`; check the lease stamp in the transaction |
| `topology/host-reaper.ts:184`, `:175` | reaper `prepare`, `condition` | reads `host-leases/` (`:33`, `:88`) | read: before `BEGIN`, stamp check |
| `mesh/bridge.ts:445` | `#mirror` `prepare` | reads `participants/*.json` (`readParticipantFiles`), `participantFilePresent` | read: before `BEGIN`, stamp check |
| `mesh/bridge.ts:332` | bridge publish `data()` | reads state (host, participant records) | needs `BEGIN IMMEDIATE` inside `.lock` (1.5) |
| `topology/control-plane.ts:342`, `:1334` | `#putFenced` `prepare`, ACK `data()` | memory (`canConsumeMesh`) | inside the transaction |
| `lifecycle/broker.ts:414`, `:441` | `condition`, `value(now)` | memory (`canConsumeMesh`) | inside the transaction |
| `topology/participant-directory.ts:1840`, `:1593` | host record and legacy session `value(leaseAt)` | memory | inside the transaction |
| `topology/stall-alarms.ts:114` | `prepare` | memory (view) | inside the transaction |
| `topology/participant-directory.ts:1816` | `confirmWritable(onAcquired)` | memory | inside the transaction |
| `mesh/store.ts:1928`, `:1930`, `:1936` | store commit internals | read-journal append, read signal + witness, commit trace | journal and signal: retire (R7); trace: after `COMMIT` |
| X1–X7, X8–X11 | `custody` bodies | durable file writes, renames, `main.receiveInboxItem` | stay inside `custody.lock`; no state transaction |
| X14 | delivery root selection | durable outbox file write | effect after `COMMIT` (1.5) |
| X15 | adoption | registry commit, participant key lock | inside the read-only state transaction (exception, 1.5) |

## 4. Change detection that reads `state.json` or `.lock` (R7)

| file:line | User | Reads today | Must read after cutover |
|---|---|---|---|
| `topology/publication-generation.ts:8` | `publicationGeneration`; used by `actors/manager.ts:4409`, `:4434` (registry save validation) | stat of `state.json`, `participants/`, `host-leases/` | `meta` counter from `state.db` (compares across time and connections; `data_version` is per connection) plus the two directory stamps |
| `mesh/store.ts:2203` | `stateStamp()`; used by `ui/controller.ts:811` | stat of `state.json` | `meta` counter |
| `mesh/store.ts:2222` | `cachedStateStamp()`; used by `ui/controller.ts:829`, `:846` | cached stat tuple | `meta` counter value of the cached read |
| `mesh/store.ts:1630` | `stateToken()`; used by `topology/participant-directory.ts:861`, `:1020`, `:1130`, `:1543` | cached parse identity | cache keyed by `data_version` |
| `mesh/store.ts:1671`, `:1697`, `:1727` | `#signalledState`, `#readSignalIndex`, `#canonicalGeneration` | `state.read-signal.json`, `readGeneration` header, witness | `data_version`; per-namespace reuse by row versions |
| `mesh/read-journal.ts:101`, `:289`, `:310` | witness, journal append, journal replay | `state.read-journal.jsonl`, `state.read-signal.json` | not used with `sqlite` (keep for `file` and rollback) |
| `mesh/store.ts:1783`, `:1816` | `#stateWriteIdentity` (optimistic write) | stat identity of `state.json` | `BEGIN IMMEDIATE`; prepared work checked by `meta` counter |
| `topology/participant-directory.ts:1816` | heartbeat witness `confirmWritable` (#24; `topology/peer-settle.ts:124` uses `confirmedAt`) | `.lock` acquire | `BEGIN IMMEDIATE` plus a witness row only lock holders write (R13) |
| `topology/participant-directory.ts:1250`, `participants-cli.ts:125` | `assertMeshStateReadable` | parses `state.json` | read from `state.db` |
| `topology/participant-directory.ts:1084` | `lockWaiting` for lease-renewal wait | `existsSync(<mesh>/.lock)` | the state write-lock signal (L8 metric or SQLite busy); `.lock` no longer shows a heartbeat writer |
| `providers/agents-message-router.ts:400` | durable actor route recovery after `FABRIC_MESH_LOCK_TIMEOUT` | `<mesh>/.lock/owner` PID and mtime | the lock that timed out: state lock evidence for state writes |

## 5. Upstream mesh-lock users (schedules, grants)

Not present in this tree. No source file has a schedule or grant store. `lifecycle/delivery-scheduler.ts` has no mesh call. Commit `b9d86895b` (wake-text grants) adds no mesh-lock user. Classify them at the next upstream sync.

## 6. Findings that change the plan

1. `exclusive()`: 15 sites, not 13. 11 are `custody`, 4 are `state`, 0 are `events`. L2b covers 4 state sites, not 13.
2. `custody.lock` (L5) is not in `main` `31be825c`. The 11 custody sites still use `.lock`.
3. The bridge publish `data()` callback reads state under `.lock` to fence native takeover (F2). With state in SQLite this breaks without an error. R20 must allow `.lock` → `BEGIN IMMEDIATE` (events outer, state inner) for the bridge, or the fence must move.
4. Adoption (X15) commits the actor registry inside the would-be state transaction. R11 needs an explicit exception.
5. Two lock observers read `.lock` directly (`topology/participant-directory.ts:1084`, `providers/agents-message-router.ts:400`). L2b must point them at the state lock.
6. `mesh-bridge` defaults to lock protocol 1, independent of the config. The census must read bridge command lines.
