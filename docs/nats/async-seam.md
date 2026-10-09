# Async store seam — U2 (held, opt-in)

Refs smarty-dev#6477; continues #716's design in [nats-kv-state.md](../nats-kv-state.md). No GitHub was contacted. Main inspected at `efa1c0c925305212368f8a804933cc96a84356fc`. At task start (`6e5f8496`), its existing `src/` files were byte-identical to this branch, apart from added experimental NATS modules/exports. The census uses an exact local main archive, so references below are pre-seam main lines.

## Decision: (a), async authority, migrated by capability

Use the existing `AsyncMeshStateStore` and `openAsyncMeshStateStore({backend: "nats-kv", nats: {experimentalNatsKv: true, ...}})` selector. Await reads **end to end** in an audited single-key caller: the mesh provider's `shared/` namespace. `MeshProvider.withStateBackend` is an explicit asynchronous factory, not an environment/runtime-config switch. The ordinary constructor is unchanged. `shared/` has no host-coordination consumers on inspected main. All other namespaces, typed StateStore/schema operations, events, participants, UI and custody stay with the supplied legacy MeshStore. Cross-namespace lists merge sorted detached results; they are not cross-backend snapshots. NATS errors never fall back to file.

This is the least-code correct seam, **not a whole-runtime NATS cutover**. The async interface intentionally has no atomic multi-key batch, snapshot token, synchronous stamp, shared object identity or synchronous write fence. Unsupported capabilities remain unavailable by type, not simulated by sequential KV puts. File/SQLite implementations/interfaces are unmodified; fabric-store owns SQLite. Existing `mesh.stateBackend`, its environment override and file default are untouched. NATS is never selected at startup/by default; no fleet setting changes.

### Cost and alternatives

A TypeScript-symbol census finds **284** relevant main state method/property references: **221** outside the four store implementations, including **159** consuming the synchronous surface (transaction-view reads, cache properties and close included). Unrelated Map/SQLite-statement methods are excluded. There are **15** external `writeBatch` sites; migration needs transaction design, not just `await`. Exact annotated inventory: retained `main-store-call-sites.txt`. This bounded implementation migrates the mesh provider's two async-capable read sites, routes its already-async put/delete, and owns awaited shutdown. Full (a) migration also propagates promises through the families below and their callers/tests; `prepare`, delete conditions, outbox effects and event-append fencing must be redesigned or kept transactional. Costs: one leader RPC/get, one RPC/scanned key, explicit errors/deadlines and awaited teardown.

(b) A write-through/watch cache is rejected: sync `fresh` reads cease being leader authority; cross-host batch replication can partially commit. #708 leases alone do not fence protected writes: the resource must persist/enforce the lease token on **every** write, and checking a lease before a separate KV put has a TOCTOU gap. Correct replication needs durable intents/cursors, provisional-vs-acknowledged versions, conflict/offline policy and downstream fencing. The local #708 lease design explicitly keeps that integration held. More code, weaker semantics.

(c) `Atomics.wait`/worker bridge is rejected: bounded blocking still stalls Pi's event loop and cannot create multi-key atomicity, snapshots or distributed synchronous custody. It adds uncertain-commit and shutdown failure modes without fixing authority.

## Synchronous results on main (file:line)

Contract: `src/mesh/state-backend.ts:190-224`; facade: `src/mesh/store.ts:368-431`. File reads/tokens: `src/mesh/state-file.ts:652-686`; fence/batch: `src/mesh/state-file.ts:1087,1148-1270`. SQLite reads/batch/fence: `src/mesh/state-sqlite.ts:513-540,638-692,726`; adapter tokens/fence: `src/mesh/state-backend.ts:392-426,484-495`. Read only, never edited.

Combine each file with each line in its cell (`file:line`). Every external synchronous-surface reference is listed; duplicate references on a line collapse. An async enclosing function still dereferences a sync result today. Transaction-view reads are included: naively awaiting them breaks callback custody.

| File / family | Lines requiring synchronous results |
| --- | --- |
| `src/actors/manager.ts` | 1015, 4304, 4543, 4544 |
| `src/actors/presence-reaper.ts` | 41, 45, 58 |
| `src/agents/completion-journal.ts` | 563, 599, 603, 621, 655 |
| `src/fabric-runtime-state.ts` | 1823, 1953 |
| `src/lifecycle/broker.ts` | 159, 172, 226, 389, 409 |
| `src/mesh/backend-migration.ts` | 567 |
| `src/mesh/bridge.ts` | 223, 309, 362, 413, 433, 434, 450, 456, 495, 504, 549, 550, 607, 632 |
| `src/mesh/commit-outbox.ts` | 141, 188, 195, 207, 213, 249 |
| `src/mesh/state-projector.ts` | 571, 716 |
| `src/providers/mesh-provider.ts` | 263, 275 |
| `src/residency/client.ts` | 1141 |
| `src/residency/host.ts` | 725, 777, 1011 |
| `src/residency/operator-safety.ts` | 41 |
| `src/schema/controller.ts` | 109, 254, 321, 515, 542, 562, 793, 797 |
| `src/state/store.ts` | 531, 572, 605, 811, 840, 851, 1086, 1117, 1130, 1181, 1223, 1265 |
| `src/topology/control-plane.ts` | 758, 999, 1046, 1069, 1079, 1306, 1316, 1345, 1350 |
| `src/topology/host-reaper.ts` | 55, 63, 87, 90, 114, 115, 118, 131, 205 |
| `src/topology/host-record-compaction.ts` | 32 |
| `src/topology/participant-directory.ts` | 974, 975, 977, 980, 981, 984, 995, 1127, 1143, 1147, 1155, 1220, 1236, 1255, 1262, 1264, 1296, 1298, 1299, 1308, 1309, 1310, 1411, 1415, 1550, 1590, 1594, 1602, 1680, 1686, 1687, 1695, 1721, 1741, 1751, 1759, 1797, 1903, 1930, 1939, 2051, 2130, 2156, 2183, 2203, 2208, 2227, 2287, 2319, 2325, 2326, 2342 |
| `src/topology/publication-generation.ts` | 47 |
| `src/topology/root-inbox.ts` | 354 |
| `src/topology/stall-alarms.ts` | 108, 114, 115, 116, 125 |
| `src/ui/controller.ts` | 695, 834, 841, 842, 852, 854, 869 |
| `src/ui/snapshot.ts` | 125 |

`ParticipantDirectory.get/list`, ActorManager indexes, `StateStore.getHead/get`, schema workspace/hypothesis lookup, operator-safety evidence and UI snapshots return synchronously. The bridge fence at `src/mesh/bridge.ts:413` surrounds ownership checks/event append; awaiting KV under local custody is not a substitute. Outbox/reaper callbacks need one transactional view. None is silently redirected to NATS.

## Consistency, fencing and failures

- Completed put/delete returns its PubAck revision. Subsequent awaited get uses leader `STREAM.MSG.GET` (`allow_direct: false`); read-your-writes holds absent an intervening writer. No local cache or optimistic success.
- Server expected-last-subject-sequence CAS gives one winner among stale writers. Revisions include retained tombstones, are bucket/root/key scoped, and are not exact per-key counters or #708 lease tokens. This seam provides **single-key CAS fencing**, not host-owner/lease fencing or multi-key custody.
- `shared/` routing is host-controlled. Existing local `shared/` values are neither migrated nor fallback: migrate separately before opt-in. Remote values cannot shadow host/private namespaces; non-shared keys retain original backend/guards.
- Limited listings pass the clamped page size through the provider to NATS KV. **Exactly one** finite header-only pull examines at most that many candidates: partial-token mismatches, retained tombstones and concurrent deletes all consume the budget, rather than causing refill scans. Raw backend `list` returns the live matches from that examined page, which can be empty or shorter than the limit; emptiness is not proof of absence. The mesh provider instead calls `listPage` and rejects any nonexhausted remote page with public `MeshListingIncompleteError` (`MESH_LISTING_INCOMPLETE`), including mixed local/remote listings. It never returns a silently partial remote result. The error carries the scanned remote `prefix`, clamped `limit`, `examined` and `nextRevision`; callers can increase the bound or explicitly enumerate via the backend page API. Exhausted pages still return the existing array result; no schema/result-shape change, refill scan, fallback or whole-runtime cutover is introduced. For explicit enumeration, `NatsKvStateStore.listPage(prefix, limit, startRevision?)` returns `{entries, examined, nextRevision?}`; pass `nextRevision` back with the same bucket/prefix until it is undefined. Continuations are stream-sequence cursors, not atomic snapshots; concurrent updates can appear again. Exact-key absence requires leader `get`. Slash-terminated prefixes select exactly their encoded namespace (`prefix.>`), never its parent; a partial final token necessarily scans its complete parent and applies decoded `startsWith`. Setup, header iteration and all leader reads share one `timeoutMs` monotonic deadline, throwing public `NatsKvListTimeoutError`; owned cleanup shares one additional `timeoutMs` budget, so total awaited time is bounded by twice the configured timeout. The pinned client's `KV.keys().stop()` does not cancel its background push subscription, so limited listings deliberately do not use that unbounded prefetch. Pages are locale-sorted **within the examined page**, not globally. `listAll` is the deliberately complete/global-sort operation and is not subject to this page budget. Mixed local/remote listings sort selected entries and apply the final public/private limit without scanning all remote keys.
- Disconnect/capacity/timeout errors propagate. Unknown acknowledgements grant no authority; mutations are not automatically replayed. Reconnect re-establishes transport/server eligibility; storage restart retains old fences. Provider close awaits only its owned async handle, never caller-owned MeshStore.
- R3/file/history=1, no TTL/purge/delete; `sync_interval: always` on every member is a deployment prerequisite. One-host loopback R3 is functional evidence, **not** three-host/power-loss qualification. Bucket-destruction/restore-epoch and TLS/ACL/three-host gates remain held.

Round 4 (c6085436137 P2) preserves the provider's array API and makes incomplete examined pages an explicit typed failure. Unit regressions cover shared partial prefixes and mixed listings; live R3 regressions place a matching key after an earlier sibling or retained tombstone, verify it by leader get, require the typed failure at limit 1, and recover the complete listing at limit 2. Raw backend pagination and its bounded wire/deadline behavior remain unchanged.

## Explicit API (no automatic registration)

```ts
import { openNatsMeshProvider } from "pi-fabric/mesh";
const provider = await openNatsMeshProvider(mesh, identity, participants, {
  backend: "nats-kv",
  nats: { servers: trustedServers, experimentalNatsKv: true },
});
// Register this provider explicitly instead of the ordinary MeshProvider, if desired.
// Only shared/* changes authority; state/current and host coordination remain local.
try {
  const written = await provider.invoke("put", { key: "shared/example", value: 1, ifVersion: 0 }, invocation);
  const read = await provider.invoke("get", { key: "shared/example" }, invocation);
} finally { await provider.close(); } // does not close mesh
```

The public factory and provider's `withStateBackend` require both selector and experimental flag. Optional imports use stable build entries (`mesh/state-async.js`, `providers/mesh-provider.js`); ordinary construction neither loads the client nor opens NATS. `NatsKvStateStore.connectionStatus()` exposes transport diagnostics only; a reconnect event never grants state/lease authority.

## Acceptance / reproduction

Caller-staged official `nats-server-v2.14.7-linux-amd64.tar.gz` / `SHA256SUMS`: verify archive SHA256 **before extraction/use**, verify executable against the archive member and require exact `v2.14.7` for this task. Extract only beneath `$TMPDIR`. Runner owns only loopback listeners, uses always-sync R3, retains logs/configs/check evidence under `$TASK_OUT`, and stops/waits children in `finally`.

Initial seam qualification on **intel1, 2026-10-09**: 75 final provider/protocol unit tests; 3 ordinary-provider membership/guard tests; 56 startup/event principal regressions; 6 real R3 backend/provider cases; **46** shared/legacy conformance cases (NATS enabled, no skip), including eight independent NATS processes with 200 unique acknowledged updates and eight provider connections with one stale-CAS winner. The probe restarts **all three** nodes, observes disconnect/reconnect on the same handle, rejects offline get with TIMEOUT, retains revision 1 after reopen, commits revision 2 with CAS, and exercises the **built** public factory successfully. All six server-process incarnations exited 0 and their PIDs were checked absent. No latency/three-host/power-loss/partition admission is claimed.

Round 2 follow-up on **intel1, 2026-10-09**: **89** provider/protocol unit tests, **7** real R3 cases and **46** shared/legacy conformance cases passed. The 10,000-live-key, limit-10 wire probe returned 10 entries with **23 total server-to-client messages** (10 headers, 10 authoritative value replies, 3 consumer API replies), rather than a full keys/value scan; the unit protocol probe asserts exactly 10 header deliveries and 10 leader gets. FILE now uses 5 updates per each of eight independent processes (**40** unique acknowledged updates); SQLite and NATS retain 25 per process (**200**). All fencing/no-lost-update assertions and the READY/start barrier remain; no production lock deadlines changed. Live fixture readiness now waits for fresh stream replication and both current-generation stream/metadata leadership after restart. Earlier qualification attempts exposed a restart-manager TIMEOUT and a fresh-stream publish 503; their logs are retained, mutations are never replayed, and the final run passed reconnect/reopen/built-public-API probes. This is Linux/local-R3 evidence only: the reported Windows CI job still needs a Windows rerun, and the prototype/production hold is unchanged.

Round 3 repair bounds **examined candidates**, not just returned matches. The original `list('shared/missing', 1)` unit regression examined all **10,000** siblings; the repaired call examines one candidate and returns empty without a leader value read. The local R3 wire probe sees **4** total server-to-client messages for that partial prefix and **3** for the exact slash-aligned `shared/missing/` namespace (zero examined keys). Sparse-prefix and tombstone-only pages no longer refill; explicit `listPage` stream-revision continuations recover later matches. A deterministic monotonic-clock test spends 120ms across two successful leader reads and proves the shared 100ms budget rejects even already-resolved RPCs. Public `NatsKvListTimeoutError` and bounded cleanup prevent sequential per-fetch timeouts from multiplying the operation deadline. The held/default-off seam, raw partial-prefix contract, full `listAll` operation, local typed/host authority, no-production-admission boundary and single-host R3 qualification remain unchanged.

Round 4 qualification on **intel1, 2026-10-09**: **109** targeted provider/protocol/ordinary-provider unit cases, project typecheck, standalone runner/probe typecheck, fresh build and **10** real R3 cases plus **46** shared/legacy conformance cases passed. The three pre-fix provider regressions resolved to empty/local-only arrays instead of rejecting; after repair they raise the public typed error. The built public factory also raises that exported error, and backend continuation from the first empty page reaches the later `shared/match` key. All-node restart/reconnect, persisted CAS fences and shutdown passed. The first live attempt hit an existing startup `JetStreamManager` TIMEOUT: one node reported metadata leadership while another had not yet learned it. The runner now waits on **every endpoint's** current-generation metadata-election event before first use and after restart; no production timeout or unknown-ack mutation retry changed. The failed attempt is retained. Six final-run and three first-attempt server incarnations exited 0 and their PIDs were checked absent. One-host R3 remains functional evidence only; the U2/default-off/production holds are unchanged.

Project typecheck, a separate typecheck of the normally excluded runner/probe scripts, fresh `bun run build` (proof artifact + declarations + native Landlock + artifact/lazy-startup assertions), and `bun run assert:lazy-graph` passed. The stock legacy conformance log has a non-failing MaxListeners warning; it is retained, not suppressed. The first build failed because intel1 lacks `cc`; seven checksum-verified **public Ubuntu compiler packages** were extracted only under `$TMPDIR`, then the unchanged build ran with per-command `CC`/library wrapper. No system compiler install, alternate NATS release, credentials or remote host was used.

```sh
bun run typecheck
bunx vitest run tests/mesh-provider-async-state.test.ts tests/mesh-state-nats-kv.test.ts
# Use the installed local compiler, or a private scratch CC wrapper when needed:
bun run build
bun run assert:lazy-graph
FABRIC_NATS_EVIDENCE_DIR="$TASK_OUT/live-r3" \
  bun scripts/run-nats-kv-conformance.ts \
  "$TMPDIR/nats-server/nats-server-v2.14.7-linux-amd64/nats-server" \
  /home/paul/lanes/nats-release/nats-server-v2.14.7-linux-amd64.tar.gz \
  /home/paul/lanes/nats-release/SHA256SUMS --async-seam
```

`--async-seam` requires the fresh build and substitutes restart/public-entry probes for the unrelated latency benchmark. Exact logs/official release verification/topology/fencing/reconnect/shutdown evidence and the head receipt are retained under `$TASK_OUT`. No push, GitHub, credentials, services or fleet backend mutation.
