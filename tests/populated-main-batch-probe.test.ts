// Trust-boundary/data-safety tests. Native cohort integration is a separate,
// caller-gated proof. The frozen observer is used only as a pure rank adapter.
import { afterEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import { DatabaseSync, StatementSync } from "node:sqlite";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";
import type { MeshBatchOperation, MeshIdentity } from "../src/mesh/store.js";
// @ts-expect-error Source-only diagnostic modules do not publish declarations.
import { parseArguments, populationPlan, semanticPopulation, actorRegistryRows, trafficSchedule, prepareOutput, privateDirectory, freshJson, readDependencyGate, eventTarget, selectCandidates, assertCommonWindow, validateChildExit, requireReady, digest, canonicalDigest, databaseIdentity, writeActorRegistries } from "../scripts/populated-main-fixture.mjs";
// @ts-expect-error Source-only diagnostic modules do not publish declarations.
import { Deadline, PhaseClock, makeRanks, assertObserverContract, counterTotal, attributeHistoricalReputs, installHistoricalAudit, ownRootBoundary, ownRootProgress } from "../scripts/probe-populated-main-batches.mjs";

// @ts-expect-error Frozen diagnostic helper has no declaration artifact.
import { aggregateSites as frozenSites, aggregateTransactions as frozenTransactions, captureTransactions } from "../scripts/main-batch-observer.mjs";

type Row = Record<string, any>;
const temporary: string[] = [];
const cleanups: Array<() => void> = [];
const scratch = () => {
  if (!process.env.TMPDIR) throw new Error("private TMPDIR required for probe tests");
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR, "cohort-pure-test-"));
  fs.chmodSync(root, 0o700); temporary.push(root); return root;
};
afterEach(() => { vi.useRealTimers(); for (const close of cleanups.splice(0).reverse()) close(); vi.restoreAllMocks(); syncBuiltinESMExports();
  for (const root of temporary.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const args = ["--profile", "profile.json", "--import-root", "candidate", "--pi-cli", "cli.js", "--out", "result.json"];
const profile = () => ({
  version: 2, backend: "sqlite", journalMode: "wal",
  requestedWorkload: { mainProcesses: 3, residentProcesses: 1, idleSeconds: 600, trafficSeconds: 600 },
  operationalPopulationPlan: { activeIdleActors: 14, mainActorCounts: [4, 4, 4], residentActorCounts: [2],
    remainingActorStatusesAreHistoricalNotLive: true, networkAndPaidInference: false },
  tables: { kv: { prefixes: [
    { prefix: "actors", rows: 30, valueBytes: 50000 }, { prefix: "topology", rows: 40, valueBytes: 50000 },
    { prefix: "sessions", rows: 8, valueBytes: 8000 }, { prefix: "residency", rows: 5, valueBytes: 8000 },
  ] } },
  semanticSnapshot: { shapes: [
    { shape: "actors/<id>/<id>", rows: 20, fields: { "status=idle": 14, "status=stopped": 4, "status=running": 2 } },
    { shape: "actors/realarm/<id>/<x>", rows: 6, fields: { "state=published": 4, "state=pending": 2 } },
    { shape: "actors/realarm/<id>", rows: 2, fields: { "state=published": 1, "state=pending": 1 } },
    { shape: "topology/participants/<id>", rows: 12, fields: { "status=idle": 6, "status=stopped": 6, "kind=root": 4, "kind=actor": 6, "kind=agent": 2 } },
    { shape: "topology/hosts/<id>", rows: 7, fields: {} }, { shape: "topology/inbox/<id>", rows: 3, fields: {} },
    { shape: "topology/root-absence/<id>", rows: 2, fields: {} }, { shape: "topology/control-seen/<id>", rows: 1, fields: {} },
    { shape: "topology/subscriptions/<id>", rows: 2, fields: {} },
    { shape: "residency/deliveries/<id>/<id>", rows: 5, fields: {} },
    { shape: "sessions/<id>", rows: 8, fields: { "status=idle": 6, "status=running": 2 } },
  ], recentChangesByShape: [{ shape: "actors/<id>/<id>", rows: 16 }, { shape: "topology/inbox/<id>", rows: 2 }] },
});
const planFor = (input = profile()) => populationPlan(input, { mains: 2, residents: 1, idleSeconds: 1, trafficSeconds: 6 });
const ownersFor = (plan: Row) => plan.owners.map((owner: Row, index: number) => ({ ...owner, sessionId: `native-${index}`,
  rootId: `session:native-${index}`, sessionFile: `private-journal-${index}` }));

const author: MeshIdentity = { id: "synthetic-main", name: "Synthetic Main", kind: "main" };
const participantKey = `topology/participants/${"a".repeat(64)}`;
const actorKey = `actors/native/${"b".repeat(32)}`;
const attributedExample = () => {
  const db = { file: "owned/state.db", device: "1", inode: "2" }, value = '{"a":1}', updatedBy = JSON.stringify(author);
  const hashes = { payloadSha256: digest(value), payloadCanonicalSha256: canonicalDigest(JSON.parse(value)),
    updatedByCanonicalSha256: canonicalDigest(author) };
  const record: Row = { key: participantKey, value, updatedBy, ...hashes, priorDigests: hashes,
    prior: { present: true, value, updatedBy, version: 8 }, beforeEvidence: "same-receiver-native-read-in-writer", rawValueEqual: true,
    version: 9, metadataUpdatedAt: 5000, label: "M00", role: "measuredMain", phase: "idle", pid: 101,
    at: 100, monotonicMs: 20, endedMonotonicMs: 21, receiverId: 1, databaseIdentity: db,
    transaction: { id: 1, writer: true, tainted: false, status: "complete-commit", beginStartedMonotonicMs: 11,
      beginEndedMonotonicMs: 13, releaseStartedMonotonicMs: 24, releaseEndedMonotonicMs: 26 } };
  const capture: Row = { calls: [{ id: 1, pid: 101, phase: "idle", monotonicMs: 10, endedMonotonicMs: 30,
    callSite: "native-site", activity: "background", storeRoot: "owned", storeId: 2, ops: [{ key: record.key, kind: "put" }], prepared: [], inputOpsCount: 1,
    results: [{ key: record.key, version: 9, applied: true }] }], transactions: [{ pid: 101, batchId: 1, writer: true,
    verb: "BEGIN IMMEDIATE", acquiredMonotonicMs: 12, releaseMonotonicMs: 25, database: { ...db, connectionId: 2 }, succeeded: true, holdStatus: "complete-commit" }] };
  return { db, record, capture };
};

// Real production SQLite prepare/#slot/#applyPut path plus the frozen SQL seam;
// no Pi Main/ResidentHost or child process is launched. Keep: data-safety boundary.
const auditedStore = async (limits = {}) => {
  const root = scratch(), mesh = path.join(root, "mesh"); fs.mkdirSync(mesh, { mode: 0o700 });
  const initial = await SqliteStateStore.open(mesh, 65536, 1000, { checkpoint: "client", checkpointLog: "off" });
  await initial.put({ key: participantKey, value: { a: 1, b: 2 }, identity: author });
  await initial.put({ key: actorKey, value: { a: 1, b: 2 }, identity: author }); initial.close();
  const db = databaseIdentity(path.join(mesh, "state.db"));
  const calls: Row[] = [], transactions: Row[] = []; let current: Row | undefined, phase = "idle";
  const restore = captureTransactions(db, () => ({ pid: process.pid, label: "M00", role: "measuredMain", phase, batchId: current?.id ?? null }),
    (row: Row) => transactions.push(row));
  const audit = installHistoricalAudit({ directory: root, sharedDatabase: db, owner: { label: "M00", role: "measuredMain" } }, () => phase, limits);
  let store: SqliteStateStore | undefined;
  cleanups.push(() => { phase = "drain"; try { store?.close(); } finally { try { audit.close(); } finally { restore(); } } });
  store = await SqliteStateStore.open(mesh, 65536, 1000, { checkpoint: "client", checkpointLog: "off" });
  const describe = (ops: MeshBatchOperation[]) => ops.map(op => ({ kind: op.kind, key: op.key }));
  const batch = async (input: Parameters<SqliteStateStore["writeBatch"]>[0]) => {
    const call: Row = { id: calls.length + 1, pid: process.pid, phase, role: "measuredMain", label: "M00", storeRoot: mesh, storeId: 1,
      monotonicMs: performance.now(), callSite: "real-production-sqlite-test", activity: "background", inputOpsCount: input.ops.length,
      ops: describe(input.ops), prepared: [], preparedOpsCount: 0 };
    calls.push(call); current = call;
    try {
      const results = await store!.writeBatch({ ...input, ...(input.prepare ? { prepare: view => {
        const result = input.prepare!(view); call.prepared.push(describe(result)); call.preparedOpsCount += result.length; return result;
      } } : {}) }); call.results = results; return results;
    } finally { call.endedMonotonicMs = performance.now(); current = undefined; }
  };
  const finish = () => {
    phase = "post-idle"; audit.flush();
    const records = fs.readFileSync(path.join(root, "historical-sql.jsonl"), "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line));
    const capture = { calls, transactions };
    return { records, capture, result: attributeHistoricalReputs(records, [{ label: "M00", capture }], { idle: { overlapMs: 1000 } }, [], db, [audit.quality()]) };
  };
  return { root, mesh, db, store, audit, batch, finish, setPhase: (value: string) => { phase = value; } };
};

describe("cohort arguments and private evidence admission", () => {
  it("requires named inputs; refuses duplicate, ambiguous, unknown and unbounded arguments", () => {
    expect(parseArguments(args).candidate).toBe(path.resolve("candidate"));
    expect(() => parseArguments(args.concat("--out", "again.json"))).toThrow(/duplicate/);
    expect(() => parseArguments(args.concat("--root", "existing-mesh"))).toThrow(/unknown/);
    expect(() => parseArguments(args.concat("--idle-seconds", "Infinity"))).toThrow(/invalid/);
    expect(() => parseArguments(args.slice(0, -1))).toThrow(/missing/);
    // LEAD-ANSWER-1318Z explicitly superseded the old-Main scope.
    expect(() => parseArguments(args.concat("--legacy-main-count", "2"))).toThrow(/OLD_MAIN_SCOPE_SUPERSEDED/);
    expect(() => parseArguments(args.concat("--legacy-import-root", "older"))).toThrow(/OLD_MAIN_SCOPE_SUPERSEDED/);
  });
  it("reserves all companions privately, refuses reuse and never chmods the parent", () => {
    const root = scratch(), out = path.join(root, "result.json");
    const output = prepareOutput(out, root);
    output.write("summary", { incomplete: true }); output.close();
    expect(fs.statSync(out).mode & 0o777).toBe(0o600);
    expect(() => prepareOutput(out, root)).toThrow(/fresh/);
    fs.chmodSync(root, 0o750);
    expect(() => privateDirectory(root)).toThrow(/0700/);
    expect(fs.statSync(root).mode & 0o777).toBe(0o750);
  });
  it.skipIf(process.platform === "win32")("refuses symlink evidence, symlink ancestors and output escapes before reserving", () => {
    const root = scratch(), other = scratch();
    fs.symlinkSync(other, path.join(root, "alias"));
    expect(() => prepareOutput(path.join(root, "alias", "result.json"), root)).toThrow(/symlink/);
    expect(() => prepareOutput(path.join(other, "result.json"), root)).toThrow(/beneath/);
    fs.symlinkSync(path.join(other, "missing"), path.join(root, "result.ranks.json"));
    expect(() => prepareOutput(path.join(root, "result.json"), root)).toThrow(/fresh/);
    expect(fs.existsSync(path.join(root, "result.json"))).toBe(false);
  });
  it("requires an explicit approved byte-pin gate, not the existence of sibling files", () => {
    expect(() => readDependencyGate(undefined)).toThrow(/DEPENDENCY_GATE_REQUIRED/);
    const root = scratch(), gate = path.join(root, "gate.json");
    freshJson(gate, { schema: 1, approved: false, interface: "populated-main-fixed-v1" });
    expect(() => readDependencyGate(gate)).toThrow();
  });
});

describe("input-derived semantic population", () => {
  it("balances allocations, keeps historical statuses nonoperational and rejects inconsistent marginals", () => {
    const input = profile(), plan = planFor(input);
    expect(plan.actors).toBe(10);
    expect(plan.historicalActorStatuses).toEqual({ idle: 4, stopped: 4, running: 2 });
    expect(plan.owners.map((owner: Row) => owner.actors)).toEqual([4, 4, 2]);
    input.operationalPopulationPlan.mainActorCounts[0] = input.operationalPopulationPlan.mainActorCounts[0]! + 1;
    expect(() => planFor(input)).toThrow();
    expect(() => populationPlan(profile(), { mains: 4 })).toThrow(/selected Mains/);
  });
  it("selects only current candidates without changing counts/runtime fields; superseded old-Main input is refused", () => {
    const plan = planFor(), owners = ownersFor(plan), selected = selectCandidates(owners, "current");
    expect(selected).toHaveLength(3);
    expect(selected.map((row: Row) => [row.label, row.candidate])).toEqual([["M00", "current"], ["M01", "current"], ["R00", "current"]]);
    expect(selected.some((row: Row) => row.legacy)).toBe(false);
    expect(owners.some((row: Row) => "livenessLeaseFiles" in row)).toBe(false);
    expect(() => selectCandidates(owners, "current", "older", 1)).toThrow(/OLD_MAIN_SCOPE_SUPERSEDED/);
  });
  it("uses supported inert registry input rows and never creates historical runnable registries, grants or pending inbox work", () => {
    const input = profile(), plan = planFor(input), owners = ownersFor(plan);
    const population = semanticPopulation(input, plan, owners, "synthetic-project", 100000000000);
    expect(population.actors).toHaveLength(10);
    expect(population.overlays.filter((row: Row) => row.key.startsWith("actors/realarm/"))).toHaveLength(8);
    expect(population.overlays.filter((row: Row) => /^actors\/(?!realarm)/.test(row.key))).toHaveLength(20);
    for (const actor of actorRegistryRows(owners, "synthetic-project", 100)) {
      expect(actor.id).toMatch(/^[a-f0-9]{32}$/); expect(actor.events).toEqual([]); expect(actor.status).toBe("idle");
      expect(actor.triggerTurn).toBe(false); expect(actor.messageHistory).toBeUndefined(); expect(actor.messages).toEqual([]);
      expect(actor.activationFilter[0].where).toEqual([{ path: "data.syntheticCohort", equals: true }]);
    }
    for (const row of population.overlays.filter((row: Row) => row.key.startsWith("topology/hosts/"))) {
      expect(row.value.expiresAt).toBeLessThan(100000000000);
      expect(row.value.incarnationToken).toBeUndefined(); expect(row.value.livenessLeaseFiles).toBeUndefined();
    }
    for (const row of population.overlays.filter((row: Row) => row.key.startsWith("topology/inbox/"))) {
      expect(row.key).toMatch(/^topology\/inbox\/[a-f0-9]{32}$/); expect(row.value.pending).toBeUndefined();
      expect(Array.isArray(row.value.deliveredAt)).toBe(true);
    }
    expect(population.overlays.some((row: Row) => /subscriptions|grants|queued/.test(row.key))).toBe(false);
    expect(population.overlays.filter((row: Row) => row.pad).every((row: Row) => !population.actors.some((actor: Row) => actor.id === row.value.id))).toBe(true);
  });
  it("consumes oversized filler slots as explicit inert archive budget rows, never padded historical participant targets", () => {
    const input = profile(); input.tables.kv.prefixes[3]!.rows = 6; input.tables.kv.prefixes[3]!.valueBytes = 250000;
    const plan = planFor(input), result = semanticPopulation(input, plan, ownersFor(plan), "synthetic-project");
    expect(result.manifest.budgetBallast).toEqual([{ prefix: "residency", rows: 1, reason: "avoid oversized filler; permit archive-only padding" }]);
    const ballast = result.overlays.find((row: Row) => row.key === "residency/fixture-budget-0");
    expect(ballast.pad).toBe(true); expect(ballast.value.fixtureArchive.executable).toBe(false);
    expect(result.overlays.filter((row: Row) => row.key.startsWith("residency/deliveries/")).every((row: Row) => row.pad)).toBe(true);
  });
  it("rejects budget overflow and needs explicit exact event targets rather than rounded physical estimates", () => {
    const input = profile(); input.tables.kv.prefixes[0]!.rows = 1;
    expect(() => semanticPopulation(input, planFor(input), ownersFor(planFor(input)), "synthetic-project")).toThrow(/exceed/);
    expect(() => eventTarget(profile())).toThrow(/EVENT_PROFILE_REQUIRED/);
    expect(eventTarget({ eventLog: { lines: 100, bytes: 100000 } })).toEqual({ lines: 100, bytes: 100000, lineBytes: 1000 });
    expect(eventTarget({ eventLogTarget: { sourceLines: 100, eventBytes: 100000, eventLineBytes: 1240 } })).toEqual({ lines: 100, bytes: 100000, lineBytes: 1240 });
  });
  it("has bounded nonoverlapping light slots, real API action types, explicit correlations and a quiet final interval", () => {
    const plan = planFor(), schedule = trafficSchedule(plan, ownersFor(plan));
    expect(schedule.length).toBeGreaterThan(0);
    const actions = schedule.flatMap((slot: Row) => slot.actions);
    expect(new Set(actions.map((action: Row) => action.correlationId)).size).toBe(actions.length);
    expect(actions.some((action: Row) => action.action === "history")).toBe(true);
    expect(actions.some((action: Row) => action.action === "inbox")).toBe(true);
    expect(actions.some((action: Row) => action.action === "followUp")).toBe(true);
    for (let i = 1; i < schedule.length; i++) expect(schedule[i - 1].deadlineOffsetMs).toBeLessThan(schedule[i].offsetMs);
    expect(schedule.at(-1).offsetMs).toBeLessThan(plan.trafficSeconds * 1000 - 1000);
  });
});

describe("historical missing-capability attribution is not synthetic live ownership", () => {
  it("retains only the two known historical cases without inventing 23 names, current owner bindings or a branch success", () => {
    const input: Row = profile(); input.version = 3;
    input.historicalMissingCapabilityRoots = { count: 2, fieldCount: 23, payloadStartedAtAgeHours: [40, 25], payloadUpdatedAtAgeHours: 23, kvUpdatedAtAgeSeconds: [27, 65] };
    const plan = planFor(input as ReturnType<typeof profile>), owners = ownersFor(plan);
    const result = semanticPopulation(input, plan, owners, "synthetic-project", 1000000000000, { id: "synthetic-test-seedwriter", name: "Test seed writer", kind: "agent" });
    expect(result.manifest.historicalTargets).toHaveLength(2);
    for (const target of result.manifest.historicalTargets) {
      const value = result.overlays.find((row: Row) => row.key === target.key).value;
      expect(value.kind).toBe("root"); expect(value.status).toBe("idle"); expect(value.role).toBeUndefined();
      expect(value.ownerHostId).toMatch(/^unmatched-historical-host:/); expect(value.ownerIdentityId).toBe("synthetic-test-seedwriter");
      expect(target.key).toBe(`topology/participants/${digest(value.id)}`);
      expect(target.seedWriterEqualityExpected).toBe(true); expect(target.parserAcceptance).toMatch(/must be observed/);
      expect(value.livenessLeaseFiles).toBeUndefined(); expect(value.updatedAt).toBe(1000000000000 - 23 * 3600000);
      expect(target.schemaExact).toBe(false); expect(target.actualFieldCount).toBe(14);
    }
    const targets = new Set(result.manifest.historicalTargets.map((row: Row) => row.key));
    for (const row of result.overlays.filter((row: Row) => row.key.startsWith("topology/participants/") && !targets.has(row.key))) expect(row.value.livenessLeaseFiles).toBe(1);
  });
  it("requires an applied-result version plus completed SQL commit, and preserves rate/site/activity, metadata and unchanged payload evidence", () => {
    const { record, capture, db } = attributedExample();
    const target = { key: record.key, payloadSha256: record.payloadSha256 };
    const result = attributeHistoricalReputs([record], [{ label: "M00", capture }], { idle: { overlapMs: 2000 } }, [target], db);
    expect(result.events[0].classification).toBe("committed-upsert"); expect(result.events[0].immediateSite).toBe("native-site");
    expect(result.events[0].payloadUnchangedFromSeed).toBe(true); expect(result.events[0].metadataUpdatedAt).toBe(5000);
    expect(result.events[0].proof).toBe("unchanged-value-and-author");
    expect(result.partitions[0].perSecond).toBe(0.5); expect(result.reproductionClaim).toBe(false);
    capture.transactions[0]!.holdStatus = "complete-rollback";
    expect(attributeHistoricalReputs([record], [{ label: "M00", capture }], {}, [target], db).events[0].classification).toBe("uncommitted-or-uncorrelated-statement");
  });
});

describe("common barrier, custody and phase safety", () => {
  const window = (start: number, end: number) => ({ start: { at: start, monotonicMs: start }, end: { at: end, monotonicMs: end } });
  it("requires full common overlap rather than adding individual durations", () => {
    const receipts = [{ label: "M00", windows: { idle: window(100, 1200) } }, { label: "R00", windows: { idle: window(200, 1300) } }];
    expect(assertCommonWindow(receipts, "idle", 1, ["M00", "R00"]).overlapMs).toBe(1000);
    receipts[1]!.windows.idle = window(300, 1400);
    expect(() => assertCommonWindow(receipts, "idle", 1, ["M00", "R00"])).toThrow(/common overlap/);
    expect(() => assertCommonWindow(receipts.slice(1), "idle", 1, ["M00", "R00"])).toThrow();
  });
  it("refuses duplicate/out-of-order arm, snapshots within windows, and closes all phase timers", () => {
    vi.useFakeTimers(); let now = 1000; const observer = { setPhase: vi.fn() };
    const clock = new PhaseClock(observer, () => {}, () => now, () => now);
    expect(() => clock.arm({ phase: "traffic", startAt: 1200, endAt: 2300 })).toThrow(/order/);
    clock.arm({ phase: "idle", startAt: 1200, endAt: 2300 });
    expect(() => clock.arm({ phase: "idle", startAt: 1300, endAt: 2300 })).toThrow(/order/);
    now = 1200; vi.advanceTimersByTime(200);
    expect(() => clock.outsideWindow()).toThrow(/snapshot inside/);
    now = 2300; vi.advanceTimersByTime(1100); expect(clock.phase).toBe("post-idle");
    clock.close(); expect(vi.getTimerCount()).toBe(0);
  });
  it("treats any child exit before termination as failure and escalation as unclean", () => {
    const exit = { label: "M00", pid: 12345, code: 0, signal: null };
    expect(() => validateChildExit(exit, false)).toThrow(/before termination/);
    expect(() => validateChildExit(exit, true)).not.toThrow();
    expect(() => validateChildExit({ ...exit, code: null, signal: "SIGTERM" }, true)).toThrow(/cleanly/);
  });
  it("requires all distinct native identities, real publications and the same database", () => {
    const plan = planFor(), owners = ownersFor(plan), db = { file: "owned/state.db", device: "1", inode: "2" };
    expect(() => requireReady([], owners, db)).toThrow(/all native/);
    const rows = owners.map((owner: Row, index: number) => ({ label: owner.label, pid: index + 100, ready: true,
      native: { sessionId: owner.sessionId, rootId: owner.rootId, trusted: true, mode: "rpc", sessionFile: owner.sessionFile },
      actors: Array.from({ length: owner.actors }, () => ({ rootId: owner.rootId, status: "idle" })),
      publications: Array.from({ length: owner.actors + 1 }, () => ({ ownerHostId: "native-owner", ownerIdentityId: "native-identity", stale: false })),
      stores: [{ backend: "sqlite", databaseIdentity: db }], canConsumeMesh: true, noWakeConfig: true }));
    expect(requireReady(rows, owners, db)).toBe(true);
    rows[0].publications[0].stale = true;
    expect(() => requireReady(rows, owners, db)).toThrow(/current native lease/);
  });
  it("has one global deadline which rejects active work rather than silently extending", async () => {
    const deadline = new Deadline(10);
    await expect(deadline.within(new Promise(() => {}), 100, "unfinished")).rejects.toThrow(/deadline|expired/);
    expect(deadline.controller.signal.aborted).toBe(true); deadline.close();
  });
});

describe("role/activity/store attribution without double-counting custody", () => {
  const db = { file: "owned/state.db", device: "1", inode: "2" };
  const capture = (activity = "background") => ({ calls: [{ id: 1, phase: "idle", activity, store: { root: "owned" } }],
    transactions: [{ batchId: 1, phase: "idle", activity, database: { ...db, scope: "mesh-state" }, holdMs: 4 }],
    locks: [{ batchId: 1, phase: "idle", activity, holdMs: 4 }], dropped: 0, observerErrors: {} });
  const aggregateSites = (value: Row, phase: string) => [{ count: value.calls.filter((row: Row) => row.phase === phase).length }];
  const aggregateTransactions = (value: Row, phase: string) => [{ holdMs: value.transactions.filter((row: Row) => row.phase === phase).reduce((n: number, row: Row) => n + row.holdMs, 0) }];
  it("retains M00, resident and driver partitions, namespaces IDs, and counts SQL custody only once", () => {
    const captures = [{ label: "M00", role: "measuredMain", capture: capture() },
      { label: "M01", role: "peerMain", capture: capture("fixture-driver") }, { label: "R00", role: "residentHost", capture: capture() }];
    assertObserverContract(captures, db);
    const ranks = makeRanks(captures, aggregateSites, aggregateTransactions, db);
    expect(ranks.incomplete).toBe(false); expect(ranks.measuredMain).toHaveLength(1);
    expect(ranks.partitions.some((row: Row) => row.label === "M01" && row.activity === "fixture-driver")).toBe(true);
    expect(ranks.sharedDatabaseRank.idle.transactions[0].holdMs).toBe(12);
    expect(ranks.sharedDatabaseRank.idle.sites[0].count).toBe(3);
    expect(digest("synthetic")).toHaveLength(64);
  });
  it("adapts the actual frozen helper schema, retaining PID-disambiguated batch IDs and exact SQL custody", () => {
    const raw = capture();
    Object.assign(raw.calls[0]!, { pid: 101, role: "measuredMain", label: "M00", storeRoot: "owned", storeId: 1,
      ops: [], prepared: [], results: [], hookRuns: [], hooks: {}, stack: [], callSite: "production-immediate" });
    Object.assign(raw.transactions[0]!, { pid: 101, role: "measuredMain", label: "M00", verb: "BEGIN IMMEDIATE", succeeded: true,
      writer: true, holdStatus: "complete-commit", acquiredMonotonicMs: 10, releaseMonotonicMs: 14,
      database: { ...db, scope: "mesh-state", connectionId: 1 }, storeRoot: "owned", storeId: 1, stack: [] });
    raw.locks = [];
    const ranks = makeRanks([{ label: "M00", role: "measuredMain", capture: raw }], frozenSites, frozenTransactions, db);
    expect(ranks.sharedDatabaseRank.idle.sites[0].transactions).toBe(1);
    expect(ranks.sharedDatabaseRank.idle.sites[0].sqlHoldMs).toBe(4);
    expect(ranks.sharedDatabaseRank.idle.transactions[0].holdMs).toBe(4);
  });
  it("does not claim complete ranks on auxiliary/unknown records, drops or observer errors", () => {
    const raw = capture(); (raw.calls[0] as Row).store = {};
    const records = [{ label: "M00", role: "measuredMain", capture: raw }];
    expect(() => assertObserverContract(records, db)).toThrow(/field adapter|identify/);
    expect(makeRanks(records, aggregateSites, aggregateTransactions, db).incomplete).toBe(true);
    expect(counterTotal({ calls: 2, transactions: 3 })).toBe(5);
    raw.dropped = 1;
    expect(() => assertObserverContract(records, db)).toThrow();
  });
});

describe("recorded Resident codec failure and static observed history", () => {
  it("loads/saves actual seeded format-1 registries through the real codec, including legitimate durable idle actors and filtered history", async () => {
    const mesh = scratch(), owners = ownersFor(planFor()), actors = actorRegistryRows(owners, "synthetic-project", 100);
    writeActorRegistries(mesh, owners, actors);
    for (const owner of owners) {
      const root = path.join(mesh, "actors", owner.sessionId), store = new ActorRegistryStore(root);
      expect(store.records()).toHaveLength(owner.actors);
      const row = store.records()[0]!;
      // The original native failure is red through the ACTUAL compactor, not a shape assertion.
      expect(() => store.prepare([{ ...row, updatedAt: 101, messageHistory: [] }])).toThrow("Invalid actor message history reference");
      await store.update(rows => ({ actors: rows.map(item => ({ ...item, updatedAt: 102 })), value: true }));
      const message = { id: `skip-${owner.label}`, source: "mesh", direction: "in", createdAt: 103, text: "filtered synthetic event", reason: "filtered: cohort-skip" };
      await store.update(rows => ({ actors: rows.map(item => item.id === row.id ? { ...item, messages: [message], registryMessageAppend: [message], filteredCount: 1 } : item), value: true }));
      const fresh = new ActorRegistryStore(root), loaded = fresh.records().find(item => item.id === row.id)!;
      expect(fresh.messages(loaded)).toEqual([message]); expect(fresh.messageCount(loaded)).toBe(1);
      expect(loaded.status).toBe("idle"); expect(loaded.triggerTurn).toBe(false);
      expect(loaded.residency).toBe(owner.role === "residentHost" ? "durable" : "session");
    }
  });
  it.each([4, 5])("consumes full v%i profile shape into exactly 23 static fields and distinct per-row Main authors, never live ownership", version => {
    const fields = Object.fromEntries(["capabilities:list", "controlProtocol:str", "cwd:str", "format:int", "id:str", "interactive:bool", "kind:str", "model:str", "name:str", "ownerHostId:str", "ownerIdentityId:str", "ownerIncarnation:str", "project:str", "projectRoot:str", "remoteHost:str", "rootId:str", "runner:str", "sessionId:str", "startedAt:int", "status:str", "thinking:str", "transport:str", "updatedAt:int"].map(item => item.split(":")));
    const input: Row = profile(); input.version = version;
    input.historicalMissingCapabilityRoots = { count: 2, fieldCount: 23, schemaFields: fields, updatedByKnown: true,
      payloadStartedAtAgeHours: [40, 25], payloadUpdatedAtAgeHours: 23, kvUpdatedAtAgeSeconds: [27, 65] };
    const plan = planFor(input as ReturnType<typeof profile>), result = semanticPopulation(input, plan, ownersFor(plan), "synthetic", 1000000000000);
    const targets = result.manifest.historicalTargets, authors = new Set(); expect(targets).toHaveLength(2);
    for (const target of targets) {
      const overlay = result.overlays.find((row: Row) => row.key === target.key), value = overlay.value;
      expect(Object.keys(value).sort()).toEqual(Object.keys(fields).sort()); expect(Object.keys(value)).toHaveLength(23);
      expect(overlay.identity).toEqual({ id: value.id, name: value.name, kind: "main", sessionId: value.sessionId }); authors.add(overlay.identity.id);
      expect(target.key).toBe(`topology/participants/${digest(value.id)}`); expect(target.key.endsWith(value.id)).toBe(false);
      expect(value.remoteHost).toBeTypeOf("string"); expect(value.ownerHostId).toMatch(/^unmatched-historical-host:/);
      expect(value.ownerIdentityId).toBe(value.id); expect(target.schemaExact).toBe(true); expect(target.staticHistory).toBe(true);
      for (const absent of ["label", "livenessLeaseFiles", "mainBindings", "mainProcess", "pendingMessages", "repository", "expiresAt", "bridgeVia", "role"]) expect(value[absent]).toBeUndefined();
      expect(result.overlays.some((row: Row) => row.key === `sessions/${value.sessionId}`)).toBe(false);
      expect(result.overlays.some((row: Row) => row.key === `topology/hosts/${digest(value.ownerHostId)}`)).toBe(false);
      expect(overlay.pad).toBeUndefined(); expect(overlay.updatedAt).toBeUndefined();
    }
    expect(authors.size).toBe(2);
  });
});

describe("passive exact-database per-commit participant and actor attribution", () => {
  it("uses real #slot reads and prepared operations; keeps SQL COMMIT/site/activity plus complete value/author proof", async () => {
    const run = await auditedStore();
    await run.batch({ identity: author, ops: [{ kind: "put", key: participantKey, value: { a: 1, b: 2 } }],
      prepare: () => [{ kind: "put", key: actorKey, value: { a: 1, b: 2 } }] });
    const { records, result } = run.finish();
    expect(records).toHaveLength(2); expect(result.complete).toBe(true);
    expect(result.events.map((row: Row) => row.proof)).toEqual(["unchanged-value-and-author", "unchanged-value-and-author"]);
    expect(result.events.map((row: Row) => row.operationOrigin)).toEqual(["original", "post-prepare"]);
    expect(result.events.every((row: Row) => row.frozenConnectionId && row.operationPaired && row.matchedSqlHoldId)).toBe(true);
    expect(result.partitions[0].provenUnchangedValueAndAuthor).toBe(2);
    expect(records.every((row: Row) => row.prior.version < row.version && row.prior.updatedAt <= row.metadataUpdatedAt)).toBe(true);
  });
  it("distinguishes changed VALUE, raw-order-only VALUE, and changed complete author despite a stable author id", async () => {
    const run = await auditedStore();
    await run.batch({ identity: author, ops: [{ kind: "put", key: participantKey, value: { b: 2, a: 1 } }] });
    await run.batch({ identity: author, ops: [{ kind: "put", key: participantKey, value: { a: 2, b: 2 } }] });
    await run.batch({ identity: { ...author, name: "Changed complete author" }, ops: [{ kind: "put", key: participantKey, value: { a: 2, b: 2 } }] });
    const { result } = run.finish();
    expect(result.events.map((row: Row) => row.proof)).toEqual(["raw-order-only-value", "changed-value", "changed-author"]);
    expect(result.partitions[0].provenUnchangedValueAndAuthor).toBe(0);
  });
  it("does not turn repeated same-key statements or rollback into unchanged committed writes", async () => {
    const run = await auditedStore();
    await run.batch({ identity: author, ops: [{ kind: "put", key: actorKey, value: { changed: true } }, { kind: "put", key: actorKey, value: { changed: true } }] });
    await expect(run.batch({ identity: author, ops: [{ kind: "put", key: participantKey, value: { a: 1, b: 2 } }],
      beforeCommit: () => { throw new Error("genuine precommit refusal"); } })).rejects.toThrow("genuine precommit refusal");
    const { result } = run.finish();
    expect(result.events.slice(0, 2).map((row: Row) => row.proof)).toEqual(["UNKNOWN", "UNKNOWN"]);
    expect(result.events[2].classification).toBe("rolled-back-statement"); expect(result.events[2].proof).toBe("UNKNOWN");
    expect(result.committedRankComplete).toBe(true); expect(result.unchangedRankComplete).toBe(false);
    expect(result.partitions[0].provenUnchangedValueAndAuthor).toBe(0);
  });
  it("a competing unobserved same-file connection can change state between commits: prior process-local writes are not pre-state", async () => {
    const run = await auditedStore();
    await run.batch({ identity: author, ops: [{ kind: "put", key: actorKey, value: { a: 1, b: 2 } }] });
    // Another real production writer updates revision/high-water/feed together.
    // Its caller is deliberately outside this test's batch-call capture.
    const other = await SqliteStateStore.open(run.mesh, 65536, 1000, { checkpoint: "client", checkpointLog: "off" });
    try { await other.put({ key: actorKey, value: { competingWriter: true }, identity: author }); }
    finally { other.close(); }
    await run.batch({ identity: author, ops: [{ kind: "put", key: actorKey, value: { a: 1, b: 2 } }] });
    const { result } = run.finish();
    expect(result.events[0].proof).toBe("unchanged-value-and-author");
    expect(result.events[1].classification).toBe("uncommitted-or-uncorrelated-statement");
    expect(result.events[2].proof).toBe("changed-value"); expect(result.events[2].prior.value).toBe('{"competingWriter":true}');
  });
  it("same-key auxiliary DB SQL is excluded and a different same-file connection's read cannot supply pre-state", async () => {
    const run = await auditedStore(), foreign = new DatabaseSync(path.join(run.root, "auxiliary.db")), peer = new DatabaseSync(run.db.file);
    const sql = "INSERT INTO kv(key, value, version, updated_at, updated_by, bytes) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, version = excluded.version, updated_at = excluded.updated_at, updated_by = excluded.updated_by, bytes = excluded.bytes";
    try {
      foreign.exec("CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT, version INTEGER, updated_at INTEGER, updated_by TEXT, bytes INTEGER)");
      const put = foreign.prepare(sql); foreign.exec("BEGIN IMMEDIATE"); put.run(actorKey, "{}", 1, 1, JSON.stringify(author), 1); foreign.exec("COMMIT");
      peer.prepare("SELECT key, value, version, updated_at, updated_by, bytes FROM kv WHERE key = ?").get(actorKey);
      peer.exec("BEGIN IMMEDIATE"); peer.prepare(sql).run(actorKey, '{"a":1,"b":2}', 999, 1, JSON.stringify(author), 1); peer.exec("COMMIT");
    } finally { foreign.close(); peer.close(); }
    const { records, result } = run.finish();
    expect(records).toHaveLength(1); expect(records[0].beforeEvidence).toBe("UNKNOWN");
    expect(result.events[0].classification).toBe("uncommitted-or-uncorrelated-statement");
    expect(run.audit.quality().foreignStatements).toBeGreaterThan(0); expect(result.partitions[0].provenUnchangedValueAndAuthor).toBe(0);
  });
  it("does not hash/canonicalize/write evidence or issue extra SQL under native writer custody", async () => {
    const run = await auditedStore(), nativeRun = StatementSync.prototype.run, nativeGet = StatementSync.prototype.get;
    const nativeExec = DatabaseSync.prototype.exec, sql: string[] = []; let holding = false;
    const exec = vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, text: string) {
      sql.push(text); if (/^BEGIN IMMEDIATE$/.test(text)) holding = true;
      try { return nativeExec.call(this, text); } finally { if (/^(COMMIT|ROLLBACK)$/.test(text)) holding = false; }
    });
    const writes = vi.spyOn(fs, "writeSync"), nativeHash = crypto.createHash; let heldHashes = 0;
    const hashes = vi.spyOn(crypto, "createHash").mockImplementation((...args: Parameters<typeof crypto.createHash>) => {
      if (holding) heldHashes++; return nativeHash(...args);
    }); syncBuiltinESMExports();
    // Production writes themselves may do I/O; the observer's fd is independently
    // empty until explicit outside-window flush, even after an actual COMMIT.
    let reads = 0, upserts = 0;
    const get = vi.spyOn(StatementSync.prototype, "get").mockImplementation(function (this: StatementSync, ...args: any[]) { if (args[0] === actorKey && holding) reads++; return Reflect.apply(nativeGet, this, args); });
    const put = vi.spyOn(StatementSync.prototype, "run").mockImplementation(function (this: StatementSync, ...args: any[]) { if (args[0] === actorKey && holding) upserts++; return Reflect.apply(nativeRun, this, args); });
    await run.batch({ identity: author, ops: [{ kind: "put", key: actorKey, value: { a: 1, b: 2 } }] });
    expect(reads).toBe(1); expect(upserts).toBe(1); expect(writes).not.toHaveBeenCalled(); expect(heldHashes).toBe(0);
    expect(fs.statSync(path.join(run.root, "historical-sql.jsonl")).size).toBe(0);
    expect(() => run.audit.flush()).toThrow(/outside-window/);
    exec.mockRestore(); get.mockRestore(); put.mockRestore(); writes.mockRestore(); hashes.mockRestore(); syncBuiltinESMExports();
    const { records } = run.finish(); expect(records).toHaveLength(1);
    expect(sql.filter(text => /SELECT|PRAGMA database_list/.test(text))).toEqual([]);
  });
  it("records explicit bounded drops and never calls an incomplete capture a complete rank", async () => {
    const run = await auditedStore({ maxRecords: 1 });
    await run.batch({ identity: author, ops: [{ kind: "put", key: participantKey, value: { a: 1, b: 2 } }, { kind: "put", key: actorKey, value: { a: 1, b: 2 } }] });
    const { records, result } = run.finish(); expect(records).toHaveLength(1);
    expect(run.audit.quality().dropped).toBe(1); expect(result.complete).toBe(false);
    expect(result.missingOrDuplicateApplied).toHaveLength(1);
  });
  it("bounds retained raw bytes as well as record counts, with explicit incomplete evidence", async () => {
    const run = await auditedStore({ maxBytes: 1000 });
    await run.batch({ identity: author, ops: [{ kind: "put", key: participantKey, value: { a: 1, b: 2 } }] });
    const { records, result } = run.finish(); expect(records).toHaveLength(0);
    expect(run.audit.quality().dropped).toBeGreaterThan(0); expect(result.complete).toBe(false);
    expect(run.audit.quality().bufferedBytes).toBe(0); expect(run.audit.quality().pendingReadBytes).toBe(0);
  });
  it("retains an actual same-transaction absent-row read and classifies creation separately", async () => {
    const run = await auditedStore(), key = `actors/new/${"c".repeat(32)}`;
    await run.batch({ identity: author, ops: [{ kind: "put", key, value: { a: 1 } }] });
    const { result } = run.finish(); expect(result.complete).toBe(true); expect(result.events[0].proof).toBe("creation");
    expect(result.events[0].prior.present).toBe(false); expect(result.partitions[0].provenUnchangedValueAndAuthor).toBe(0);
  });
  it("rejects wrong inode/PID, ambiguous calls, missing preparation and missing native before-state without seed/boundary promotion", () => {
    for (const mutate of [
      (row: Row) => { row.record.databaseIdentity = { ...row.db, inode: "foreign" }; },
      (row: Row) => { row.record.pid = 202; },
      (row: Row) => { row.capture.calls.push({ ...row.capture.calls[0] }); },
      (row: Row) => { row.capture.calls[0].ops = []; },
      (row: Row) => { row.capture.calls[0].storeRoot = "foreign-store"; },
      (row: Row) => { row.capture.transactions.push({ ...row.capture.transactions[0], database: { ...row.db, connectionId: 99 } }); },
      (row: Row) => { row.record.beforeEvidence = "UNKNOWN"; },
    ]) {
      const row = attributedExample(); mutate(row);
      const result = attributeHistoricalReputs([row.record], [{ label: "M00", capture: row.capture }], {}, [{ key: row.record.key, payloadSha256: row.record.payloadSha256 }], row.db);
      expect(result.events[0].proof).toBe("UNKNOWN"); expect(result.complete).toBe(false);
    }
  });
  it("observes own-root KV separately from participant/lease files at BOTH boundaries; equal endpoints remain only candidates", () => {
    const meshRoot = scratch(), rootId = "session:synthetic-root", hostId = "synthetic-host";
    fs.mkdirSync(path.join(meshRoot, "participants")); fs.mkdirSync(path.join(meshRoot, "host-leases"));
    freshJson(path.join(meshRoot, "participants", `${digest(rootId)}.json`), { key: participantKey, value: { id: rootId } });
    freshJson(path.join(meshRoot, "host-leases", `${digest(hostId).slice(0, 32)}.json`), { id: hostId, expiresAt: 1000 });
    let kv: Row | undefined;
    const mesh = { get: () => kv }, spec = { mesh: meshRoot, sharedDatabase: { file: "owned/state.db", device: "1", inode: "2" } }, native = { rootId };
    const observe = (phase: string) => ownRootBoundary(mesh, spec, native, { ownerHostId: hostId }, phase);
    const before = observe("startup"); expect(before.publication).toBe("participant-file-only-no-kv"); expect(before.hostLeaseFile.value.id).toBe(hostId);
    kv = { key: `topology/participants/${digest(rootId)}`, value: { id: rootId }, version: 2, updatedBy: author, updatedAt: 100 };
    const idle = observe("post-idle"); kv = { ...kv, version: 3, updatedAt: 200 }; const traffic = observe("post-traffic");
    const wrap = (value: Row) => [{ label: "M00", role: "measuredMain", ownRootObservation: value }];
    expect(ownRootProgress(wrap(before), wrap(idle), "idle")[0].boundaryEqualCandidate).toBe(false);
    const progress = ownRootProgress(wrap(idle), wrap(traffic), "traffic")[0];
    expect(progress.boundaryEqualCandidate).toBe(true); expect(progress.perCommitProof).toBe(false);
    const attribution = { complete: true, events: [{ key: traffic.key, phase: "traffic", classification: "committed-upsert",
      proof: "unchanged-value-and-author", version: 3, label: "M01", immediateSite: "actual-peer-caller" },
      { key: actorKey, phase: "traffic", classification: "committed-upsert", proof: "unchanged-value-and-author" }] };
    const joined = ownRootProgress(wrap(idle), wrap(traffic), "traffic", attribution)[0];
    expect(joined.sqlAttribution.committedWrites).toBe(1); expect(joined.sqlAttribution.provenUnchangedValueAndAuthor).toBe(1);
    expect(joined.sqlAttribution.writes[0].immediateSite).toBe("actual-peer-caller");
  });
});
