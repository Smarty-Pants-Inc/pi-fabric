import { performance } from "node:perf_hooks";
import { AsyncLocalStorage } from "node:async_hooks";
import fs from "node:fs";
import type { MeshStore } from "../src/mesh/store.js";
import type { StoreBridgeSide } from "../src/mesh/bridge.js";
import { isMeshSnapshotRetriesExhausted, MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE } from "../src/core/atomic-write.js";

export type SnapshotExhaustion = { code: typeof MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE;
  installationSide: "local" | "remote"; direction: "toLocal" | "toRemote"; phase: string; at: number };
export function summarizeSnapshotExhaustions(events: SnapshotExhaustion[], load: { from: number; to: number }) {
  return Object.fromEntries((["toLocal", "toRemote"] as const).map(direction => {
    const values = events.filter(event => event.direction === direction);
    const loadCount = values.filter(event => event.at >= load.from && event.at < load.to).length;
    return [direction, { installationSide: direction === "toLocal" ? "local" : "remote", count: values.length,
      loadCount, loadPerMinute: load.to > load.from ? loadCount * 60000 / (load.to - load.from) : null,
      byPhase: Object.fromEntries([...new Set(values.map(event => event.phase))].map(phase =>
        [phase, values.filter(event => event.phase === phase).length])) }];
  }));
}

/** Completion callbacks must be synchronous, immediate, and use the performance clock. */
export type MirrorPhaseEvent = { phase: "compute" | "leaseRead" | "leaseDigest"; holdMs: number };
type PhaseSample = MirrorPhaseEvent & { began: number; ended: number };
type NativeLockClass = "bridge" | "writeBatch" | "other";
type Breakdown = { aMs: number; bMs: number; cMs: number; computeInclusiveMs: number; nestedLeaseMs: number;
  overlapInconsistent: boolean; complete: boolean; phaseEvents: number; computeEvents: number; leaseEvents: number };

/** Driver observation only: never issue SQL, retry, change admission, or delay a callback. */
export function observeFences(hubDatabase: string) {
  const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
  const dbProto = sqlite.DatabaseSync.prototype;
  const statementProto = sqlite.StatementSync.prototype;
  const originalExec = dbProto.exec;
  const originalPrepare = dbProto.prepare;
  const originalAll = statementProto.all;
  const originalGet = statementProto.get;
  const locations = new WeakMap<object, string>();
  const statementDbs = new WeakMap<object, object>();
  type Context = { site: string; scans: number; stack: string };
  let context: Context | undefined;
  let phase = "startup";
  type Io = { calls: number; totalMs: number; maxMs: number };
  type Observation = { site: string; optimized: boolean; fallback: boolean; holdMs: number; attempt: number; retry: boolean };
  type Tx = { began: number; epoch: number; site: string; phase: string; scans: number; io: Record<string, Io>; observations: Observation[];
    lockClass: NativeLockClass; lockClassAttested: boolean; phaseSamples: PhaseSample[]; invalidPhaseEvents: number };
  type Hold = { site: string; phase: string; began: number; ended: number; ms: number; scans: number;
    path: string; prepareMs: number; afterCommitMs: number; observations: Observation[];
    lockClass: NativeLockClass; lockClassAttested: boolean; breakdown: Breakdown;
    phaseObservations: PhaseSample[]; invalidPhaseEvents: number };
  const batchScope = new AsyncLocalStorage<{ lockClass: NativeLockClass; active: boolean }>();
  const restorers: Array<() => void> = [];
  const snapshotExhaustions: SnapshotExhaustion[] = [];
  const observeMirror = (side: StoreBridgeSide, installationSide: "local" | "remote") => {
    const original = side.mirror;
    const wrapped: StoreBridgeSide["mirror"] = async (...args) => {
      try { return await original.apply(side, args); }
      catch (error) {
        // Advisory only: one record per actual failed installation call, never per
        // traffic-loop log/streak. Preserve the exact native rejection and arguments.
        try { if (isMeshSnapshotRetriesExhausted(error)) snapshotExhaustions.push({
          code: MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE, installationSide,
          direction: installationSide === "local" ? "toLocal" : "toRemote", phase, at: Date.now() }); } catch { /* observation cannot affect admission */ }
        throw error;
      }
    };
    side.mirror = wrapped;
    restorers.push(() => { if (side.mirror === wrapped) side.mirror = original; });
  };
  let phaseHookSeen = false;
  const outsidePhaseEvents = { compute: 0, leaseRead: 0, leaseDigest: 0, invalid: 0 };
  const transactions = new WeakMap<object, Tx>();
  let active: Tx | undefined;
  const holds: Hold[] = [];
  const callbacks: Array<{ site: string; phase: string; ms: number; scans: number }> = [];
  const queries: Record<string, { calls: number; totalMs: number; maxMs: number; samples: number[]; sql: string }> = {};
  const stacks: Record<string, string> = {};
  let listAllOutside = 0;
  let listAllInside = 0;
  let topologyScansInside = 0;
  let kvFullScansInside = 0;
  let busyBegins = 0;
  let attached = false;
  let callbackDepth = 0;
  const io: Record<string, Io> = {};
  const fdCategories = new Map<number, string>();
  const category = (file: unknown): string => {
    if (typeof file === "number") return fdCategories.get(file) ?? "fd-other";
    const value = String(file);
    for (const name of ["event-receipts", "host-leases", "participants", "archive-hub", "event-archive", "host-lease-commits"])
      if (value.includes(`/${name}/`) || value.endsWith(`/${name}`)) return name;
    return value.endsWith("events.jsonl") ? "event-log" : value.split("/").at(-1) ?? "other";
  };
  const txPath = (tx: Tx) => {
    const mirror = tx.observations.filter(observation => observation.site === "mirror.prepare");
    if (!mirror.length) {
      if (tx.observations.some(observation => /afterCommit/i.test(observation.site))) return tx.phase === "startup" ? "recovery-afterCommit" : "afterCommit-reacquire";
      return tx.site === "StoreBridgeSide.mirror" ? "effects-or-unattributed" : "not-mirror";
    }
    if (tx.phase === "startup") return "recovery";
    // Existing fallback=true covers BOTH initial authoritative recovery and full-plan
    // fallback. The raw flags alone cannot attest #recovered outside startup.
    if (mirror.some(observation => observation.fallback)) return "fallback-or-recovery";
    return mirror.every(observation => observation.optimized) ? "optimized-prepare" : "unattributed-prepare";
  };
  const fsOriginals: Record<string, (...args: any[]) => any> = {};
  const fsMethods = fs as unknown as Record<string, (...args: any[]) => any>;
  for (const name of ["writeSync", "readSync", "writeFileSync", "appendFileSync", "readFileSync", "fsyncSync", "fdatasyncSync", "openSync", "closeSync", "statSync", "lstatSync", "fstatSync", "renameSync", "readdirSync"]) {
    const original = fsMethods[name]!;
    fsOriginals[name] = original;
    fsMethods[name] = function (...args: any[]) {
      const tx = active;
      const group = category(args[0]);
      const invoke = () => {
        const result = original.apply(fs, args);
        if (name === "openSync" && typeof result === "number") fdCategories.set(result, group);
        if (name === "closeSync") fdCategories.delete(Number(args[0]));
        return result;
      };
      if (!tx) return invoke();
      const key = `${name}|${group}`;
      const entry = tx.io[key] ??= { calls: 0, totalMs: 0, maxMs: 0 };
      const began = performance.now();
      try { return invoke(); }
      finally { const ms = performance.now() - began; entry.calls++; entry.totalMs += ms; entry.maxMs = Math.max(entry.maxMs, ms); }
    };
  }
  const isHub = (db: object): boolean => (locations.get(db) ?? (db as InstanceType<typeof sqlite.DatabaseSync>).location()) === hubDatabase;
  const callSite = (stack: string): string => {
    if (/\bcapture\b/.test(stack)) return "subscribeChanges.capture";
    if (/#heldFence|heldFence|at Object\.fence|at fence \(/.test(stack)) return "StoreBridgeSide.heldFence";
    if (/StoreBridgeSide.*mirror|#mirror/.test(stack)) return "StoreBridgeSide.mirror";
    const named = stack.split("\n").map(line => line.trim()).find(line => /SqliteStateStore\.(put|writeBatch)|StoreBridgeSide|MeshBridge/.test(line));
    return named?.replace(/^at /, "").replace(/\s*\(.*$/, "") ?? "state-write";
  };
  dbProto.prepare = function (sql) {
    const statement = originalPrepare.call(this, sql);
    locations.set(this, this.location() ?? "");
    statementDbs.set(statement, this);
    return statement;
  };
  dbProto.exec = function (sql) {
    const token = sql.trim().toUpperCase();
    if (token === "BEGIN IMMEDIATE" && isHub(this)) {
      const stack = context?.stack ?? new Error().stack ?? "";
      const site = context?.site ?? callSite(stack);
      stacks[site] ??= stack;
      try { originalExec.call(this, sql); }
      catch (error) { busyBegins++; throw error; }
      // Acquisition/wait and stack capture are explicitly outside this interval.
      const began = performance.now();
      const batch = batchScope.getStore();
      // A synchronous read fence is native class other even when called from a batch's
      // pre-serialization/afterCommit context. A stack alone can NEVER attest bridge.
      const readFence = context !== undefined || /\bfenceSync\b/.test(stack);
      const lockClassAttested = !readFence && batch?.active === true;
      active = { began, epoch: performance.timeOrigin + began, site, phase, scans: 0, io: {}, observations: [],
        lockClass: lockClassAttested ? batch!.lockClass : "other", lockClassAttested,
        phaseSamples: [], invalidPhaseEvents: 0 };
      transactions.set(this, active);
      return;
    }
    const transaction = transactions.get(this);
    try { originalExec.call(this, sql); }
    finally {
      if (transaction && (token === "COMMIT" || token === "ROLLBACK") && !this.isTransaction) {
        const ms = performance.now() - transaction.began;
        const path = txPath(transaction);
        holds.push({ site: transaction.site, phase: transaction.phase, began: transaction.epoch,
          ended: transaction.epoch + ms, ms, scans: transaction.scans, path,
          prepareMs: transaction.observations.filter(observation => observation.site === "mirror.prepare").reduce((sum, observation) => sum + observation.holdMs, 0),
          afterCommitMs: transaction.observations.filter(observation => /afterCommit/i.test(observation.site)).reduce((sum, observation) => sum + observation.holdMs, 0),
          observations: transaction.observations, lockClass: transaction.lockClass, lockClassAttested: transaction.lockClassAttested,
          breakdown: phaseBreakdown(transaction, transaction.began + ms, ms, path, phaseHookSeen),
          phaseObservations: transaction.phaseSamples, invalidPhaseEvents: transaction.invalidPhaseEvents });
        for (const [method, value] of Object.entries(transaction.io)) {
          const key = `${transaction.phase}|${transaction.site}|${path}|${method}`;
          const entry = io[key] ??= { calls: 0, totalMs: 0, maxMs: 0 };
          entry.calls += value.calls; entry.totalMs += value.totalMs; entry.maxMs = Math.max(entry.maxMs, value.maxMs);
        }
        transactions.delete(this); if (active === transaction) active = undefined;
      }
    }
  };
  function query(statement: { sourceSQL: string }, method: "all" | "get", args: unknown[], invoke: () => unknown) {
    const db = statementDbs.get(statement);
    if (!db || !isHub(db)) return invoke();
    const transaction = transactions.get(db);
    const sql = statement.sourceSQL;
    const topology = method === "all" && sql.includes("key >= ?") && typeof args[0] === "string" &&
      /^topology\/(hosts|participants|handoffs)\//.test(args[0]);
    const full = method === "all" && /\bFROM kv\b/i.test(sql) && !/\bWHERE\b/i.test(sql);
    if (transaction && (topology || full)) {
      transaction.scans++;
      if (topology) topologyScansInside++;
      if (full) kvFullScansInside++;
    }
    let label = "other";
    if (sql.includes("min(commit_no)")) label = "changesOldest";
    else if (/FROM changes WHERE commit_no >/.test(sql)) label = "changesSince";
    else if (full) label = "kvFullScan";
    else if (topology) label = "topologyRange";
    else if (/FROM kv/.test(sql)) label = "kvPointOrRange";
    const key = `${phase}|${transaction?.site ?? "outside-write-lock"}|${method}:${label}`;
    const entry = queries[key] ??= { calls: 0, totalMs: 0, maxMs: 0, samples: [], sql };
    const started = performance.now();
    try { return invoke(); }
    finally {
      const ms = performance.now() - started;
      entry.calls++; entry.totalMs += ms; entry.maxMs = Math.max(entry.maxMs, ms);
      entry.samples.push(ms);
    }
  }
  statementProto.all = function (...args) {
    return query(this, "all", args, () => originalAll.apply(this, args as never)) as ReturnType<typeof originalAll>;
  };
  statementProto.get = function (...args) {
    return query(this, "get", args, () => originalGet.apply(this, args as never)) as ReturnType<typeof originalGet>;
  };
  const attach = (store: MeshStore) => {
    if (attached) throw new Error("fence metrics already attached");
    attached = true;
    const originalBatch = store.writeBatch;
    const wrappedBatch: MeshStore["writeBatch"] = input => {
      const scope = { lockClass: (input.lockClass === "bridge" ? "bridge" : "writeBatch") as NativeLockClass, active: true };
      return batchScope.run(scope, async () => {
        try { return await originalBatch.call(store, input); }
        finally { scope.active = false; }
      });
    };
    store.writeBatch = wrappedBatch;
    restorers.push(() => { if (store.writeBatch === wrappedBatch) store.writeBatch = originalBatch; });
    // A different native writer invoked during a batch (including pre-serialization)
    // must not inherit bridge's class merely because its async ancestry is a batch.
    const methods = store as unknown as Record<string, unknown>;
    for (const name of ["put", "delete", "confirmWritable", "exclusive"]) {
      const original = methods[name] as (...args: unknown[]) => unknown;
      const wrapped = (...args: unknown[]) => batchScope.exit(() => original.apply(store, args));
      methods[name] = wrapped;
      restorers.push(() => { if (methods[name] === wrapped) methods[name] = original; });
    }
    const originalFence = store.withStateWriteFence, originalList = store.listAll;
    const fence = originalFence.bind(store);
    const list = originalList.bind(store);
    store.withStateWriteFence = <T>(operation: () => T): T => {
      const previous = context;
      const stack = new Error().stack ?? "";
      const current: Context = { site: callSite(stack), scans: 0, stack };
      stacks[current.site] ??= stack;
      context = current;
      try {
        return fence(() => {
          // The callback starts only after acquisition: this second metric excludes waiting too.
          const began = performance.now(); callbackDepth++;
          try { return operation(); }
          finally { callbackDepth--; callbacks.push({ site: current.site, phase, ms: performance.now() - began, scans: current.scans }); }
        });
      } finally { context = previous; }
    };
    const wrappedFence = store.withStateWriteFence;
    store.listAll = (prefix, options) => {
      if (context) { context.scans++; listAllInside++; }
      else listAllOutside++;
      return list(prefix, options);
    };
    const wrappedList = store.listAll;
    restorers.push(() => {
      if (store.withStateWriteFence === wrappedFence) store.withStateWriteFence = originalFence;
      if (store.listAll === wrappedList) store.listAll = originalList;
    });
  };
  return {
    attach, observeMirror,
    observe: (observation: Observation) => { if (active) {
      if (observation.site.startsWith("mirror.")) active.site = "StoreBridgeSide.mirror";
      active.observations.push({ ...observation });
    } },
    observeMirrorPhase: (event: MirrorPhaseEvent): void => {
      const ended = performance.now();
      if (!event || !["compute", "leaseRead", "leaseDigest"].includes(event.phase) || !Number.isFinite(event.holdMs) || event.holdMs < 0) {
        if (active) active.invalidPhaseEvents++; else outsidePhaseEvents.invalid++;
        return;
      }
      phaseHookSeen = true;
      if (active) active.phaseSamples.push({ phase: event.phase, holdMs: event.holdMs, began: ended - event.holdMs, ended });
      else outsidePhaseEvents[event.phase]++;
    },
    phase: (value: string) => { phase = value; },
    result: () => ({ holds, callbacks, queries: Object.fromEntries(Object.entries(queries).map(([key, entry]) =>
      [key, { calls: entry.calls, totalMs: entry.totalMs, maxMs: entry.maxMs, p50Ms: percentile(entry.samples, .5),
        p99Ms: percentile(entry.samples, .99), sql: entry.sql }])),
      io, listAllInside, listAllOutside, topologyScansInside, kvFullScansInside, busyBegins, stacks, snapshotExhaustions,
      outsidePhaseEvents: { ...outsidePhaseEvents }, phaseHookSeen, bridgeTransactions: summarizeBridgeTransactions(holds),
      fullScanFallbackCallbacks: callbacks.filter(callback => callback.scans > 0).length }),
    restore: () => { dbProto.exec = originalExec; dbProto.prepare = originalPrepare;
      statementProto.all = originalAll; statementProto.get = originalGet;
      for (const [name, original] of Object.entries(fsOriginals)) fsMethods[name] = original;
      for (const restore of restorers.reverse()) restore(); },
  };
}

/** Union intervals, not marginal category sums. Only compute/lease nesting is expected. */
function phaseBreakdown(tx: { began: number; phaseSamples: PhaseSample[]; invalidPhaseEvents: number }, ended: number, ms: number, txPath: string, phaseHookSeen: boolean): Breakdown {
  // Completion-only structural events cannot carry exact timer endpoints. Allow 0.05ms
  // callback-entry skew; larger crossing/overlap is explicit inconsistent evidence.
  const precisionMs = 0.05;
  let inconsistent = tx.invalidPhaseEvents > 0;
  // Source compute always completes a #scan leaseRead first. Callback-entry
  // estimates must preserve that containment, even when the intervals are disjoint.
  let pendingReads: PhaseSample[] = [];
  for (const sample of tx.phaseSamples) {
    if (sample.phase === "leaseRead") pendingReads.push(sample);
    else if (sample.phase === "compute") {
      if (!pendingReads.length || pendingReads.some(read =>
        read.began < sample.began - precisionMs || read.ended > sample.ended + precisionMs)) inconsistent = true;
      pendingReads = [];
    }
  }
  // Unconsumed reads are legitimate optimized validation/afterCommit lease work.
  const ranges = (kind: "compute" | "lease") => tx.phaseSamples
    .filter(sample => kind === "compute" ? sample.phase === "compute" : sample.phase !== "compute")
    .map(sample => {
      if (sample.began < tx.began - precisionMs || sample.ended > ended + precisionMs) inconsistent = true;
      return [Math.max(tx.began, sample.began), Math.min(ended, sample.ended)] as [number, number];
    }).sort((left, right) => left[0] - right[0]);
  const union = (ranges: Array<[number, number]>) => {
    const result: Array<[number, number]> = [];
    for (const range of ranges) {
      const last = result.at(-1);
      if (last && range[0] < last[1]) {
        if (last[1] - range[0] > precisionMs) inconsistent = true;
        last[1] = Math.max(last[1], range[1]);
      } else result.push([...range]);
    }
    return result;
  };
  const computes = union(ranges("compute")), leases = union(ranges("lease"));
  const length = (ranges: Array<[number, number]>) => ranges.reduce((sum, [from, to]) => sum + Math.max(0, to - from), 0);
  let nestedLeaseMs = 0;
  for (const [from, to] of leases) for (const [computeFrom, computeTo] of computes) {
    const overlap = Math.max(0, Math.min(to, computeTo) - Math.max(from, computeFrom));
    nestedLeaseMs += overlap;
    if (overlap > precisionMs && (from < computeFrom - precisionMs || to > computeTo + precisionMs)) inconsistent = true;
  }
  const computeInclusiveMs = length(computes), bMs = length(leases);
  const aMs = Math.max(0, computeInclusiveMs - nestedLeaseMs);
  const rest = ms - aMs - bMs;
  if (rest < -precisionMs) inconsistent = true;
  return { aMs, bMs, cMs: Math.max(0, rest), computeInclusiveMs, nestedLeaseMs,
    overlapInconsistent: inconsistent,
    complete: tx.invalidPhaseEvents === 0 && (tx.phaseSamples.some(sample => sample.phase === "compute") ||
      (phaseHookSeen && (txPath === "optimized-prepare" || /afterCommit/.test(txPath)))),
    phaseEvents: tx.phaseSamples.length, computeEvents: tx.phaseSamples.filter(sample => sample.phase === "compute").length,
    leaseEvents: tx.phaseSamples.filter(sample => sample.phase !== "compute").length };
}

export function summarizeBridgeTransactions(holds: Array<{ lockClass?: string; lockClassAttested?: boolean; phase: string; path: string;
  site: string; ms: number; began: number; breakdown?: Breakdown }>) {
  const bridge = holds.filter(hold => hold.lockClass === "bridge" && hold.lockClassAttested === true);
  const group = (values: typeof holds) => {
    const ordered = values.slice().sort((a, b) => a.ms - b.ms || a.began - b.began);
    const sample = (hold: (typeof holds)[number] | undefined) => hold ? { holdMs: hold.ms, began: hold.began,
      site: hold.site, phase: hold.phase, path: hold.path, ...hold.breakdown,
      aFraction: hold.ms > 0 && hold.breakdown ? hold.breakdown.aMs / hold.ms : null } : null;
    const p50Tx = sample(ordered[Math.max(0, Math.ceil(ordered.length * .5) - 1)]);
    const p99Tx = sample(ordered[Math.max(0, Math.ceil(ordered.length * .99) - 1)]), maxTx = sample(ordered.at(-1));
    return { ...distribution(values.map(hold => hold.ms)), p50Tx, p99Tx, maxTx,
      incompleteTransactions: values.filter(hold => !hold.breakdown?.complete).length,
      inconsistentTransactions: values.filter(hold => hold.breakdown?.overlapInconsistent).length,
      go: [p99Tx, maxTx].every(tx => tx !== null && tx.complete === true && !tx.overlapInconsistent && tx.aFraction !== null && tx.aFraction >= .5) };
  };
  const steady = bridge.filter(hold => ["load", "drain", "idlebusy"].includes(hold.phase));
  return { all: group(bridge), startup: group(bridge.filter(hold => hold.phase === "startup")),
    steady: group(steady), byPhase: Object.fromEntries([...new Set(bridge.map(hold => hold.phase))].map(phase => [phase, group(bridge.filter(hold => hold.phase === phase))])),
    byPath: Object.fromEntries([...new Set(bridge.map(hold => hold.path))].map(path => [path, group(bridge.filter(hold => hold.path === path))])) };
}

export function percentile(values: number[], quantile: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(sorted.length * quantile) - 1)]!;
}

export function distribution(values: number[]) {
  return { n: values.length, totalMs: values.reduce((sum, value) => sum + value, 0),
    p50Ms: percentile(values, .5), p95Ms: percentile(values, .95), p99Ms: percentile(values, .99), maxMs: values.reduce((max, value) => Math.max(max, value), 0) };
}
