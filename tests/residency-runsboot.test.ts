import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { persistRunStopIntent } from "../src/agents/archive-custody.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, residentResultPath, type ResidentAgentMetadata, type ResidentHostConfig } from "../src/residency/protocol.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { readHostLease } from "../src/topology/host-leases.js";
import { retentionV2Enabled } from "../src/storage/retention-platform.js";
import { normalizeScope } from "../src/scope.js";
import { registerAgentRunner, type FabricHostedRunner } from "../src/runners.js";
import { diagnostics } from "../src/core/diagnostics.js";

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-runsboot-"));
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:runsboot", sessionId: "runsboot", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    // Unsafe-metadata cases need seven actually started jobs, not queued handles.
    fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, maxConcurrent: 16 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
    workerPath: "worker.js", fabricExtensionPath: "index.js", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  return { root, config, runs: path.join(config.residencyRoot, "runs") };
};

/** Observe the real filesystem calls, not source text or replacement recovery implementations. */
const observeRuns = (runs: string, ...additionalRoots: string[]) => {
  const roots = [runs, ...additionalRoots];
  const work = { listings: 0, opens: 0, entries: 0, reads: 0, statusReads: 0, stats: 0 };
  const atRoot = (target: unknown) => roots.includes(String(target));
  const within = (target: unknown) => roots.some(root => String(target) === root || String(target).startsWith(root + path.sep));
  const readdir = fs.readdirSync, opendir = fs.opendirSync, read = fs.readFileSync;
  const stat = fs.statSync, lstat = fs.lstatSync;
  const asyncReaddir = fs.promises.readdir, asyncOpendir = fs.promises.opendir, asyncRead = fs.promises.readFile;
  const asyncStat = fs.promises.stat, asyncLstat = fs.promises.lstat;
  const observeCursor = (directory: fs.Dir): fs.Dir => {
    const nextSync = directory.readSync.bind(directory), next = directory.read.bind(directory);
    vi.spyOn(directory, "readSync").mockImplementation(() => {
      const entry = nextSync();
      if (entry) work.entries++;
      return entry;
    });
    vi.spyOn(directory, "read").mockImplementation((async () => {
      const entry = await next();
      if (entry) work.entries++;
      return entry;
    }) as typeof directory.read);
    return directory;
  };
  const noteRead = (file: unknown) => {
    if (within(file)) {
      work.reads++;
      if (path.basename(String(file)) === "status.json") work.statusReads++;
    }
  };
  vi.spyOn(fs, "readdirSync").mockImplementation(((...args: Parameters<typeof fs.readdirSync>) => {
    if (atRoot(args[0])) work.listings++;
    return readdir(...args);
  }) as typeof fs.readdirSync);
  vi.spyOn(fs, "opendirSync").mockImplementation((...args: Parameters<typeof fs.opendirSync>) => {
    if (atRoot(args[0])) work.opens++;
    const directory = opendir(...args);
    return atRoot(args[0]) ? observeCursor(directory) : directory;
  });
  vi.spyOn(fs.promises, "readdir").mockImplementation(((...args: Parameters<typeof fs.promises.readdir>) => {
    if (atRoot(args[0])) work.listings++;
    return asyncReaddir(...args);
  }) as typeof fs.promises.readdir);
  vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
    if (atRoot(args[0])) work.opens++;
    const directory = await asyncOpendir(...args);
    return atRoot(args[0]) ? observeCursor(directory) : directory;
  });
  vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
    noteRead(args[0]);
    return read(...args);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs.promises, "readFile").mockImplementation(((...args: Parameters<typeof fs.promises.readFile>) => {
    noteRead(args[0]);
    return asyncRead(...args);
  }) as typeof fs.promises.readFile);
  vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
    if (within(args[0])) work.stats++;
    return stat(...args);
  }) as typeof fs.statSync);
  vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
    if (within(args[0])) work.stats++;
    return lstat(...args);
  }) as typeof fs.lstatSync);
  vi.spyOn(fs.promises, "stat").mockImplementation(((...args: Parameters<typeof fs.promises.stat>) => {
    if (within(args[0])) work.stats++;
    return asyncStat(...args);
  }) as typeof fs.promises.stat);
  vi.spyOn(fs.promises, "lstat").mockImplementation(((...args: Parameters<typeof fs.promises.lstat>) => {
    if (within(args[0])) work.stats++;
    return asyncLstat(...args);
  }) as typeof fs.promises.lstat);
  return work;
};

/** Follow the target through pathname and opened-descriptor reads. A
 * readFileSync(path)-only spy misses a bounded open/fstat/readSync receiver. */
const observeMetadataReads = (file: string, unreadable = false) => {
  const open = fs.openSync, close = fs.closeSync, readFile = fs.readFileSync;
  const read = fs.readSync, readv = fs.readvSync;
  const descriptors = new Set<number>();
  const work = { opens: 0, reads: 0, failures: 0 };
  const matches = (target: unknown): boolean => typeof target === "number"
    ? descriptors.has(target) : String(target) === file;
  const fail = (): never => {
    work.failures++;
    throw Object.assign(new Error("metadata unreadable"), { code: "EACCES" });
  };
  const noteRead = (target: unknown) => {
    if (!matches(target)) return;
    work.reads++;
    if (unreadable) fail();
  };
  vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
    const target = matches(args[0]);
    if (target) {
      work.opens++;
      if (unreadable) fail();
    }
    const fd = open(...args);
    if (target) descriptors.add(fd);
    return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "closeSync").mockImplementation((...args: Parameters<typeof fs.closeSync>) => {
    try { return close(...args); }
    finally { descriptors.delete(args[0]); } // descriptor reuse must not implicate an unrelated file
  });
  vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
    noteRead(args[0]);
    return readFile(...args);
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "readSync").mockImplementation(((...args: Parameters<typeof fs.readSync>) => {
    noteRead(args[0]);
    return read(...args);
  }) as typeof fs.readSync);
  vi.spyOn(fs, "readvSync").mockImplementation(((...args: Parameters<typeof fs.readvSync>) => {
    noteRead(args[0]);
    return readv(...args);
  }) as typeof fs.readvSync);
  return work;
};

/** Observe and join the real manager operation, including its directory cursor. */
const observeHostedRecovery = (onRecover?: (manager: AgentManager, options: Parameters<AgentManager["recoverHostedRuns"]>[1]) => void) => {
  const recover = AgentManager.prototype.recoverHostedRuns;
  const fallbacks: Array<{ manager: AgentManager; options: Parameters<AgentManager["recoverHostedRuns"]>[1]; pending: Promise<string[]> }> = [];
  vi.spyOn(AgentManager.prototype, "recoverHostedRuns").mockImplementation(function (this: AgentManager, candidates, options) {
    // The unified host operation makes exactly one no-args manager pass AFTER
    // its metadata/import phase. Do not confuse other native/reference cursors
    // with this pass merely because they happen to open runs/.
    if (candidates === undefined) onRecover?.(this, options);
    const pending = recover.call(this, candidates, options);
    if (candidates === undefined) {
      fallbacks.push({ manager: this, options, pending });
      void pending.catch(() => undefined); // cancellation is asserted by its owner
    }
    return pending;
  });
  const complete = async (index = 0) => {
    await vi.waitFor(() => expect(fallbacks.length).toBeGreaterThan(index), { timeout: 5_000 });
    await fallbacks[index]!.pending;
    await new Promise<void>(resolve => setImmediate(resolve)); // host's completion/finally custody
  };
  return { fallbacks, complete };
};

const hostedRunner = () => {
  const calls = { starts: [] as string[], attaches: [] as string[], stops: 0 };
  const adapter: FabricHostedRunner = {
    id: "runsboot-daemon", label: "Runsboot daemon", kind: "hosted",
    capabilities: { recursiveFabric: false, steer: false, followUp: false, persistentSessions: false,
      kernels: false, handoff: false, modelDiscovery: false, imageInput: false, compaction: false,
      questions: false, sleep: false, writePolicy: false },
    prepare: context => ({ job: context.idempotencyKey }),
    start: (_locator, context) => { calls.starts.push(context.id); },
    attach: (_locator, context) => { calls.attaches.push(context.id); },
    liveness: () => "running",
    stop: () => { calls.stops++; return { confirmed: true }; },
  };
  return { adapter, calls, unregister: registerAgentRunner(adapter) };
};

const saveMetadata = (config: ResidentHostConfig, metadata: ResidentAgentMetadata) => {
  const directory = path.join(config.residencyRoot, "agents");
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, `${metadata.id}.json`), JSON.stringify(metadata));
};

const seedManager = (config: ResidentHostConfig) => new AgentManager(config.cwd, config.agents, {
  runRoot: path.join(config.residencyRoot, "runs"), fullCodeMode: false,
});

/** Real dynamic import with top-level await, not a replacement module loader. */
const gatedRunnerModule = (root: string, register: () => void) => {
  const key = `runsboot-import:${root}`;
  let enter!: () => void, release!: () => void, registered!: () => void;
  const entered = new Promise<void>(resolve => { enter = resolve; });
  const wait = new Promise<void>(resolve => { release = resolve; });
  const finished = new Promise<void>(resolve => { registered = resolve; });
  const globals = globalThis as unknown as Record<string, unknown>;
  globals[key] = { enter, wait, register: () => { register(); registered(); } };
  const module = path.join(root, "gated-runner.mjs");
  fs.writeFileSync(module, `const gate = globalThis[${JSON.stringify(key)}]; gate.enter(); await gate.wait; gate.register();`);
  return { module, entered, release, finished,
    finish: async () => { release(); await import(pathToFileURL(module).href); await finished; },
    dispose: () => { delete globals[key]; } };
};

const startBeforeImport = async (host: ResidentHost, gate: ReturnType<typeof gatedRunnerModule>) => {
  const starting = host.start();
  const winner = await Promise.race([starting.then(() => "ready"), gate.entered.then(() => "pre-ready import")]);
  // A regressed pre-ready importer must fail WITHOUT stranding its module or
  // startup fixture. Release and join it before reporting the boundary failure.
  if (winner !== "ready") gate.release();
  await starting;
  expect(winner).toBe("ready");
};

describe("resident retained-run startup (smarty-dev#4796)", () => {
  // Keep reason: the real 20k-hex-ID state catches the full startup walk that
  // the older archived-N fixture misses; retention work must stay incremental.
  it("reaches readiness and the first event-loop turn with an owned actor without walking 20k runs or retained metadata", { timeout: 30_000 }, async () => {
    const { root, config, runs } = fixture();
    // Use the actual create/registry/close path, not a hand-authored row that
    // misses claimResidency and constructor-time native recovery.
    const previous = new ResidentHost(config);
    let actorId: string;
    try {
      await previous.start();
      const actor = await previous.actors.create({ name: "owned-startup", instructions: "wait", residency: "durable", scope: "project" });
      actorId = actor.id;
    } finally { await previous.close(); }
    expect(new ActorRegistryStore(config.actorRoot).records().find(row => row.id === actorId!))
      .toMatchObject({ rootId: config.rootId, residency: "durable" });
    for (let index = 1; index <= 20_000; index++) {
      const id = index.toString(16).padStart(32, "0");
      const directory = path.join(runs, id);
      fs.mkdirSync(directory, { recursive: true });
      fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({
        id, name: "archived task", runner: "pi", status: "completed", transport: "process",
        sessionId: "2147483647", startedAt: 1, updatedAt: 2, finishedAt: 2, text: "archived answer",
        turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: 0 },
      }));
      saveMetadata(config, { format: 1, rootId: config.rootId, id,
        handle: { id, name: "retained native task", runner: "pi", status: "completed", transport: "process", cwd: root },
        runDirectory: directory, createdAt: 1, updatedAt: 2, completionConsumedAt: 2 });
    }
    const host = new ResidentHost(config);
    // One observer owns each fs spy; stacking spyOn on the same function
    // rewrites the first implementation rather than making an independent tap.
    const work = observeRuns(runs, path.join(config.residencyRoot, "agents"));
    try {
      await host.start();
      const ready = { ...work };
      expect(readHostLease(config.meshRoot, host.hostId)?.expiresAt).toBeGreaterThan(Date.now());
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "maintenance-ready.json"), "utf8"))).toMatchObject({ token: host.generation });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(ready).toEqual({ listings: 0, opens: 0, entries: 0, reads: 0, statusReads: 0, stats: 0 });
      // An owned actor's first ready-gated maintenance may open its native
      // recovery cursor by the first event-loop turn. Count async work too: it must remain a
      // bounded first slice, never the historical startup sweep.
      expect(work.listings).toBe(0);
      expect(work.opens).toBeLessThanOrEqual(2); // metadata + owned actor native cursors
      expect(work.entries).toBeLessThanOrEqual(128); // at most one bounded slice per cursor
      expect(work.reads).toBeLessThanOrEqual(128);
      expect(work.statusReads).toBeLessThanOrEqual(64);
      expect(host.actors.status(actorId!).residency).toBe("durable");
      expect(host.actors.status(actorId!).status).not.toBe("dormant"); // scheduled recovery retains custody

      if (retentionV2Enabled()) {
        const slices: Array<{ options: Parameters<AgentManager["retentionReferences"]>[0]; entries: number; statusRuns: number; wildcard: boolean }> = [];
        const references = host.agents.retentionReferences.bind(host.agents);
        vi.spyOn(host.agents, "retentionReferences").mockImplementation(options => {
          const before = { ...work };
          const readsBefore = vi.mocked(fs.readFileSync).mock.calls.length;
          const refs = references(options);
          // An exit proof can legitimately re-read one status several times.
          // The count limit bounds visited run units, not raw predicate calls.
          const statuses = new Set(vi.mocked(fs.readFileSync).mock.calls.slice(readsBefore)
            .map(([file]) => String(file)).filter(file => file.startsWith(runs + path.sep) && path.basename(file) === "status.json"));
          slices.push({ options, entries: work.entries - before.entries, statusRuns: statuses.size, wildcard: refs.has("*") });
          return refs;
        });
        await vi.waitFor(() => expect(slices.length).toBeGreaterThanOrEqual(3), { timeout: 5_000 });
        expect(slices.some(slice => slice.entries > 0)).toBe(true);
        for (const slice of slices) {
          expect(slice.options).toMatchObject({ budgetMs: 2, maxEntries: 64, now: expect.any(Number) });
          expect(slice.entries).toBeLessThanOrEqual(64);
          expect(slice.statusRuns).toBeLessThanOrEqual(64);
          expect(slice.wildcard).toBe(true);
        }
        expect(work.listings).toBe(0); // no unbounded zero-delay fallback after readiness either
      }
    } finally {
      vi.restoreAllMocks();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // Keep reason: cold-history wedge regression. Fault ONLY the real hosted
  // recovery's first status read per candidate, not retention's repeated safety
  // predicates. The owner lease and native request/response path stay real.
  it("serves a trusted request and renews its persisted lease throughout 20k slow historical status reads", { timeout: 240_000 }, async () => {
    const { root, config } = fixture();
    config.residencyRoot = residentRoot(config.meshRoot, config.rootId);
    config.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    config.piModels = { available: [{ provider: "test", id: "visible" }], aliases: {}, defaultModel: "test/visible" };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const runs = path.join(config.residencyRoot, "runs");
    const historical = new Set<string>();
    for (let index = 1; index <= 20_000; index++) {
      const id = index.toString(16).padStart(32, "0");
      historical.add(id);
      const directory = path.join(runs, id);
      fs.mkdirSync(directory, { recursive: true });
      fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, name: "cold history", runner: "pi",
        status: "completed", transport: "process", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2,
        text: "saved", turns: 1, toolCalls: 0,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: 0 } }));
    }
    const host = new ResidentHost(config);
    const client = new ResidentActorClient(config.meshRoot, config.rootId, 10_000);
    const waitCell = new Int32Array(new SharedArrayBuffer(4));
    let statusReadCount = 0, historicalReads = 0, faultFile: string | undefined;
    let active = false, settled = false, entered!: () => void;
    const midRecovery = new Promise<void>(resolve => { entered = resolve; });
    let yieldCount = 0, readsAtYield = 0, maxSliceReads = 0, maxYieldGapMs = 0, lastYieldAt = 0;
    let leaseRenewCount = 0, maxLeaseGapMs = 0, minLeaseRemainingMs = Infinity, maxLeaseAgeMs = 0;
    let previousLease: ReturnType<typeof readHostLease>;
    const sampleLease = () => {
      const lease = readHostLease(config.meshRoot, host.hostId);
      if (!lease) throw new Error("Owner's persisted lease disappeared during recovery");
      const now = Date.now();
      minLeaseRemainingMs = Math.min(minLeaseRemainingMs, lease.expiresAt - now);
      maxLeaseAgeMs = Math.max(maxLeaseAgeMs, now - lease.updatedAt);
      if (previousLease && previousLease.updatedAt !== lease.updatedAt) {
        leaseRenewCount++;
        maxLeaseGapMs = Math.max(maxLeaseGapMs, lease.updatedAt - previousLease.updatedAt);
      }
      previousLease = lease;
    };
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      const file = String(args[0]);
      if (file.startsWith(runs + path.sep) && path.basename(file) === "status.json" && historical.has(path.basename(path.dirname(file)))) historicalReads++;
      if (file === faultFile) {
        faultFile = undefined;
        sampleLease();
        Atomics.wait(waitCell, 0, 0, 5); // deterministic synchronous cold-read fault
        statusReadCount++;
        if (statusReadCount === 1_000) entered();
      }
      return read(...args);
    }) as typeof fs.readFileSync);
    const immediate = globalThis.setImmediate;
    vi.spyOn(globalThis, "setImmediate").mockImplementation(((callback: (...args: unknown[]) => void, ...args: unknown[]) =>
      immediate(() => {
        if (active) {
          const now = performance.now();
          yieldCount++;
          maxYieldGapMs = Math.max(maxYieldGapMs, now - lastYieldAt);
          maxSliceReads = Math.max(maxSliceReads, statusReadCount - readsAtYield);
          readsAtYield = statusReadCount; lastYieldAt = now;
        }
        callback(...args);
      })) as typeof setImmediate);
    const nativeLaunches = vi.spyOn(ProcessTransport.prototype, "launch");
    const recovery = observeHostedRecovery((_manager, options) => {
      active = true; lastYieldAt = performance.now();
      const shouldRecover = options!.shouldRecover!;
      options!.shouldRecover = id => {
        const allowed = shouldRecover(id);
        if (allowed && historical.has(id)) faultFile = path.join(runs, id, "status.json");
        return allowed;
      };
    });
    try {
      const startAt = performance.now();
      await host.start();
      const readyMs = performance.now() - startAt;
      expect(historicalReads).toBe(0); // structural readiness check, no flaky wall-clock budget
      expect(statusReadCount).toBe(0);
      sampleLease();
      await midRecovery;
      expect(recovery.fallbacks).toHaveLength(1);
      void recovery.fallbacks[0]!.pending.then(() => { settled = true; }, () => { settled = true; });
      const readsAtRequest = statusReadCount;
      const requestAt = performance.now();
      const actor = await client.createActor({ name: "served-during-recovery", instructions: "Respond", residency: "durable",
        topics: ["runsboot.slow-history-wake"], responseMode: "text" });
      const trustedRequestMs = performance.now() - requestAt;
      expect(trustedRequestMs).toBeLessThan(10_000); // far below the ~100s injected scan
      const pendingAtResponse = !settled;
      const readsAtResponse = statusReadCount;
      expect(pendingAtResponse).toBe(true);
      expect(readsAtRequest).toBeGreaterThanOrEqual(1_000);
      expect(readsAtResponse).toBeLessThan(20_000);
      expect(new ActorRegistryStore(config.actorRoot).records().find(row => row.id === actor.id))
        .toMatchObject({ rootId: config.rootId, residency: "durable" });
      // The actor client has no tell API. Use the real native mesh wake route,
      // not a direct manager call or a mocked receiver, and require actual output.
      const wakeAt = performance.now();
      await host.mesh.publish({ topic: "runsboot.slow-history-wake", from: host.identity,
        data: { message: "Accept this wake while historical recovery is still pending" } });
      await vi.waitFor(() => expect(host.actors.status(actor.id).queued).toBeGreaterThan(0), { timeout: 10_000 });
      const wakeAcceptedMs = performance.now() - wakeAt;
      const pendingAtWakeAccepted = !settled;
      const readsAtWakeAccepted = statusReadCount;
      expect(pendingAtWakeAccepted).toBe(true);
      expect(readsAtWakeAccepted).toBeLessThan(20_000);
      expect(wakeAcceptedMs).toBeLessThan(10_000);
      expect(nativeLaunches).not.toHaveBeenCalled();
      expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(0);
      // Authorized fallback A: request/mesh acceptance remains live, but actor
      // output is held until the hosted scan finishes. Do not hide that cost.
      await recovery.complete();
      const scanHeldActorOutputMs = performance.now() - wakeAt;
      await vi.waitFor(() => expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1), { timeout: 45_000 });
      const wakeLatencyMs = performance.now() - wakeAt;
      const pendingAtWakeOutput = !settled;
      const readsAtWakeOutput = statusReadCount;
      expect(pendingAtWakeOutput).toBe(false);
      expect(readsAtWakeOutput).toBe(20_000);
      expect(nativeLaunches).toHaveBeenCalledTimes(1);
      expect(await client.actorStatus(actor.id)).toMatchObject({ id: actor.id, lastRunId: expect.any(String) });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1);
      active = false;
      sampleLease();
      maxSliceReads = Math.max(maxSliceReads, statusReadCount - readsAtYield);
      const recoveryMs = performance.now() - startAt - readyMs;
      console.log(JSON.stringify({ metric: "resident-slow-history", logicalRuns: 20_000, delayPerStatusMs: 5,
        readyMs, recoveryMs, statusReadCount, yieldCount, maxSliceReads, maxYieldGapMs,
        heartbeatMaxGapMs: maxLeaseGapMs, persistedLeaseRenewCount: leaseRenewCount,
        persistedLeaseMaxGapMs: maxLeaseGapMs, persistedLeaseMaxAgeMs: maxLeaseAgeMs,
        persistedLeaseMinRemainingTtlMs: minLeaseRemainingMs, trustedRequestMs,
        pendingRecoveryAtResponse: pendingAtResponse, readsAtRequest, readsAtResponse,
        wakeAcceptedMs, pendingRecoveryAtWakeAccepted: pendingAtWakeAccepted, readsAtWakeAccepted,
        scanHeldActorOutputMs, wakeLatencyMs, pendingRecoveryAtWakeOutput: pendingAtWakeOutput, readsAtWakeOutput, nativeWakeOutputs: 1 }));
      expect(statusReadCount).toBe(20_000);
      expect(yieldCount).toBeGreaterThanOrEqual(Math.floor(20_000 / 64));
      expect(maxSliceReads).toBeLessThanOrEqual(64);
      expect(leaseRenewCount).toBeGreaterThanOrEqual(3);
      // Production heartbeat is 5s, lease TTL 15s. Require observed renewal
      // within TWO heartbeats, not merely a lease that happens to remain live.
      expect(maxLeaseGapMs).toBeLessThanOrEqual(10_000);
      expect(maxLeaseAgeMs).toBeLessThanOrEqual(10_000);
      expect(minLeaseRemainingMs).toBeGreaterThan(0);
    } finally {
      active = false;
      vi.restoreAllMocks();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(!retentionV2Enabled())("counts cached directory and watermark visits against maxEntries", async () => {
    const { root, config, runs } = fixture();
    const addRun = (index: number) => {
      const directory = path.join(runs, index.toString(16).padStart(32, "0"));
      fs.mkdirSync(directory, { recursive: true });
      fs.writeFileSync(path.join(directory, "status.json"), "{}"); // unknown exit remains a wildcard, not collection authority
    };
    for (let index = 1; index <= 96; index++) addRun(index);
    const manager = seedManager(config);
    const work = observeRuns(runs);
    const now = Date.now();
    const prepare = () => {
      const slices: Array<{ entries: number; reads: number; ready: boolean }> = [];
      for (let pass = 0; pass < 128; pass++) {
        const before = { ...work };
        const refs = manager.retentionReferences({ now, budgetMs: 10_000, maxEntries: 7 });
        const ready = manager.retentionReferencesReady(now);
        slices.push({ entries: work.entries - before.entries, reads: work.statusReads - before.statusReads, ready });
        expect(refs.has("*")).toBe(true);
        expect(slices.at(-1)!.entries).toBeLessThanOrEqual(7);
        expect(slices.at(-1)!.reads).toBeLessThanOrEqual(7);
        if (ready) return slices;
      }
      throw new Error("retention preparation failed to progress within its bounded fixture");
    };
    try {
      expect(prepare().at(-1)!.ready).toBe(true);
      addRun(97);
      const changed = new Date(now - 10_000);
      fs.utimesSync(runs, changed, changed); // force a distinct generation even on coarse-timestamp filesystems
      const warm = prepare();
      expect(warm.some(slice => slice.entries > 0 && slice.reads === 0)).toBe(true);
      expect(warm.some(slice => slice.entries === 0 && !slice.ready)).toBe(true); // watermark replay also yields
      expect(work.listings).toBe(0);
    } finally {
      vi.restoreAllMocks();
      await manager.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // Keep reason: reattachment is an execution boundary. Optimizing historical
  // discovery must not submit a hosted job twice or skip admitted metadata.
  it("reattaches the exact resident hosted metadata selection, never starts it again", async () => {
    const { root, config, runs } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    try {
      const kept = await first.spawn({ task: "survive restart", runner: runner.adapter.id, residency: "durable" });
      const foreign = await first.spawn({ task: "other owner", runner: runner.adapter.id, residency: "durable" });
      await first.close();
      const metadata = (handle: typeof kept): ResidentAgentMetadata => ({
        format: 1, rootId: config.rootId, id: handle.id, handle,
        runDirectory: path.join(runs, handle.id), createdAt: 1, updatedAt: 1,
      });
      saveMetadata(config, metadata(kept));
      saveMetadata(config, { ...metadata(foreign), rootId: "session:foreign" });
      const work = observeRuns(runs);
      const recovery = observeHostedRecovery();
      await host.start();
      expect(runner.calls.attaches).toEqual([]);
      expect(work.statusReads).toBe(0);
      await recovery.complete();
      expect(runner.calls.attaches).toEqual([kept.id]);
      expect(runner.calls.starts).toEqual([kept.id, foreign.id]);
      expect(host.agents.status(kept.id)).toMatchObject({ status: "running", transport: "hosted" });
      expect(work.listings).toBe(0);
      expect(work.statusReads).toBeGreaterThan(0);
      // Independent retention safety may inspect this file after readiness;
      // that read is not admission. Foreign metadata still cannot adopt/attach.
      expect(JSON.parse(fs.readFileSync(path.join(runs, foreign.id, "status.json"), "utf8"))).toMatchObject({ status: "running" });
      expect(runner.calls.stops).toBe(0);
      await recovery.complete(); // the delayed orphan sweep must not undo the ownership filter
      expect(runner.calls.attaches).toEqual([kept.id]);
      expect(runner.calls.starts).toEqual([kept.id, foreign.id]);
      expect(recovery.fallbacks[0]!.options).toMatchObject({ budgetMs: 2, maxEntries: 64 });
    } finally {
      vi.restoreAllMocks();
      await host.close();
      await first.close();
      runner.unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // Keep reason: reproduce a launched native actor's real pre-delivery crash
  // image, rather than inventing an unsupported hosted-adapter actor. Restored
  // work must stay held during the hosted scan and reuse its original result.
  it("holds a restored launched native activation until discovery completes and delivers its original run once", { timeout: 30_000 }, async () => {
    const { root, config, runs } = fixture();
    config.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    config.piModels = { available: [{ provider: "test", id: "visible" }], aliases: {}, defaultModel: "test/visible" };
    const previous = new ResidentHost(config);
    let host!: ResidentHost;
    let releaseResult!: () => void, enteredResult!: () => void, enteredManager!: () => void;
    const resultHeld = new Promise<void>(resolve => { releaseResult = resolve; });
    const resultEntered = new Promise<void>(resolve => { enteredResult = resolve; });
    const managerEntered = new Promise<void>(resolve => { enteredManager = resolve; });
    const launch = ProcessTransport.prototype.launch;
    const launches = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(function (this: ProcessTransport, request) {
      return launch.call(this, request);
    });
    try {
      await previous.start();
      const actor = await previous.actors.create({ name: "launched-before-restart", instructions: "Respond", residency: "durable", responseMode: "text" });
      const run = previous.agents.run.bind(previous.agents);
      let original: Awaited<ReturnType<AgentManager["run"]>> | undefined;
      const runGate = vi.spyOn(previous.agents, "run").mockImplementation(async (...args: Parameters<AgentManager["run"]>) => {
        const result = await run(...args); // actual launch, worker, disk result, and manager join
        original = result;
        enteredResult();
        await resultHeld; // crash image is captured before actor output/queue acknowledgement
        return result;
      });
      previous.actors.tell(actor.id, "produce the original durable result", undefined);
      await resultEntered;
      expect(original).toMatchObject({ status: "completed", text: "fake worker complete" });
      expect(launches).toHaveBeenCalledTimes(1);
      const runDirectory = path.join(runs, original!.id);
      const actorSnapshot = path.join(root, "crash-actors");
      const runSnapshot = path.join(root, "crash-run");
      fs.cpSync(config.actorRoot, actorSnapshot, { recursive: true,
        filter: source => source !== path.join(config.actorRoot, "actors.json.lock") }); // never restore a live writer's transient lock
      fs.cpSync(runDirectory, runSnapshot, { recursive: true });
      const actorDirectory = path.join(actorSnapshot, actor.id);
      const queueFiles = fs.readdirSync(actorDirectory).filter(name => /^queue-.+\.json$/.test(name));
      const queued = queueFiles.flatMap(name => {
        const saved = JSON.parse(fs.readFileSync(path.join(actorDirectory, name), "utf8"));
        return saved.items ?? [];
      });
      expect(queued).toContainEqual(expect.objectContaining({ executionStarted: true, resumed: true }));
      // Use the supported durable-stop/result recovery receipt. The status,
      // task activation identity, launched queue and result are actual bytes
      // produced above; only the crash/stop boundary is injected here.
      const recoveryRecord = JSON.parse(fs.readFileSync(path.join(runSnapshot, "manager-result.json"), "utf8"));
      expect(recoveryRecord).toMatchObject({ id: original!.id, actorId: actor.id, transport: "process", status: "completed" });
      persistRunStopIntent(runSnapshot, "explicit", recoveryRecord);
      const closing = previous.close();
      releaseResult();
      await closing;
      runGate.mockRestore();
      // Restore only this fixture's captured crash image. No shared roots or
      // real runroots are opened, replaced, or removed by the test.
      fs.rmSync(config.actorRoot, { recursive: true, force: true });
      fs.cpSync(actorSnapshot, config.actorRoot, { recursive: true });
      fs.rmSync(runDirectory, { recursive: true, force: true });
      fs.cpSync(runSnapshot, runDirectory, { recursive: true });
      host = new ResidentHost(config); // all new-generation watchers see the restored image
      // start() initializes both directories: observe the actual prototypes
      // before bootstrap, only after the previous generation has fully closed.
      const actorResumes = vi.spyOn(ActorDirectory.prototype, "resumeAfterRelease"); // passthrough, actual bootstrap transition
      const replay = vi.spyOn(AgentManager.prototype, "run"); // passthrough: never fake reconciliation
      const recovery = observeHostedRecovery(manager => {
        manager.pauseForRelease(true);
        enteredManager();
      });
      await host.start();
      await managerEntered;
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(recovery.fallbacks).toHaveLength(1);
      expect(actorResumes).not.toHaveBeenCalled(); // deterministic old-head red, independent of drain microtask timing
      expect(replay).not.toHaveBeenCalled(); // restored drain cannot enter before this pass completes
      expect(launches).toHaveBeenCalledTimes(1);
      expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(0);
      host.agents.resumeAfterRelease();
      await recovery.complete();
      await vi.waitFor(() => expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1), { timeout: 10_000 });
      const output = host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error);
      expect(output[0]).toMatchObject({ runId: original!.id, text: original!.text });
      expect(host.actors.status(actor.id).lastRunId).toBe(original!.id);
      expect(actorResumes).toHaveBeenCalled();
      expect(replay).toHaveBeenCalledTimes(1);
      expect(launches).toHaveBeenCalledTimes(1); // original worker only; no replay/submission
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1);
    } finally {
      releaseResult();
      await previous.close(); await host?.close();
      vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // Keep reason: P1 is a real host-to-manager visibility gap, not an attach
  // failure. Hold real discovery before its first row, then hold the real
  // manager at entry AFTER runner imports; neither status nor UI may adopt.
  it.each(["before metadata discovery", "after runner imports"] as const)(
    "projects known hosted runs at %s without early adoption or attachment", { timeout: 15_000 }, async boundary => {
    const { root, config, runs } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    let unregisterLate: (() => void) | undefined;
    const gate = gatedRunnerModule(root, () => { unregisterLate = registerAgentRunner(runner.adapter); });
    let releaseMetadata!: () => void, enteredMetadata!: () => void, enteredManager!: () => void;
    const metadataHeld = new Promise<void>(resolve => { releaseMetadata = resolve; });
    const metadataEntered = new Promise<void>(resolve => { enteredMetadata = resolve; });
    const managerEntered = new Promise<void>(resolve => { enteredManager = resolve; });
    try {
      const kept = await first.spawn({ task: "visible while recovering", runner: runner.adapter.id, residency: "durable" });
      const foreign = await first.spawn({ task: "foreign metadata is not visibility authority", runner: runner.adapter.id, residency: "durable" });
      const invalid = await first.spawn({ task: "invalid metadata is not visibility authority", runner: runner.adapter.id, residency: "durable" });
      const absent = "0".repeat(32); // no metadata AND no retained run; not the legitimate crash window
      await first.close();
      expect(kept.status).toBe("running");
      runner.unregister();
      const metadata = (handle: typeof kept): ResidentAgentMetadata => ({
        format: 1, rootId: config.rootId, id: handle.id, handle,
        runDirectory: path.join(runs, handle.id), createdAt: 1, updatedAt: 1,
      });
      saveMetadata(config, { ...metadata(kept), runnerModule: gate.module,
        handle: { ...kept, attachCommand: "untrusted metadata attach command", sessionId: "untrusted-session",
          runnerSessionId: "untrusted-runner-session", recursive: true, spawner: { id: "session:forged", kind: "main" },
          capabilityRequirements: ["untrusted-capability"], capabilityDigest: "untrusted-digest" } });
      saveMetadata(config, { ...metadata(foreign), rootId: "session:foreign" });
      saveMetadata(config, { ...metadata(invalid), handle: { ...invalid, id: absent } });
      const agents = path.join(config.residencyRoot, "agents");
      const scans = { opens: 0, listings: 0, entries: 0 };
      let held = false;
      const opendir = fs.promises.opendir, readdir = fs.readdirSync, asyncReaddir = fs.promises.readdir;
      vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
        const directory = await opendir(...args);
        if (String(args[0]) === agents) {
          scans.opens++;
          const read = directory.read.bind(directory);
          vi.spyOn(directory, "read").mockImplementation((async () => {
            if (!held) { held = true; enteredMetadata(); await metadataHeld; }
            const entry = await read();
            if (entry) scans.entries++;
            return entry;
          }) as typeof directory.read);
        }
        return directory;
      });
      vi.spyOn(fs, "readdirSync").mockImplementation(((...args: Parameters<typeof fs.readdirSync>) => {
        if (String(args[0]) === agents) scans.listings++;
        return readdir(...args);
      }) as typeof fs.readdirSync);
      vi.spyOn(fs.promises, "readdir").mockImplementation(((...args: Parameters<typeof fs.promises.readdir>) => {
        if (String(args[0]) === agents) scans.listings++;
        return asyncReaddir(...args);
      }) as typeof fs.promises.readdir);
      const recovery = observeHostedRecovery(manager => {
        manager.pauseForRelease(true); // real pre-adoption release gate; do not replace recovery
        enteredManager();
      });
      await host.start();
      await metadataEntered;
      expect(scans.entries).toBe(0);
      expect(recovery.fallbacks).toHaveLength(0);
      // Warm the empty UI cache before a direct known-ID lookup populates the
      // read-only projection. The lookup must not walk or advance the cursor.
      const beforeQueryUi = host.agents.listForUi();
      expect(beforeQueryUi).toEqual([]); // partial is readable query metadata, not an enumerable legacy-array entry
      expect(beforeQueryUi.some(row => row.id === kept.id)).toBe(false);
      const beforeQuery = { ...scans };
      const pending = {
        id: kept.id, transport: "hosted", residency: "durable", status: "finishing",
        recovery: { state: "pending", lastKnownStatus: "running" },
      };
      if (boundary === "before metadata discovery") {
        const pendingStatus = host.agents.status(kept.id); // red-on-base: Unknown Fabric agent, not just a missing partial marker
        expect(pendingStatus).toMatchObject(pending);
        expect(beforeQueryUi.partial).toBe(true);
        for (const field of ["attachCommand", "sessionId", "runnerSessionId", "sessionFile", "recursive", "spawner", "capabilityRequirements", "capabilityDigest"]) {
          expect(pendingStatus).not.toHaveProperty(field); // metadata grants observation, never execution/attribution capabilities
        }
        expect(host.agents.listForUi().partial).toBe(true);
        expect(host.agents.listForUi().find(row => row.id === kept.id)).toMatchObject(pending);
      }
      for (const id of [absent, foreign.id, invalid.id]) {
        expect(() => host.agents.status(id)).toThrow(/Unknown Fabric agent/);
        expect(host.agents.listForUi().some(row => row.id === id)).toBe(false);
      }
      expect(scans).toEqual(beforeQuery);
      expect(scans.entries).toBe(0);
      expect(runner.calls.attaches).toEqual([]);
      expect(runner.calls.starts).toEqual([kept.id, foreign.id, invalid.id]);
      expect(unregisterLate).toBeUndefined(); // direct lookup did not import its runner

      releaseMetadata();
      await gate.entered;
      expect(recovery.fallbacks).toHaveLength(0);
      expect(runner.calls.attaches).toEqual([]);
      gate.release();
      await gate.finished;
      await managerEntered;
      expect(recovery.fallbacks).toHaveLength(1);
      expect(unregisterLate).toBeTypeOf("function"); // imports really finished
      // This is the reported P1 seam: imports are done, the actual manager
      // pass exists, but its adoption/attach path is held at entry.
      // Capture UI BEFORE status: the after-imports case never queried kept,
      // so this also proves incremental discovery populated the list itself.
      const midRecoveryUi = host.agents.listForUi();
      expect(host.agents.status(kept.id)).toMatchObject(pending); // independent mid-recovery red-on-base Unknown
      expect(midRecoveryUi.partial).toBe(true);
      expect(midRecoveryUi.filter(row => row.id === kept.id)).toEqual([expect.objectContaining(pending)]);
      for (const id of [absent, foreign.id, invalid.id]) {
        expect(() => host.agents.status(id)).toThrow(/Unknown Fabric agent/);
        expect(host.agents.listForUi().some(row => row.id === id)).toBe(false);
      }
      expect(runner.calls.attaches).toEqual([]);
      expect(runner.calls.starts).toEqual([kept.id, foreign.id, invalid.id]);
      host.agents.resumeAfterRelease();
      await recovery.complete(); // joins real adoption, attach, and host finally custody
      const adopted = host.agents.status(kept.id);
      expect(adopted).toMatchObject({ id: kept.id, transport: "hosted", status: "running" });
      expect(adopted).not.toHaveProperty("recovery");
      const completeUi = host.agents.listForUi();
      expect(completeUi.partial).toBe(false);
      const adoptedUi = completeUi.filter(row => row.id === kept.id);
      expect(adoptedUi).toHaveLength(1);
      expect(adoptedUi[0]).toMatchObject({ id: kept.id, transport: "hosted", status: "running" });
      expect(adoptedUi[0]).not.toHaveProperty("recovery"); // cached pending row was invalidated
      expect(runner.calls.attaches).toEqual([kept.id]);
      expect(runner.calls.starts).toEqual([kept.id, foreign.id, invalid.id]);
      for (const handle of [foreign, invalid]) {
        expect(() => host.agents.status(handle.id)).toThrow(/Unknown Fabric agent/);
        expect(host.agents.listForUi().some(row => row.id === handle.id)).toBe(false);
        expect(JSON.parse(fs.readFileSync(path.join(runs, handle.id, "status.json"), "utf8"))).toMatchObject({ status: "running" });
      }
      expect(runner.calls.stops).toBe(0);
    } finally {
      releaseMetadata();
      await gate.finish();
      await host.close(); await first.close();
      vi.restoreAllMocks();
      unregisterLate?.(); runner.unregister(); gate.dispose();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // Keep reason: a single oversized metadata row must not bypass the host's
  // cooperative slice budget or turn skipped metadata into orphan authority.
  it("rejects oversized metadata before pathname or descriptor payload reads without changing the live run", async () => {
    const { root, config, runs } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    try {
      const handle = await first.spawn({ task: "oversized metadata remains unknown", runner: runner.adapter.id, residency: "durable" });
      await first.close();
      const agents = path.join(config.residencyRoot, "agents");
      fs.mkdirSync(agents, { recursive: true });
      const file = path.join(agents, `${handle.id}.json`);
      const source = JSON.stringify({ format: 1, rootId: config.rootId, id: handle.id, handle,
        runDirectory: path.join(runs, handle.id), createdAt: 1, updatedAt: 1,
        padding: "x".repeat(1024 * 1024 + 1) });
      fs.writeFileSync(file, source);
      expect(fs.statSync(file).size).toBeGreaterThan(1024 * 1024);
      const statusFile = path.join(runs, handle.id, "status.json");
      const beforeStatus = fs.readFileSync(statusFile, "utf8");
      const hostedFile = path.join(runs, handle.id, "hosted.json");
      const beforeHosted = fs.readFileSync(hostedFile, "utf8");
      const reads = observeMetadataReads(file);
      let parses = 0;
      const parse = JSON.parse;
      vi.spyOn(JSON, "parse").mockImplementation((text, reviver) => {
        if (text === source) parses++;
        return parse(text, reviver);
      });
      const warnings = vi.spyOn(diagnostics, "warn").mockImplementation(() => undefined);
      const recovery = observeHostedRecovery();
      await host.start();
      await recovery.complete();
      expect(reads.reads).toBe(0); // red-on-base: payload was read; includes readFileSync(fd), readSync(fd), and readvSync(fd)
      expect(reads.opens).toBeGreaterThan(0); // receiver encountered the real oversized file
      expect(parses).toBe(0);
      expect(warnings).toHaveBeenCalledTimes(1); // one host-wide diagnostic, not once per revalidation
      expect(warnings.mock.calls[0]!.map(String).join(" ")).toMatch(/metadata/i);
      expect(warnings.mock.calls[0]!.map(String).join(" ")).toMatch(/oversiz|too large|exceed|size|byte|limit/i);
      expect(() => host.agents.status(handle.id)).toThrow(/Unknown Fabric agent/);
      expect(host.agents.listForUi().some(row => row.id === handle.id)).toBe(false);
      expect(reads.reads).toBe(0); // direct known-ID query also obeys the byte cap
      expect(parses).toBe(0);
      expect(warnings).toHaveBeenCalledTimes(1);
      expect(fs.readFileSync(statusFile, "utf8")).toBe(beforeStatus);
      expect(fs.statSync(file).size).toBe(Buffer.byteLength(source));
      expect(fs.readFileSync(hostedFile, "utf8")).toBe(beforeHosted);
      expect(runner.calls.attaches).toEqual([]);
      expect(runner.calls.starts).toEqual([handle.id]);
      expect(runner.calls.stops).toBe(0);
    } finally {
      vi.restoreAllMocks();
      await host.close(); await first.close();
      runner.unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("attaches metadata-less crash-window runs and result-deferred metadata only after readiness, once", async () => {
    const { root, config, runs } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    try {
      const orphan = await first.spawn({ task: "started before metadata publication", runner: runner.adapter.id, residency: "durable" });
      const deferred = await first.spawn({ task: "result presence is not terminal authority", runner: runner.adapter.id, residency: "durable" });
      await first.close();
      const imported = path.join(root, "runner-module-imported");
      const runnerModule = path.join(root, "recorded-runner.mjs");
      fs.writeFileSync(runnerModule, `import fs from "node:fs"; fs.writeFileSync(${JSON.stringify(imported)}, "registered");`);
      saveMetadata(config, { format: 1, rootId: config.rootId, id: deferred.id, handle: deferred, runnerModule,
        runDirectory: path.join(runs, deferred.id), createdAt: 1, updatedAt: 1 });
      const result = residentResultPath(config.residencyRoot, deferred.id);
      fs.mkdirSync(path.dirname(result), { recursive: true });
      fs.writeFileSync(result, JSON.stringify({ id: deferred.id, status: "completed" }));
      const work = observeRuns(runs);
      const recovery = observeHostedRecovery();
      await host.start();
      expect(runner.calls.attaches).toEqual([]);
      expect(work.statusReads).toBe(0); // retained metadata + result does not eagerly read its archived status
      expect(fs.existsSync(imported)).toBe(false); // imports too are post-ready
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(work.opens).toBe(0);
      expect(runner.calls.attaches).toEqual([]);
      await recovery.complete();
      expect(fs.readFileSync(imported, "utf8")).toBe("registered"); // deferral retains admitted module imports
      expect(new Set(runner.calls.attaches)).toEqual(new Set([orphan.id, deferred.id]));
      expect(runner.calls.attaches).toHaveLength(2);
      expect(host.agents.status(orphan.id)).toMatchObject({ status: "running", transport: "hosted" });
      expect(host.agents.status(deferred.id)).toMatchObject({ status: "running", transport: "hosted" });
      expect(fs.existsSync(result)).toBe(true); // deferral never authorizes result/tree removal
      expect(fs.existsSync(path.join(runs, deferred.id, "hosted.json"))).toBe(true);
      await new Promise<void>(resolve => setTimeout(resolve, 250)); // later existing maintenance ticks
      expect(recovery.fallbacks).toHaveLength(1);
      expect(work.listings).toBe(0);
      expect(runner.calls.starts).toEqual([orphan.id, deferred.id]);
      expect(runner.calls.stops).toBe(0);
    } finally {
      vi.restoreAllMocks();
      await host.close();
      await first.close();
      runner.unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("does not turn torn, unreadable, wrong-ID or unsafe metadata into orphan authority", async () => {
    const { root, config, runs } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    const kinds = ["torn", "unreadable", "wrong id", "wrong handle", "outside run root", "directory",
      ...(process.platform === "win32" ? [] : ["symlink"])];
    try {
      const handles = [];
      for (const kind of kinds) handles.push(await first.spawn({ task: kind, runner: runner.adapter.id, residency: "durable" }));
      await first.close();
      const agents = path.join(config.residencyRoot, "agents");
      fs.mkdirSync(agents, { recursive: true });
      let unreadable = "";
      for (const [index, handle] of handles.entries()) {
        const kind = kinds[index];
        const file = path.join(agents, `${handle.id}.json`);
        const metadata: ResidentAgentMetadata = { format: 1, rootId: config.rootId, id: handle.id, handle,
          runDirectory: path.join(runs, handle.id), createdAt: 1, updatedAt: 1 };
        if (kind === "wrong id") metadata.id = "0".repeat(32);
        if (kind === "wrong handle") metadata.handle = { ...handle, id: "0".repeat(32) };
        if (kind === "outside run root") metadata.runDirectory = path.join(root, "foreign", handle.id);
        if (kind === "directory") fs.mkdirSync(file);
        else if (kind === "symlink") {
          const target = path.join(root, "linked-metadata.json");
          fs.writeFileSync(target, JSON.stringify(metadata));
          fs.symlinkSync(target, file);
        } else fs.writeFileSync(file, kind === "torn" ? "{torn" : JSON.stringify(metadata));
        if (kind === "unreadable") unreadable = file;
      }
      const unreadableReads = observeMetadataReads(unreadable, true);
      const recovery = observeHostedRecovery();
      await host.start();
      await recovery.complete();
      expect(runner.calls.attaches).toEqual([]);
      expect(runner.calls.starts).toEqual(handles.map(handle => handle.id));
      expect(unreadableReads.failures).toBeGreaterThan(0); // EACCES reached the actual receiver, including descriptor-based implementations
      for (const handle of handles) {
        expect(() => host.agents.status(handle.id)).toThrow(/Unknown Fabric agent/);
        expect(host.agents.listForUi().some(row => row.id === handle.id)).toBe(false);
        expect(JSON.parse(fs.readFileSync(path.join(runs, handle.id, "status.json"), "utf8"))).toMatchObject({ status: "running" });
      }
    } finally {
      vi.restoreAllMocks();
      await host.close();
      await first.close();
      runner.unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // Keep reason: close cannot await an admission whose release wait is only
  // canceled later by agents.close(). Exercise the real manager hold and signal.
  it("closes while fallback recovery is waiting on the manager release gate", { timeout: 10_000 }, async () => {
    const { root, config } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      const orphan = await first.spawn({ task: "held recovery", runner: runner.adapter.id, residency: "durable" });
      await first.close();
      const recovery = observeHostedRecovery(manager => manager.pauseForRelease(true));
      await host.start();
      await vi.waitFor(() => expect(recovery.fallbacks).toHaveLength(1), { timeout: 5_000 });
      expect(runner.calls.attaches).toEqual([]);
      const closed = await Promise.race([
        host.close().then(() => true),
        new Promise<boolean>(resolve => { deadline = setTimeout(() => resolve(false), 2_000); }),
      ]);
      expect(closed).toBe(true);
      expect(recovery.fallbacks[0]!.options?.signal?.aborted).toBe(true);
      expect(runner.calls.attaches).toEqual([]);
      expect(runner.calls.starts).toEqual([orphan.id]);
    } finally {
      if (deadline) clearTimeout(deadline);
      host.agents?.resumeAfterRelease(); // retire a regressed close without hanging fixture teardown
      await host.close();
      await first.close();
      vi.restoreAllMocks();
      runner.unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("retains interrupted discovery for a fresh pass after the lease gate resumes", { timeout: 10_000 }, async () => {
    const { root, config, runs } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    let releaseRead: (() => void) | undefined;
    let releaseRefresh: (() => void) | undefined;
    let restoreRefresh: (() => void) | undefined;
    const refreshes: Promise<void>[] = [];
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
    let entered = false;
    try {
      const orphan = await first.spawn({ task: "lease-fenced recovery", runner: runner.adapter.id, residency: "durable" });
      await first.close();
      const recovery = observeHostedRecovery();
      const opendir = fs.promises.opendir;
      vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
        const directory = await opendir(...args);
        // Only hold the host fallback's cursor, not a different startup component.
        if (String(args[0]) === runs && recovery.fallbacks.length > 0 && !entered) {
          const read = directory.read.bind(directory);
          const held = new Promise<void>(resolve => { releaseRead = resolve; });
          vi.spyOn(directory, "read").mockImplementation((async () => {
            if (!entered) { entered = true; await held; }
            return read();
          }) as typeof directory.read);
        }
        return directory;
      });
      await host.start();
      await vi.waitFor(() => expect(entered).toBe(true), { timeout: 5_000 });
      expect(recovery.fallbacks).toHaveLength(1);
      expect(host.participants.canConsumeMesh()).toBe(true);
      // Keep the actual heartbeat renewal behind a gate: canConsumeMesh's
      // forward-clock recovery must not heal the stale receipt before we assert.
      const refresh = host.participants.refresh.bind(host.participants);
      const heldRefresh = new Promise<void>(resolve => { releaseRefresh = resolve; });
      const refreshing = vi.spyOn(host.participants, "refresh").mockImplementation(() => {
        const pending = heldRefresh.then(() => refresh());
        refreshes.push(pending);
        void pending.catch(() => undefined);
        return pending;
      });
      restoreRefresh = () => refreshing.mockRestore();
      const samples = vi.spyOn(host.actors, "hasOwnedActors");
      samples.mockClear();
      now += 10_001; // real heartbeat freshness is two 5s intervals
      expect(host.participants.canConsumeMesh()).toBe(false);
      releaseRead!();
      await expect(recovery.fallbacks[0]!.pending).rejects.toThrow(/interrupt/i);
      await new Promise<void>(resolve => setImmediate(resolve)); // host completion/finally custody
      expect(runner.calls.attaches).toEqual([]);
      // More than two ordinary 100ms maintenance polls with recovery settled:
      // stale authority keeps discovery debt, so idle must not sample actors.
      await new Promise<void>(resolve => setTimeout(resolve, 250));
      expect(host.participants.canConsumeMesh()).toBe(false);
      expect(samples).not.toHaveBeenCalled();
      expect(recovery.fallbacks).toHaveLength(1);
      releaseRefresh!();
      restoreRefresh();
      await Promise.all(refreshes);
      await host.participants.refresh();
      expect(host.participants.canConsumeMesh()).toBe(true);
      await recovery.complete(1);
      expect(recovery.fallbacks).toHaveLength(2);
      expect(runner.calls.attaches).toEqual([orphan.id]);
      expect(runner.calls.starts).toEqual([orphan.id]);
    } finally {
      releaseRead?.();
      releaseRefresh?.();
      restoreRefresh?.();
      await Promise.allSettled(refreshes);
      clock.mockRestore();
      vi.restoreAllMocks();
      await host.close();
      await first.close();
      runner.unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["startup", "strict release resume"] as const)(
    "imports completed non-hosted metadata before launching a restored native actor after %s", { timeout: 20_000 }, async boundary => {
    const { root, config, runs } = fixture();
    config.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    config.piModels = { available: [{ provider: "test", id: "visible" }], aliases: {}, defaultModel: "test/visible" };
    const previous = new ResidentHost(config);
    const host = new ResidentHost(config);
    let registered = false;
    const gate = gatedRunnerModule(root, () => { registered = true; });
    try {
      await previous.start();
      const actor = await previous.actors.create({ name: "module-bootstrap", instructions: "Respond", residency: "durable" });
      previous.actors.pauseForRelease();
      previous.actors.tell(actor.id, "accepted before restart", undefined);
      await previous.close();
      const id = "f".repeat(32);
      saveMetadata(config, { format: 1, rootId: config.rootId, id, runnerModule: gate.module,
        handle: { id, name: "consumed native admission", status: "completed", runner: "pi", transport: "process", cwd: root },
        runDirectory: path.join(runs, id), createdAt: 1, updatedAt: 2, completionConsumedAt: 2 });
      if (boundary === "strict release resume") {
        // Inject only the native strict hold, at the last pre-resume seam.
        // The existing drain-endpoint timeout test exercises the real deadline;
        // this counterexample keeps a real import and restored actor pending
        // across the shared host resume helper without exposing private state.
        const recover = ActorDirectory.prototype.recoverProtectedActors;
        vi.spyOn(ActorDirectory.prototype, "recoverProtectedActors").mockImplementation(async function (this: ActorDirectory) {
          await recover.call(this);
          this.pauseForRelease({ strict: true });
        });
      }
      const launch = ProcessTransport.prototype.launch;
      const native = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(function (this: ProcessTransport, request) {
        expect(registered).toBe(true);
        return launch.call(this, request);
      });
      await startBeforeImport(host, gate);
      await gate.entered;
      expect(registered).toBe(false);
      expect(native).not.toHaveBeenCalled();
      expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(0);
      if (boundary === "strict release resume") {
        expect(host.actors.releaseAdmissionHeld).toBe(false); // strict drain fence, not bootstrap hold
        const admitted = await host.actors.create({ name: "after-strict-resume", instructions: "Wait", residency: "durable" });
        expect(admitted.name).toBe("after-strict-resume");
        await new Promise<void>(resolve => setImmediate(resolve));
        expect(registered).toBe(false);
        expect(native).not.toHaveBeenCalled();
        expect(host.actors.messages(actor.id).filter(message => message.error)).toHaveLength(0);
        expect(host.actors.status(actor.id).lastError).toBeUndefined();
      }
      gate.release();
      await gate.finished;
      await vi.waitFor(() => expect(host.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1), { timeout: 10_000 });
      expect(native).toHaveBeenCalledOnce();
    } finally {
      await gate.finish();
      vi.restoreAllMocks();
      await host.close(); await previous.close(); gate.dispose();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  // Keep reason: selection-to-attach crosses a module await and an owner lease.
  // A still-own-root replacement is NOT the exact row originally admitted.
  it.each(["live unchanged", "lease lost", "metadata changed", "close"] as const)(
    "fences a real paused runner import against %s", { timeout: 15_000 }, async outcome => {
      const { root, config, runs } = fixture();
      const runner = hostedRunner();
      const first = seedManager(config);
      const host = new ResidentHost(config);
      let unregisterLate: (() => void) | undefined;
      const gate = gatedRunnerModule(root, () => { unregisterLate = registerAgentRunner(runner.adapter); });
      let deadline: ReturnType<typeof setTimeout> | undefined;
      try {
        const handle = await first.spawn({ task: "import-boundary recovery", runner: runner.adapter.id, residency: "durable" });
        await first.close();
        runner.unregister();
        const metadata: ResidentAgentMetadata = { format: 1, rootId: config.rootId, id: handle.id, handle,
          runnerModule: gate.module, runDirectory: path.join(runs, handle.id), createdAt: 1, updatedAt: 1 };
        saveMetadata(config, metadata);
        const recovery = observeHostedRecovery();
        const leaseAbort = new AbortController();
        let metadataCursorClosed = false;
        const opendir = fs.promises.opendir;
        vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
          const directory = await opendir(...args);
          if (String(args[0]) === path.join(config.residencyRoot, "agents")) {
            const close = directory.close.bind(directory);
            vi.spyOn(directory, "close").mockImplementation((async () => {
              await close(); metadataCursorClosed = true;
            }) as typeof directory.close);
          }
          return directory;
        });
        await startBeforeImport(host, gate);
        // Same host signal contract as NATS, with a deterministic lease-loss
        // event rather than waiting for a maintenance timer or import completion.
        if (outcome === "lease lost") vi.spyOn(host.participants, "transportLeaseSignal", "get").mockReturnValue(leaseAbort.signal);
        expect(runner.calls.attaches).toEqual([]);
        await gate.entered;
        expect(recovery.fallbacks).toHaveLength(0); // imports precede the SAME manager pass
        expect(runner.calls.attaches).toEqual([]);
        if (outcome === "lease lost") {
          vi.spyOn(host.participants, "canConsumeMesh").mockReturnValue(false);
          leaseAbort.abort(new Error("Owner lease revoked during module import"));
          await vi.waitFor(() => expect(metadataCursorClosed).toBe(true)); // import is STILL held
          expect(recovery.fallbacks).toHaveLength(0);
        } else if (outcome === "metadata changed") {
          saveMetadata(config, { ...metadata, updatedAt: 2 }); // remains syntactically own-root
        } else if (outcome === "close") {
          const closed = await Promise.race([host.close().then(() => true),
            new Promise<boolean>(resolve => { deadline = setTimeout(() => resolve(false), 2_000); })]);
          expect(closed).toBe(true); // must NOT force the module's top-level await first
        }
        gate.release();
        await gate.finished;
        if (outcome === "live unchanged" || outcome === "metadata changed") await recovery.complete();
        else await new Promise<void>(resolve => setImmediate(resolve));
        expect(runner.calls.attaches).toEqual(outcome === "live unchanged" ? [handle.id] : []);
        expect(runner.calls.starts).toEqual([handle.id]);
        if (outcome === "live unchanged") {
          await recovery.complete();
          expect(recovery.fallbacks).toHaveLength(1);
          expect(host.agents.status(handle.id)).toMatchObject({ transport: "hosted", status: "running" });
        }
      } finally {
        if (deadline) clearTimeout(deadline);
        await gate.finish();
        vi.restoreAllMocks();
        await host.close(); await first.close();
        unregisterLate?.(); runner.unregister(); gate.dispose();
        fs.rmSync(root, { recursive: true, force: true });
      }
    });

  it.each([
    { failure: "missing runner", metadata: true }, { failure: "damaged scope", metadata: true },
    { failure: "missing runner", metadata: false }, { failure: "damaged scope", metadata: false },
  ] as const)("keeps hosted recovery fail-closed for $failure (metadata=$metadata)", async ({ failure, metadata }) => {
    const { root, config, runs } = fixture();
    const runner = hostedRunner();
    const first = seedManager(config);
    const host = new ResidentHost(config);
    try {
      const scope = normalizeScope({ principal: { id: "runsboot-service" }, grants: [{ resource: "fs:/repo/**", actions: ["read"] }] });
      const handle = await first.spawn({ task: "recover safely", runner: runner.adapter.id, residency: "durable", inheritedScope: scope });
      await first.close();
      if (metadata) saveMetadata(config, { format: 1, rootId: config.rootId, id: handle.id, handle,
        runDirectory: path.join(runs, handle.id), createdAt: 1, updatedAt: 1 });
      if (failure === "missing runner") runner.unregister();
      else {
        const stateFile = path.join(runs, handle.id, "hosted.json");
        const state = JSON.parse(fs.readFileSync(stateFile, "utf8"));
        state.context.scope.grants = [{ resource: "fs:*", actions: ["write"] }];
        fs.writeFileSync(stateFile, JSON.stringify(state));
      }
      const recovery = observeHostedRecovery();
      await host.start();
      await recovery.complete();
      const record = JSON.parse(fs.readFileSync(path.join(runs, handle.id, "status.json"), "utf8"));
      expect(record).toMatchObject({ status: "failed", outcome: "indeterminate" });
      expect(record.error).toMatch(failure === "missing runner" ? /not registered/ : /scope is invalid/);
      expect(runner.calls.attaches).toEqual([]);
      expect(runner.calls.starts).toEqual([handle.id]);
    } finally {
      vi.restoreAllMocks();
      await host.close();
      await first.close();
      runner.unregister();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
