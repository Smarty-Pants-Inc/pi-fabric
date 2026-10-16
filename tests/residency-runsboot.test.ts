import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentResultPath, type ResidentAgentMetadata, type ResidentHostConfig } from "../src/residency/protocol.js";
import { readHostLease } from "../src/topology/host-leases.js";
import { retentionV2Enabled } from "../src/storage/retention-platform.js";
import { normalizeScope } from "../src/scope.js";
import { registerAgentRunner, type FabricHostedRunner } from "../src/runners.js";

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
const observeRuns = (runs: string) => {
  const work = { listings: 0, opens: 0, entries: 0, reads: 0, statusReads: 0, stats: 0 };
  const within = (target: unknown) => String(target) === runs || String(target).startsWith(runs + path.sep);
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
    if (String(args[0]) === runs) work.listings++;
    return readdir(...args);
  }) as typeof fs.readdirSync);
  vi.spyOn(fs, "opendirSync").mockImplementation((...args: Parameters<typeof fs.opendirSync>) => {
    if (String(args[0]) === runs) work.opens++;
    const directory = opendir(...args);
    return String(args[0]) === runs ? observeCursor(directory) : directory;
  });
  vi.spyOn(fs.promises, "readdir").mockImplementation(((...args: Parameters<typeof fs.promises.readdir>) => {
    if (String(args[0]) === runs) work.listings++;
    return asyncReaddir(...args);
  }) as typeof fs.promises.readdir);
  vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
    if (String(args[0]) === runs) work.opens++;
    const directory = await asyncOpendir(...args);
    return String(args[0]) === runs ? observeCursor(directory) : directory;
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

/** Observe and join the real manager operation, including its directory cursor. */
const observeHostedRecovery = () => {
  const recover = AgentManager.prototype.recoverHostedRuns;
  const fallbacks: Array<{ manager: AgentManager; options: Parameters<AgentManager["recoverHostedRuns"]>[1]; pending: Promise<string[]> }> = [];
  vi.spyOn(AgentManager.prototype, "recoverHostedRuns").mockImplementation(function (this: AgentManager, candidates, options) {
    const pending = recover.call(this, candidates, options);
    if (candidates === undefined) fallbacks.push({ manager: this, options, pending });
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

describe("resident retained-run startup (smarty-dev#4796)", () => {
  // Keep reason: the real 20k-hex-ID state catches the full startup walk that
  // the older archived-N fixture misses; retention work must stay incremental.
  it("reaches readiness and first idle with a persisted owned actor without walking 20k native runs", { timeout: 30_000 }, async () => {
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
    }
    const host = new ResidentHost(config);
    const work = observeRuns(runs);
    try {
      await host.start();
      const ready = { ...work };
      expect(readHostLease(config.meshRoot, host.hostId)?.expiresAt).toBeGreaterThan(Date.now());
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "maintenance-ready.json"), "utf8"))).toMatchObject({ token: host.generation });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(ready).toEqual({ listings: 0, opens: 0, entries: 0, reads: 0, statusReads: 0, stats: 0 });
      // An owned actor's first ready-gated maintenance may open its native
      // recovery cursor by first idle. Count async work too: it must remain a
      // bounded first slice, never the historical startup sweep.
      expect(work.listings).toBe(0);
      expect(work.opens).toBeLessThanOrEqual(1);
      expect(work.entries).toBeLessThanOrEqual(64);
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
      expect(runner.calls.attaches).toEqual([kept.id]);
      expect(runner.calls.starts).toEqual([kept.id, foreign.id]);
      expect(host.agents.status(kept.id)).toMatchObject({ status: "running", transport: "hosted" });
      expect(work.listings).toBe(0);
      expect(work.statusReads).toBeGreaterThan(0);
      expect(vi.mocked(fs.readFileSync).mock.calls.some(([file]) => String(file) === path.join(runs, foreign.id, "status.json"))).toBe(false);
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
      expect(fs.readFileSync(imported, "utf8")).toBe("registered"); // deferral must retain admitted module imports
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(work.opens).toBe(0);
      expect(runner.calls.attaches).toEqual([]);
      await recovery.complete();
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
      const read = fs.readFileSync;
      vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
        if (String(args[0]) === unreadable) throw Object.assign(new Error("metadata unreadable"), { code: "EACCES" });
        return read(...args);
      }) as typeof fs.readFileSync);
      const recovery = observeHostedRecovery();
      await host.start();
      await recovery.complete();
      expect(runner.calls.attaches).toEqual([]);
      expect(runner.calls.starts).toEqual(handles.map(handle => handle.id));
      for (const handle of handles) {
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
      const recovery = observeHostedRecovery();
      await host.start();
      host.agents.pauseForRelease(true);
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
      const lease = vi.spyOn(host.participants, "canConsumeMesh").mockReturnValue(false);
      releaseRead!();
      await expect(recovery.fallbacks[0]!.pending).rejects.toThrow(/interrupt/i);
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(runner.calls.attaches).toEqual([]);
      lease.mockRestore();
      await recovery.complete(1);
      expect(recovery.fallbacks).toHaveLength(2);
      expect(runner.calls.attaches).toEqual([orphan.id]);
      expect(runner.calls.starts).toEqual([orphan.id]);
    } finally {
      releaseRead?.();
      vi.restoreAllMocks();
      await host.close();
      await first.close();
      runner.unregister();
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
