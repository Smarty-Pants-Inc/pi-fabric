import fs from "node:fs";
import { createHash } from "node:crypto";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const managers: ActorManager[] = [];
const agents: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  await Promise.all(agents.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function fixture(count: number) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actor-startup-")); roots.push(root);
  const actorRoot = path.join(root, "actors"); fs.mkdirSync(actorRoot);
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  const identity: MeshIdentity = { id: "session:startup", name: "main", kind: "main", sessionId: "startup" };
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "worker-runs"),
  }); agents.push(manager);
  // Isolate presence publication from the retention/registry slice being tested.
  vi.spyOn(mesh, "put").mockResolvedValue({ key: "presence", value: {}, version: 1, updatedAt: Date.now(), updatedBy: identity });
  const old = Date.now() - 10 * 24 * 60 * 60 * 1_000;
  const records = Array.from({ length: count }, (_, i) => {
    const id = i.toString(16).padStart(32, "0"), directory = path.join(actorRoot, id);
    for (let run = 0; run < 10; run++) {
      const dir = path.join(directory, "runs", `run-${i}-${run}`); fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483646", finishedAt: old }));
      fs.writeFileSync(path.join(dir, "events.jsonl"), '{"type":"complete"}\n');
      fs.writeFileSync(path.join(dir, "task.txt"), "fixture task");
    }
    return { id, name: `actor-${i}`, rootId: identity.id, instructions: "Review.\n".repeat(1_400), status: "stopped",
      events: ["input", "turn_end", "agent_settled"], topics: ["fleet.work", "fleet.review"], residency: "session",
      delivery: "mailbox", runner: "pi", responseMode: "text", requirements: [], createdAt: old,
      lastRunId: `run-${i}-9`, messages: Array.from({ length: 3 }, (_, j) => ({ id: `message-${i}-${j}`, source: "direct",
        createdAt: old, actorId: id, actorName: `actor-${i}`, direction: "in", text: "A retained message.".repeat(20) })) };
  });
  new ActorRegistryStore(actorRoot).write(records);
  const make = (options: ConstructorParameters<typeof ActorManager>[6] = {}) => {
    const value = new ActorManager("startup", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, manager, () => {}, {
      actorRoot, persistent: true, rootId: identity.id, claimResidency: "session", reapDeadSessionPresence: false, ...options,
    }); managers.push(value); return value;
  };
  const runDir = (actor: number, run: number) => path.join(actorRoot, records[actor]!.id, "runs", `run-${actor}-${run}`);
  return { actorRoot, records, make, runDir, agents: manager };
}

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
async function eventually(predicate: () => boolean) {
  const deadline = Date.now() + 10_000;
  while (!predicate()) { if (Date.now() >= deadline) throw new Error("Retention did not complete"); await turn(); }
}

describe("ActorManager bounded startup (#4250 item 4)", () => {
  it.each(["native", "win32"] as const)("loads 200 real-shaped actors in one linear read and eventually sweeps 2,000 runs in slices below 250ms (%s)", async mode => {
    const f = fixture(200);
    const platform = mode === "win32" ? vi.spyOn(process, "platform", "get").mockReturnValue("win32") : undefined;
    const rawOwnership = vi.fn(() => true);
    const snapshot = vi.fn(() => new Map(f.records.map((record) => [record.id, true])));
    const read = vi.spyOn(ActorRegistryStore.prototype, "read");
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    await turn();
    let previous = performance.now(), longest = 0, active = true;
    let prunedSinceBeat = 0, largestBatch = 0;
    const prune = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(function (this: ActorLogStore, ...args) {
      prunedSinceBeat++; return prune.apply(this, args);
    });
    const queued = ActorLogStore.prototype.pruneRunsAsync;
    vi.spyOn(ActorLogStore.prototype, "pruneRunsAsync").mockImplementation(function(this: ActorLogStore, ...args) {
      prunedSinceBeat++; return queued.apply(this, args);
    });
    let deletedSinceBeat = 0, largestRunBatch = 0;
    const remove = fs.rmSync;
    vi.spyOn(fs, "rmSync").mockImplementation((...args) => {
      if (/[/\\\\]runs[/\\\\]run-\d+-\d+$/.test(String(args[0]))) deletedSinceBeat++;
      return remove(...args);
    });
    const removeAsync = fs.promises.rm;
    vi.spyOn(fs.promises, "rm").mockImplementation(async (...args) => {
      if (/[/\\\\]runs[/\\\\]run-\d+-\d+$/.test(String(args[0]))) deletedSinceBeat++;
      return removeAsync(...args);
    });
    let heartbeat: NodeJS.Immediate;
    const beat = () => {
      const now = performance.now(); longest = Math.max(longest, now - previous); previous = now;
      largestBatch = Math.max(largestBatch, prunedSinceBeat); prunedSinceBeat = 0;
      largestRunBatch = Math.max(largestRunBatch, deletedSinceBeat); deletedSinceBeat = 0;
      if (active) heartbeat = setImmediate(beat);
    };
    heartbeat = setImmediate(beat);
    try {
      const started = performance.now();
      f.make({ canManageActor: rawOwnership, snapshotActorOwnership: snapshot });
      const construction = performance.now() - started;
      expect(construction).toBeLessThan(250);
      expect(read).toHaveBeenCalledTimes(1);
      expect(write).not.toHaveBeenCalled();
      expect(snapshot).toHaveBeenCalledTimes(1);
      expect(rawOwnership).not.toHaveBeenCalled();
      // No archive traversal or deletion on the constructor stack.
      expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
      await eventually(() => !fs.existsSync(f.runDir(199, 8)));
      await turn();
      expect(longest).toBeLessThan(250);
      expect(largestBatch).toBeGreaterThan(0);
      expect(largestBatch).toBeLessThanOrEqual(process.platform === "win32" ? 1 : 8);
      if (process.platform === "win32") expect(largestRunBatch).toBeLessThanOrEqual(1);
      for (let actor = 0; actor < 200; actor++) {
        for (let run = 0; run < 9; run++) expect(fs.existsSync(f.runDir(actor, run))).toBe(false);
        expect(fs.existsSync(f.runDir(actor, 9))).toBe(true); // lastRunId fence survives
      }
      process.stdout.write(JSON.stringify({ probe: "ActorManager-200-2000", mode, platform: process.platform, constructorMs: construction, longestSliceMs: longest, largestBatch, largestRunBatch }) + "\n");
    } finally { active = false; clearImmediate(heartbeat!); platform?.mockRestore(); }
  });

  it("uses fresh single actor ownership checks instead of full-fleet snapshots on each Windows run", async () => {
    const f = fixture(200);
    let runPhase = false, fullFleetSnapshots = 0, copiedDecisions = 0, singleActorChecks = 0;
    const queued = ActorLogStore.prototype.pruneRunsAsync;
    vi.spyOn(ActorLogStore.prototype, "pruneRunsAsync").mockImplementation(async function(this: ActorLogStore, ...args) {
      runPhase = true;
      try { await queued.apply(this, args); } finally { runPhase = false; }
    });
    const snapshot = () => {
      if (runPhase && new Error().stack?.includes("sweepRetainedRuns")) {
        fullFleetSnapshots++; copiedDecisions += f.records.length;
      }
      return new Map(f.records.map(record => [record.id, true]));
    };
    const single = (_id: string, fresh = true) => {
      expect(fresh).toBe(true);
      if (runPhase) singleActorChecks++;
      return true;
    };
    // The fixture's agent manager is already constructed on the native host.
    // Select the actual Windows retention path, not Windows ACL admission.
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    try {
      f.make({ canManageActor: single, snapshotActorOwnership: snapshot });
      await eventually(() => !fs.existsSync(f.runDir(199, 8))); await turn();
      process.stdout.write(JSON.stringify({ probe: "Windows-200-actors-2000-runs-ownership", fullFleetSnapshots,
        copiedDecisions, singleActorChecks }) + "\n");
      expect(fullFleetSnapshots).toBe(0);
      expect(copiedDecisions).toBe(0);
      expect(singleActorChecks).toBeGreaterThanOrEqual(2_000);
      expect(fs.existsSync(f.runDir(199, 9))).toBe(true);
    } finally { platform.mockRestore(); }
  });

  it("bounds slow Windows run deletion inside a single actor below 250ms", async () => {
    const f = fixture(1);
    const native = process.platform;
    const platform = vi.spyOn(process, "platform", "get").mockImplementation(() =>
      new Error().stack?.includes("src/actors/manager.ts") ? "win32" : native);
    const remove = fs.promises.rm;
    let deleted = 0, sinceBeat = 0, largestBatch = 0, activeDeletes = 0, largestActive = 0;
    vi.spyOn(fs.promises, "rm").mockImplementation(async (...args) => {
      if (/[/\\\\]runs[/\\\\]run-\d+-\d+$/.test(String(args[0]))) {
        activeDeletes++; largestActive = Math.max(largestActive, activeDeletes);
        deleted++; sinceBeat++;
        await new Promise<void>(resolve => setTimeout(resolve, 35)); // Native work is off the RPC turn.
        try { return await remove(...args); } finally { activeDeletes--; }
      }
      return remove(...args);
    });
    let previous = performance.now(), longest = 0, active = true;
    let heartbeat: NodeJS.Immediate;
    const beat = () => {
      const now = performance.now(); longest = Math.max(longest, now - previous); previous = now;
      largestBatch = Math.max(largestBatch, sinceBeat); sinceBeat = 0;
      if (active) heartbeat = setImmediate(beat);
    };
    heartbeat = setImmediate(beat);
    try {
      f.make();
      await eventually(() => !fs.existsSync(f.runDir(0, 8))); await turn();
      expect(deleted).toBe(9);
      expect(largestActive).toBe(1);
      expect(longest).toBeLessThan(250);
      expect(largestBatch).toBe(1);
      expect(fs.existsSync(f.runDir(0, 9))).toBe(true);
    } finally { active = false; clearImmediate(heartbeat!); platform.mockRestore(); }
  });

  it("refreshes ownership between individual Windows runs, not just between actors", async () => {
    const f = fixture(1);
    let owned = true;
    const native = process.platform;
    const platform = vi.spyOn(process, "platform", "get").mockImplementation(() =>
      new Error().stack?.includes("src/actors/manager.ts") ? "win32" : native);
    try {
      f.make({ canManageActor: () => owned, snapshotActorOwnership: () => new Map([[f.records[0]!.id, owned]]) });
      await eventually(() => !fs.existsSync(f.runDir(0, 0)));
      owned = false;
      await turn(); await turn();
      for (let run = 1; run < 10; run++) expect(fs.existsSync(f.runDir(0, run))).toBe(true);
    } finally { platform.mockRestore(); }
  });

  it.each(["snapshot-only", "publication"] as const)("refreshes the %s fence between Windows run slices", async (fence) => {
    const f = fixture(1);
    let owned = true, published = true;
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    try {
      f.make({
        ...(fence === "publication" ? { canManageActor: () => owned } : {}),
        snapshotActorOwnership: () => new Map([[f.records[0]!.id, owned]]),
        canConsumeMesh: () => published,
      });
      await eventually(() => !fs.existsSync(f.runDir(0, 0)));
      if (fence === "snapshot-only") owned = false;
      else published = false;
      await turn(); await turn();
      for (let run = 1; run < 10; run++) expect(fs.existsSync(f.runDir(0, run))).toBe(true);
    } finally { platform.mockRestore(); }
  });

  it("keeps idle snapshots coalesced but rechecks canonical ownership for tell", () => {
    const f = fixture(1);
    let canonical = true;
    const rawOwnership = vi.fn(() => canonical);
    const snapshot = vi.fn((fresh = true) => new Map(f.records.map((record) => [record.id, fresh ? canonical : true])));
    const manager = f.make({ canManageActor: rawOwnership, snapshotActorOwnership: snapshot });
    expect(snapshot).toHaveBeenCalledExactlyOnceWith(true);
    canonical = false;
    snapshot.mockClear();
    expect(manager.listOwned().map((actor) => actor.id)).toContain(f.records[0]!.id);
    expect(snapshot).toHaveBeenCalledExactlyOnceWith(false);
    expect(rawOwnership).not.toHaveBeenCalled();
    snapshot.mockClear();
    expect(() => manager.tell(f.records[0]!.id, "must not be delivered")).toThrow("owned by another host");
    expect(snapshot).toHaveBeenCalledWith(true);
    expect(f.agents.list()).toEqual([]);
  });

  it("rechecks lease/cede/ownership after the initial yield and preserves live/unknown workers", async () => {
    const f = fixture(3);
    const ownership = new Map(f.records.map((record) => [record.id, true]));
    const manager = f.make({ canManageActor: (id) => ownership.get(id), snapshotActorOwnership: () => new Map(ownership) });
    const status = path.join(f.runDir(0, 0), "status.json");
    fs.writeFileSync(status, JSON.stringify({ status: "completed", transport: "process", sessionId: String(process.pid), finishedAt: 1 }));
    fs.writeFileSync(path.join(f.runDir(0, 1), "status.json"), JSON.stringify({ status: "completed", finishedAt: 1 }));
    ownership.set(f.records[1]!.id, false);
    await manager.cede(f.records[2]!.id);
    await eventually(() => !fs.existsSync(f.runDir(0, 8)));
    expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
    expect(fs.existsSync(f.runDir(0, 1))).toBe(true);
    expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
    expect(fs.existsSync(f.runDir(2, 8))).toBe(true);
  });

  it("reads a shared foreign lineage once per synchronous refresh, never once per actor", () => {
    const f = fixture(200);
    const lineageAlive = vi.fn(() => true);
    const manager = f.make({ rootId: "session:other", canManageActor: () => undefined,
      snapshotActorOwnership: () => new Map(), lineageAlive });
    expect(manager.listOwned()).toEqual([]);
    expect(lineageAlive).toHaveBeenCalledTimes(1);
    expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
  });

  it.each(["restore", "admission"] as const)("rejects expired context in a later batch before maintenance, but protects a fresh active snapshot (%s)", async (phase) => {
    const f = fixture(17), actor = f.records[16]!;
    new ActorRegistryStore(f.actorRoot).write(f.records.map((record) => ({ ...record, status: "idle" })));
    const sessionFile = path.join(f.actorRoot, actor.id, "session.jsonl");
    const store = new ActorChildCompletionStore(sessionFile);
    const expired = "e".repeat(32), fresh = "f".repeat(32);
    const old = new Date(Date.now() - DEFAULT_FABRIC_CONFIG.retention.actorRunArchiveMs - 1000);
    fs.mkdirSync(store.directory, { recursive: true });
    for (const id of [expired, fresh]) {
      fs.writeFileSync(store.resultFile(id), JSON.stringify({ id, text: id }));
      store.consume(id, { handoff: true, mailbox: true });
      if (id === expired && phase === "restore") for (const suffix of [".result.json", ".receipt"]) fs.utimesSync(path.join(store.directory, id + suffix), old, old);
    }
    const key = createHash("sha256").update(["session:startup", "session"].join("\0")).digest("hex").slice(0, 16);
    fs.writeFileSync(path.join(f.actorRoot, actor.id, `queue-${key}.json`), JSON.stringify({ format: 1, cleanHandover: true,
      items: [expired, fresh].map((id) => ({ id, source: "child-completion", deferredHandoff: true, createdAt: Date.now(),
        activation: { kind: "direct", source: "child-completion", sequence: 1 }, payload: { resultFile: store.resultFile(id) } })) }));
    let release!: () => void, started!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const running = new Promise<void>((resolve) => { started = resolve; });
    const tasks: string[] = [];
    vi.spyOn(f.agents, "run").mockImplementation(async (request) => {
      tasks.push(request.task); started(); await gate; throw new Error("fixture ended");
    });
    // Hold only initial maintenance turns, not promise/lock I/O. Then release
    // actual native IO: fake-timer advancement cannot drain an async NTFS queue.
    const held: Array<() => void> = [];
    const immediate = vi.spyOn(globalThis, "setImmediate").mockImplementation(((callback: () => void) => {
      held.push(callback); return {} as NodeJS.Immediate;
    }) as typeof setImmediate);
    const resumeMaintenance = () => { immediate.mockRestore(); for (const callback of held.splice(0)) callback(); };
    try {
      const manager = f.make();
      // Also cover a restored handoff that expires after loading but before drain.
      if (phase === "admission") for (const suffix of [".result.json", ".receipt"]) fs.utimesSync(path.join(store.directory, expired + suffix), old, old);
      manager.tell(actor.id, "immediate post-restart activation");
      await running;
      expect(tasks).toHaveLength(1);
      expect(tasks[0]).not.toContain(JSON.stringify(store.resultFile(expired)).slice(1, -1));
      expect(tasks[0]).toContain(JSON.stringify(store.resultFile(fresh)).slice(1, -1));
      expect(fs.existsSync(store.resultFile(expired))).toBe(true); // No sweep yet.
      for (const suffix of [".result.json", ".receipt"]) fs.utimesSync(path.join(store.directory, fresh + suffix), old, old);
      resumeMaintenance();
      await eventually(() => !fs.existsSync(store.resultFile(expired)));
      expect(fs.existsSync(store.resultFile(expired))).toBe(false);
      expect(fs.existsSync(store.resultFile(fresh))).toBe(true); // Already-active context is fenced.
    } finally { release(); resumeMaintenance(); }
  });

  it.each(["resumeQueued", "resumeAfterRelease", "poll"] as const)("retries deferred startup retention after publication through %s", async boundary => {
    const f = fixture(1); let published = false;
    const manager = f.make({ canConsumeMesh: () => published });
    await turn(); await turn();
    expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
    published = true;
    if (boundary !== "poll") manager[boundary]();
    // The publication boundary itself must remain archive-I/O free.
    expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
    await eventually(() => !fs.existsSync(f.runDir(0, 8)));
  });

  it("rechecks publication between maintenance slices and retries an interrupted startup sweep", async () => {
    const f = fixture(17); let published = true;
    const manager = f.make({ canConsumeMesh: () => published });
    // A callback queued behind the first maintenance slice withdraws publication
    // before the second slice, like quiesce/reload while slow filesystem work yields.
    await turn(); published = false;
    await turn(); await turn();
    // Windows can stop before inspecting even the first run; POSIX completes
    // its original eight-actor slice before publication is withdrawn.
    expect(fs.existsSync(f.runDir(0, 8))).toBe(process.platform === "win32");
    expect(fs.existsSync(f.runDir(16, 8))).toBe(true);
    published = true; manager.resumeQueued();
    await eventually(() => !fs.existsSync(f.runDir(16, 8)));
  });

  it("does not consume archives when the resident lease is lost before the deferred sweep", async () => {
    const f = fixture(1); let lease = true;
    f.make({ canConsumeMesh: () => lease });
    lease = false;
    await turn(); await turn();
    expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
  });
});
