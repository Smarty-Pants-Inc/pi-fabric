import fs from "node:fs";
import { createHash } from "node:crypto";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ACTOR_RETENTION_BATCH_SIZE, ActorManager } from "../src/actors/manager.js";
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
  it("loads 200 real-shaped actors in one linear read and eventually sweeps 2,000 runs in bounded event-loop slices", async () => {
    const f = fixture(200);
    const rawOwnership = vi.fn(() => true);
    const snapshot = vi.fn(() => new Map(f.records.map((record) => [record.id, true])));
    const read = vi.spyOn(ActorRegistryStore.prototype, "read");
    const write = vi.spyOn(ActorRegistryStore.prototype, "write");
    const runsPerActor = 10; // Includes the lastRunId fence in fixture().
    const batchSize = ACTOR_RETENTION_BATCH_SIZE[process.platform === "win32" ? "win32" : "other"];
    const runDirectories = new Set(f.records.map((_, actor) => path.dirname(f.runDir(actor, 0))));
    const readdir = fs.readdirSync;
    let runsThisTurn = 0, totalRuns = 0;
    const runsPerTurn: number[] = [];
    // Observe real archive enumeration, not elapsed time or a mocked pruning result.
    vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      const entries = Reflect.apply(readdir, fs, [file, ...args]);
      if (runDirectories.has(String(file))) {
        runsThisTurn += entries.length;
        totalRuns += entries.length;
      }
      return entries;
    }) as never);
    await turn();
    let previous = performance.now(), longest = 0, active = true;
    let heartbeat: NodeJS.Immediate;
    const beat = () => {
      const now = performance.now(); longest = Math.max(longest, now - previous); previous = now;
      if (runsThisTurn) runsPerTurn.push(runsThisTurn);
      runsThisTurn = 0;
      if (active) heartbeat = setImmediate(beat);
    };
    heartbeat = setImmediate(beat);
    try {
      const started = performance.now();
      f.make({ canManageActor: rawOwnership, snapshotActorOwnership: snapshot });
      const construction = performance.now() - started;
      expect(construction).toBeLessThan(2_000); // Gross-regression sanity, not a shared-runner performance SLA.
      expect(read).toHaveBeenCalledTimes(1);
      expect(write).not.toHaveBeenCalled();
      expect(snapshot).toHaveBeenCalledTimes(1);
      expect(rawOwnership).not.toHaveBeenCalled();
      // No archive traversal or deletion on the constructor stack.
      expect(totalRuns).toBe(0);
      expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
      await eventually(() => !fs.existsSync(f.runDir(199, 8)));
      await turn();
      // A recurring setImmediate observes every maintenance turn. A microtask
      // yield (or no yield) cannot reset the counter, even on a fast filesystem.
      expect(totalRuns).toBe(f.records.length * runsPerActor);
      expect(Math.max(...runsPerTurn)).toBeGreaterThan(0);
      expect(Math.max(...runsPerTurn)).toBeLessThanOrEqual(batchSize * runsPerActor);
      expect(runsPerTurn.length).toBeGreaterThanOrEqual(Math.ceil(f.records.length / batchSize));
      expect(longest).toBeLessThan(2_000); // Allow CI descheduling/GC; work per turn is the regression gate.
      for (let actor = 0; actor < 200; actor++) {
        for (let run = 0; run < 9; run++) expect(fs.existsSync(f.runDir(actor, run))).toBe(false);
        expect(fs.existsSync(f.runDir(actor, 9))).toBe(true); // lastRunId fence survives
      }
      process.stdout.write(JSON.stringify({ probe: "ActorManager 200/2000", constructorMs: construction,
        longestSliceMs: longest, totalRuns, maxRunsPerTurn: Math.max(...runsPerTurn),
        runsPerTurnLimit: batchSize * runsPerActor, workTurns: runsPerTurn.length }) + "\n");
    } finally { active = false; clearImmediate(heartbeat!); }
  });

  it("keeps Windows startup on main's synchronous per-actor sweep, without an added custody queue", async () => {
    const f = fixture(9);
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const asyncQueue = vi.spyOn(ActorLogStore.prototype, "pruneRunsAsync");
    const sync = vi.spyOn(ActorLogStore.prototype, "pruneRuns");
    try {
      f.make({ canManageActor: () => true,
        snapshotActorOwnership: () => new Map(f.records.map(record => [record.id, true])) });
      await eventually(() => !fs.existsSync(f.runDir(8, 8)));
      expect(sync).toHaveBeenCalledTimes(9);
      expect(asyncQueue).not.toHaveBeenCalled();
      for (let actor = 0; actor < 9; actor++) expect(fs.existsSync(f.runDir(actor, 9))).toBe(true);
    } finally { platform.mockRestore(); }
  });

  it.each(["ownership", "publication"] as const)("keeps main's fresh Windows %s veto between actor batches", async fence => {
    const f = fixture(9);
    const boundary = ACTOR_RETENTION_BATCH_SIZE.win32 - 1;
    let owned = true, published = true;
    const prune = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(function(this: ActorLogStore, actor, now) {
      prune.call(this, actor, now);
      if (path.dirname(actor.sessionFile) === path.join(f.actorRoot, f.records[boundary]!.id)) setImmediate(() => {
        if (fence === "ownership") owned = false; else published = false;
      });
    });
    const platform = vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    try {
      f.make({ snapshotActorOwnership: () => new Map(f.records.map(record => [record.id, owned])),
        canConsumeMesh: () => published });
      await eventually(() => !fs.existsSync(f.runDir(boundary, 8)));
      await turn(); await turn();
      for (let run = 0; run < 10; run++) expect(fs.existsSync(f.runDir(8, run))).toBe(true);
    } finally { platform.mockRestore(); }
  });

  it("yields Windows startup maintenance between actors without adding per-run filesystem work", async () => {
    const f = fixture(17);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const lstat = fs.lstatSync;
    let statusProbes = 0;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (path.basename(String(file)) === "status.json" && String(file).includes(`${path.sep}runs${path.sep}run-`)) {
        statusProbes++;
        // Controlled metadata latency, independent of whether this host has NTFS.
        // Eight actors share 72 candidates: four status probes/run at 2 ms each
        // exceed the existing heartbeat bound. One actor still does identical work.
        const until = performance.now() + 2;
        while (performance.now() < until) { /* slow filesystem metadata */ }
      }
      return Reflect.apply(lstat, fs, [file, ...args]);
    }) as never);
    await turn();
    let previous = performance.now(), longest = 0, active = true;
    let heartbeat: NodeJS.Immediate;
    const beat = () => {
      const now = performance.now(); longest = Math.max(longest, now - previous); previous = now;
      if (active) heartbeat = setImmediate(beat);
    };
    heartbeat = setImmediate(beat);
    try {
      f.make();
      await eventually(() => !fs.existsSync(f.runDir(16, 8)));
      await turn();
      process.stdout.write(JSON.stringify({ probe: "Windows startup metadata latency", statusProbes, longestSliceMs: longest }) + "\n");
      expect(statusProbes).toBe(17 * 9 * 4); // Same four status metadata probes per candidate as main.
      expect(longest).toBeLessThan(250);
      for (let actor = 0; actor < 17; actor++) expect(fs.existsSync(f.runDir(actor, 9))).toBe(true);
    } finally { active = false; clearImmediate(heartbeat!); }
  });

  it.each(["ownership", "publication"] as const)("rechecks Windows %s between actor maintenance turns", async (fence) => {
    const f = fixture(3);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const ownership = new Map(f.records.map(record => [record.id, true]));
    let published = true;
    const manager = f.make({ canManageActor: id => ownership.get(id), snapshotActorOwnership: () => new Map(ownership),
      canConsumeMesh: () => published });
    await turn();
    expect(fs.existsSync(f.runDir(0, 8))).toBe(false);
    expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
    if (fence === "ownership") {
      ownership.set(f.records[1]!.id, false);
      await eventually(() => !fs.existsSync(f.runDir(2, 8)));
      expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
    } else {
      published = false;
      await turn(); await turn();
      expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
      expect(fs.existsSync(f.runDir(2, 8))).toBe(true);
      published = true; manager.resumeAfterRelease();
      await eventually(() => !fs.existsSync(f.runDir(2, 8)));
      expect(fs.existsSync(f.runDir(1, 8))).toBe(false);
    }
    for (let actor = 0; actor < 3; actor++) expect(fs.existsSync(f.runDir(actor, 9))).toBe(true);
  });

  it("does not build a directory snapshot for an empty manager's idle ownership refresh", () => {
    const f = fixture(0);
    const snapshot = vi.fn(() => new Map<string, boolean>());
    const manager = f.make({ canManageActor: () => true, snapshotActorOwnership: snapshot });
    snapshot.mockClear();
    expect(manager.listOwned()).toEqual([]);
    expect(snapshot).not.toHaveBeenCalled();
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
    const batchSize = ACTOR_RETENTION_BATCH_SIZE[process.platform === "win32" ? "win32" : "other"];
    // One initial yield, then one turn per batch, including the final completion yield.
    const maintenanceTurns = Math.ceil(f.records.length / batchSize) + 1;
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
    // Freeze only maintenance turns, not promise/lock I/O. This proves admission
    // happens before the last actor's retention slice, rather than relying on timing.
    vi.useFakeTimers({ toFake: ["setImmediate", "clearImmediate"] });
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
      await vi.advanceTimersByTimeAsync(maintenanceTurns);
      expect(fs.existsSync(store.resultFile(expired))).toBe(false);
      expect(fs.existsSync(store.resultFile(fresh))).toBe(true); // Already-active context is fenced.
    } finally { release(); await vi.advanceTimersByTimeAsync(maintenanceTurns); vi.useRealTimers(); }
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
    expect(fs.existsSync(f.runDir(0, 8))).toBe(false);
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
