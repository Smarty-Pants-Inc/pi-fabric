import fs from "node:fs";
import { createHash } from "node:crypto";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ACTOR_RETENTION_BATCH_SIZE, ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
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
// A failure deadline, not a performance budget: the asserted bounds are per-turn work.
async function eventually(predicate: () => boolean, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
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
    let actorsThisTurn = 0, totalActors = 0;
    const actorsPerTurn: number[] = [];
    const pruneRuns = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation((actor, now) => {
      actorsThisTurn++;
      totalActors++;
      return pruneRuns.call(ActorLogStore.prototype, actor, now);
    });
    await turn();
    let previous = performance.now(), longest = 0, active = true;
    let heartbeat: NodeJS.Immediate;
    const beat = () => {
      const now = performance.now(); longest = Math.max(longest, now - previous); previous = now;
      if (actorsThisTurn) actorsPerTurn.push(actorsThisTurn);
      actorsThisTurn = 0;
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
      // Construction does not synchronously start maintenance.
      expect(totalActors).toBe(0);
      expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
      // Run-archive pruning is disabled by the keep-all-traces contract. The real
      // bounded actor maintenance still runs in slices; observe its per-actor
      // archive hook rather than expecting 2,000 run directories to disappear.
      await eventually(() => totalActors === f.records.length, 45_000);
      await turn();
      // A recurring setImmediate observes every maintenance turn. A microtask
      // yield (or no yield) cannot reset the counter, even on a fast filesystem.
      expect(totalActors).toBe(f.records.length);
      expect(Math.max(...actorsPerTurn)).toBeGreaterThan(0);
      expect(Math.max(...actorsPerTurn)).toBeLessThanOrEqual(batchSize);
      expect(actorsPerTurn.length).toBeGreaterThanOrEqual(Math.ceil(f.records.length / batchSize));
      expect(longest).toBeLessThan(2_000); // Allow CI descheduling/GC; work per turn is the regression gate.
      for (let actor = 0; actor < 200; actor++) {
        for (let run = 0; run < runsPerActor; run++) {
          const directory = f.runDir(actor, run);
          expect(fs.existsSync(directory), `${actor}/${run} remains retained`).toBe(true);
          expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe('{"type":"complete"}\n');
        }
      }
      expect(fs.readFileSync(path.join(f.runDir(0, 0), "task.txt"), "utf8")).toBe("fixture task");
      process.stdout.write(JSON.stringify({ probe: "ActorManager 200/2000", constructorMs: construction,
        longestSliceMs: longest, totalActors, maxActorsPerTurn: Math.max(...actorsPerTurn),
        actorsPerTurnLimit: batchSize, workTurns: actorsPerTurn.length }) + "\n");
    } finally { active = false; clearImmediate(heartbeat!); }
  }, 60_000);

  it("yields Windows startup maintenance between actors without adding per-run filesystem work", async () => {
    const f = fixture(17);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const lstat = fs.lstatSync;
    let statusProbes = 0, actorsThisTurn = 0, totalActors = 0;
    const actorsPerTurn: number[] = [];
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (path.basename(String(file)) === "status.json" && String(file).includes(`${path.sep}runs${path.sep}run-`)) statusProbes++;
      return Reflect.apply(lstat, fs, [file, ...args]);
    }) as never);
    const pruneRuns = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation((actor, now) => {
      actorsThisTurn++;
      totalActors++;
      // Controlled per-actor maintenance work, independent of host filesystem latency.
      const until = performance.now() + 2;
      while (performance.now() < until) { /* bounded maintenance slice */ }
      return pruneRuns.call(ActorLogStore.prototype, actor, now);
    });
    await turn();
    let previous = performance.now(), longest = 0, active = true;
    let heartbeat: NodeJS.Immediate;
    // A recurring setImmediate observes every maintenance turn. Count the slow
    // metadata work each turn performs instead of timing it: a shared Windows
    // runner can deschedule any turn for seconds, but cannot change this count.
    const beat = () => {
      const now = performance.now(); longest = Math.max(longest, now - previous); previous = now;
      if (actorsThisTurn) actorsPerTurn.push(actorsThisTurn);
      actorsThisTurn = 0;
      if (active) heartbeat = setImmediate(beat);
    };
    heartbeat = setImmediate(beat);
    try {
      f.make();
      await eventually(() => totalActors === 17);
      await turn();
      process.stdout.write(JSON.stringify({ probe: "Windows startup maintenance slices", statusProbes, longestSliceMs: longest,
        maxActorsPerTurn: Math.max(...actorsPerTurn), workTurns: actorsPerTurn.length }) + "\n");
      expect(totalActors).toBe(17);
      // On Windows the maintenance loop yields after each actor. Run retention no
      // longer reads run status files or deletes run trees.
      expect(statusProbes).toBe(0);
      expect(Math.max(...actorsPerTurn)).toBe(1);
      expect(actorsPerTurn.length).toBe(17);
      expect(actorsPerTurn.every((count) => count === 1)).toBe(true);
      for (let actor = 0; actor < 17; actor++) {
        for (let run = 0; run < 10; run++) {
          const directory = f.runDir(actor, run);
          expect(fs.existsSync(directory), `${actor}/${run} remains retained`).toBe(true);
          expect(fs.readFileSync(path.join(directory, "events.jsonl"), "utf8")).toBe('{"type":"complete"}\n');
        }
      }
    } finally { active = false; clearImmediate(heartbeat!); }
  });

  it.each(["ownership", "publication"] as const)("rechecks Windows %s between actor maintenance turns", async (fence) => {
    const f = fixture(3);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const ownership = new Map(f.records.map(record => [record.id, true]));
    let published = true;
    const pruned: string[] = [];
    const pruneRuns = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation((actor, now) => {
      pruned.push(path.basename(path.dirname(actor.sessionFile)));
      return pruneRuns.call(ActorLogStore.prototype, actor, now);
    });
    const manager = f.make({ canManageActor: id => ownership.get(id), snapshotActorOwnership: () => new Map(ownership),
      canConsumeMesh: () => published });
    await turn();
    expect(pruned).toEqual([f.records[0]!.id]);
    expect(fs.existsSync(f.runDir(0, 8))).toBe(true);
    expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
    if (fence === "ownership") {
      ownership.set(f.records[1]!.id, false);
      await manager.cede(f.records[2]!.id);
      await turn(); await turn();
      expect(pruned).toEqual([f.records[0]!.id]);
      expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
      expect(fs.existsSync(f.runDir(2, 8))).toBe(true);
    } else {
      published = false;
      await turn(); await turn();
      expect(pruned).toEqual([f.records[0]!.id]);
      expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
      expect(fs.existsSync(f.runDir(2, 8))).toBe(true);
      published = true; manager.resumeAfterRelease();
      await eventually(() => pruned.includes(f.records[2]!.id));
      expect(new Set(pruned)).toEqual(new Set(f.records.map(record => record.id)));
      expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
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
    // fv2-11107: a poll names its managed ids so the host may reuse a validated poll map.
    expect(snapshot).toHaveBeenCalledExactlyOnceWith(false, [f.records[0]!.id]);
    expect(rawOwnership).not.toHaveBeenCalled();
    snapshot.mockClear();
    expect(() => manager.tell(f.records[0]!.id, "must not be delivered")).toThrow("owned by another host");
    expect(snapshot).toHaveBeenCalledWith(true);
    expect(f.agents.list()).toEqual([]);
  });

  it("rechecks lease/cede/ownership after the initial yield and preserves live/unknown workers", async () => {
    const f = fixture(3);
    const ownership = new Map(f.records.map((record) => [record.id, true]));
    const pruned: string[] = [];
    const pruneRuns = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation((actor, now) => {
      pruned.push(path.basename(path.dirname(actor.sessionFile)));
      return pruneRuns.call(ActorLogStore.prototype, actor, now);
    });
    const manager = f.make({ canManageActor: (id) => ownership.get(id), snapshotActorOwnership: () => new Map(ownership) });
    const status = path.join(f.runDir(0, 0), "status.json");
    fs.writeFileSync(status, JSON.stringify({ status: "completed", transport: "process", sessionId: String(process.pid), finishedAt: 1 }));
    fs.writeFileSync(path.join(f.runDir(0, 1), "status.json"), JSON.stringify({ status: "completed", finishedAt: 1 }));
    const liveStatus = fs.readFileSync(status);
    const unknownStatus = fs.readFileSync(path.join(f.runDir(0, 1), "status.json"));
    ownership.set(f.records[1]!.id, false);
    await manager.cede(f.records[2]!.id);
    await eventually(() => pruned.includes(f.records[0]!.id));
    expect(pruned).toEqual([f.records[0]!.id]); // Lost ownership and cede still fence maintenance.
    expect(fs.readFileSync(status)).toEqual(liveStatus);
    expect(fs.readFileSync(path.join(f.runDir(0, 1), "status.json"))).toEqual(unknownStatus);
    expect(fs.existsSync(f.runDir(1, 8))).toBe(true);
    expect(fs.existsSync(f.runDir(2, 8))).toBe(true);
    expect(fs.readFileSync(path.join(f.runDir(0, 0), "events.jsonl"), "utf8")).toBe('{"type":"complete"}\n');
  });

  it("reads a shared foreign lineage once per synchronous refresh, never once per actor", () => {
    // Three retention batches (8 per batch) tell once-per-refresh from once-per-actor; 200 actors took ~4 s (smarty-dev#7834).
    const f = fixture(17);
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
    const pruned: string[] = [];
    const pruneRuns = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation((actor, now) => {
      pruned.push(path.basename(path.dirname(actor.sessionFile)));
      return pruneRuns.call(ActorLogStore.prototype, actor, now);
    });
    const manager = f.make({ canConsumeMesh: () => published });
    await turn(); await turn();
    expect(pruned).toEqual([]);
    published = true;
    if (boundary !== "poll") manager[boundary]();
    // The publication boundary itself must remain archive-I/O free.
    expect(pruned).toEqual([]);
    await eventually(() => pruned.length === 1);
    expect(pruned).toEqual([f.records[0]!.id]);
    expect(fs.existsSync(f.runDir(0, 8))).toBe(true);
    expect(fs.readFileSync(path.join(f.runDir(0, 8), "events.jsonl"), "utf8")).toBe('{"type":"complete"}\n');
  });

  it("rechecks publication between maintenance slices and retries an interrupted startup sweep", async () => {
    const f = fixture(17); let published = true;
    const batchSize = ACTOR_RETENTION_BATCH_SIZE[process.platform === "win32" ? "win32" : "other"];
    const firstBatch = f.records.slice(0, batchSize).map(record => record.id);
    const pruned: string[] = [];
    const pruneRuns = ActorLogStore.prototype.pruneRuns;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation((actor, now) => {
      pruned.push(path.basename(path.dirname(actor.sessionFile)));
      return pruneRuns.call(ActorLogStore.prototype, actor, now);
    });
    const manager = f.make({ canConsumeMesh: () => published });
    // A callback queued behind the first maintenance slice withdraws publication
    // before the second slice, like quiesce/reload while slow filesystem work yields.
    await turn(); published = false;
    await turn(); await turn();
    expect(pruned).toEqual(firstBatch);
    expect(fs.existsSync(f.runDir(0, 8))).toBe(true);
    expect(fs.existsSync(f.runDir(16, 8))).toBe(true);
    published = true; manager.resumeQueued();
    await eventually(() => pruned.includes(f.records[16]!.id));
    expect(new Set(pruned)).toEqual(new Set(f.records.map(record => record.id)));
    expect(fs.readFileSync(path.join(f.runDir(16, 8), "events.jsonl"), "utf8")).toBe('{"type":"complete"}\n');
  });

  it("does not consume archives when the resident lease is lost before the deferred sweep", async () => {
    const f = fixture(1); let lease = true;
    const pruneRuns = vi.spyOn(ActorLogStore.prototype, "pruneRuns");
    f.make({ canConsumeMesh: () => lease });
    lease = false;
    await turn(); await turn();
    expect(pruneRuns).not.toHaveBeenCalled();
    expect(fs.existsSync(f.runDir(0, 0))).toBe(true);
    expect(fs.readFileSync(path.join(f.runDir(0, 0), "events.jsonl"), "utf8")).toBe('{"type":"complete"}\n');
  });
});
