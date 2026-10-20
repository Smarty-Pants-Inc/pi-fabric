import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { persistRunStopIntent } from "../src/agents/archive-custody.js";
import { ENODEV_UNRESOLVED_REASON, isRecoveredDurableStop } from "../src/agents/enodev-recovery.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { registerAgentRunner, type FabricHostedRunner } from "../src/runners.js";

// Keep reasons: #4796's startup regression, durable stop admission and recovered-answer data safety.
// All process identities are synthetic known-dead PIDs; these tests never launch or signal a worker.
const roots: string[] = [];
const agents: AgentManager[] = [];
const actors: ActorManager[] = [];
const registrations: Array<() => void> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const manager of actors.splice(0).reverse()) await manager.close();
  for (const manager of agents.splice(0).reverse()) await manager.close();
  for (const unregister of registrations.splice(0)) unregister();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const temp = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-runsboot-enodev-"));
  roots.push(root);
  return root;
};
const managerFor = (root: string, runRoot = root) => {
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true }, {
    runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  agents.push(manager);
  return manager;
};
const immediate = () => new Promise<void>(resolve => setImmediate(resolve));
const idOf = (index: number) => index.toString(16).padStart(32, "0");
const ACTOR = "actor-runsboot";
const history = (root: string, count: number) => {
  for (let index = 1; index <= count; index++) {
    const id = idOf(index);
    const directory = path.join(root, id);
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, status: "completed", transport: "process" }));
  }
};
const run = (root: string, id: string, activation: string, actorId = ACTOR, marker = true, answer = true) => {
  const directory = path.join(root, id);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, actorId, name: "recovered", runner: "pi",
    status: "running", transport: "process", sessionId: "2147483647", startedAt: 1 }));
  fs.writeFileSync(path.join(directory, "task.txt"), JSON.stringify({ actorId, id: activation }, null, 2));
  if (marker) fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({ runId: id, reason: ENODEV_UNRESOLVED_REASON }));
  if (answer) fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: id,
    outcome: "completed", text: `saved:${activation}` }));
  return directory;
};
const markerAt = (directory: string) => JSON.parse(fs.readFileSync(path.join(directory, "unresolved-worker.json"), "utf8"));
const statusAt = (directory: string) => JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));

/** Native event-loop input surrogate; no elapsed-time performance threshold or same-process import measurement. */
const heartbeats = async <T>(operation: () => Promise<T>, sample: () => void) => {
  let active = true;
  let ticks = 0;
  const beat = () => {
    if (!active) return;
    ticks++;
    sample();
    setImmediate(beat);
  };
  setImmediate(beat);
  try { return { value: await operation(), ticks }; }
  finally { active = false; }
};

/** Real actor create/checkpoint/restore, with no activation required for startup loss judgment. */
const ownedFixture = async () => {
  const root = temp();
  const runRoot = path.join(root, "runs");
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const rootId = "session:runsboot-owned";
  const makeActors = (manager: AgentManager, ready: () => boolean, paused = false) => {
    const directory = new ActorManager("runsboot-owned", { id: rootId, name: "main", kind: "main", sessionId: "runsboot-owned" },
      mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager, () => {}, {
        actorRoot, persistent: true, rootId, claimResidency: "durable", releasePaused: paused, canConsumeMesh: ready,
        meshCursorPath: path.join(actorRoot, "mesh-cursor.json"),
      });
    actors.push(directory);
    return directory;
  };
  const previous = managerFor(root, runRoot);
  const before = makeActors(previous, () => true);
  const actor = await before.create({ name: "owned-idle", instructions: "Wait", residency: "durable", responseMode: "text" });
  await before.close();
  await previous.close();
  return { root, runRoot, actorRoot, rootId, actor, makeActors };
};

describe.runIf(process.platform === "linux")("bounded native recovery (smarty-dev#4796)", () => {
  it("finds saved answers at the real cursor's end, shares discovery, and never spawns a duplicate", async () => {
    const root = temp();
    history(root, 384);
    // Inspect the actual filesystem order before changing only directory contents, not root membership.
    const cursor = await fs.promises.opendir(root);
    const names: string[] = [];
    for await (const entry of cursor) names.push(entry.name);
    const last = names.slice(-2);
    const first = run(root, last[0]!, "first");
    const second = run(root, last[1]!, "second", "actor-second");
    const manager = managerFor(root);
    const spawn = vi.spyOn(manager, "spawn").mockRejectedValue(new Error("duplicate spawn"));
    const open = fs.promises.opendir;
    let opens = 0;
    let visited = 0;
    let yieldedBeforeEnd = false;
    vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
      const directory = await open(...args);
      if (String(args[0]) === root) {
        opens++;
        const read = directory.read.bind(directory);
        vi.spyOn(directory, "read").mockImplementation((async () => {
          const entry = await read();
          if (entry) visited++;
          return entry;
        }) as typeof directory.read);
      }
      return directory;
    });
    const result = await heartbeats(() => Promise.all([
      manager.run({ task: "must recover first", actorId: ACTOR, activationId: "first" }),
      manager.run({ task: "must recover second", actorId: "actor-second", activationId: "second" }),
    ]), () => { if (visited > 0 && visited < 384) yieldedBeforeEnd = true; });
    expect(result.value).toMatchObject([
      { id: last[0], text: "saved:first", enodevRecovered: true },
      { id: last[1], text: "saved:second", enodevRecovered: true },
    ]);
    expect(result.ticks).toBeGreaterThan(1);
    expect(yieldedBeforeEnd).toBe(true);
    expect(opens).toBe(3); // two FRESH durable-stop searches, one shared ENODEV discovery
    expect(spawn).not.toHaveBeenCalled();
    for (const [directory, id] of [[first, last[0]!], [second, last[1]!]]) {
      expect(fs.existsSync(path.join(directory!, "unresolved-worker.json"))).toBe(true);
      expect(manager.noteEnodevDelivered(id!)).toBe(true);
      expect(manager.commitEnodevRecovery(id!)).toBe(true);
      expect(statusAt(directory!)).toMatchObject({ status: "completed", enodevDelivered: true });
      expect(fs.existsSync(path.join(directory!, "unresolved-worker.json"))).toBe(false);
    }
  });

  it("also slices count, cached candidate peek and lost loops, not only enumeration", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const root = temp();
    for (let index = 1; index <= 256; index++) run(root, idOf(index), `old-${index}`);
    const manager = managerFor(root);
    await manager.prepareEnodevRecovery();
    const read = fs.readFileSync;
    let statuses = 0;
    vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      if (String(args[0]).startsWith(root + path.sep) && path.basename(String(args[0])) === "status.json") statuses++;
      return read(...args);
    }) as typeof fs.readFileSync);
    const sampleBounds: number[] = [];
    const measured = async <T>(operation: () => Promise<T>) => {
      let previous = statuses;
      const result = await heartbeats(operation, () => { sampleBounds.push(statuses - previous); previous = statuses; });
      sampleBounds.push(statuses - previous);
      expect(result.ticks).toBeGreaterThan(1);
      return result.value;
    };
    expect(await measured(() => manager.countUnknownEnodevStarts(ACTOR, true))).toEqual({ starts: 1, alarm: false });
    // A maintenance/resume retry in this host cannot count another unknown start.
    expect(await manager.countUnknownEnodevStarts(ACTOR, true)).toEqual({ starts: 1, alarm: false });
    expect(markerAt(path.join(root, idOf(1))).unknownStarts).toBe(1);
    vi.spyOn(manager, "spawn").mockRejectedValue(new Error("no matching activation"));
    await measured(async () => {
      await expect(manager.run({ task: "unmatched", actorId: ACTOR, activationId: "unmatched" })).rejects.toThrow("no matching activation");
    });
    await measured(() => manager.settleLostEnodevRuns(ACTOR, new Set()));
    expect(sampleBounds.every(count => count <= 64)).toBe(true);
    expect(statusAt(path.join(root, idOf(256)))).toMatchObject({ status: "completed", settledLost: true });
  });

  it("resumes an interrupted unknown count without incrementing already visited markers again", async () => {
    const root = temp();
    for (let index = 1; index <= 192; index++) run(root, idOf(index), `unknown-${index}`);
    const manager = managerFor(root);
    await manager.prepareEnodevRecovery();
    const read = fs.readFileSync;
    let statuses = 0;
    let allowed = true;
    vi.spyOn(fs, "readFileSync").mockImplementation(((...args: Parameters<typeof fs.readFileSync>) => {
      if (String(args[0]).startsWith(root + path.sep) && path.basename(String(args[0])) === "status.json" && ++statuses === 5) {
        setImmediate(() => { allowed = false; });
      }
      return read(...args);
    }) as typeof fs.readFileSync);
    await expect(manager.countUnknownEnodevStarts(ACTOR, true, () => allowed)).rejects.toThrow("recovery interrupted");
    expect(statuses).toBeGreaterThanOrEqual(5);
    expect(statuses).toBeLessThan(192);
    allowed = true;
    expect(await manager.countUnknownEnodevStarts(ACTOR, true, () => allowed)).toEqual({ starts: 1, alarm: false });
    for (let index = 1; index <= 192; index++) expect(markerAt(path.join(root, idOf(index))).unknownStarts).toBe(1);
  });

  it("refreshes current pending protection between lost slices while keeping the original restored IDs", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const root = temp();
    for (let index = 1; index <= 192; index++) run(root, idOf(index), `pending-${index}`);
    const cursor = await fs.promises.opendir(root);
    const names: string[] = [];
    for await (const entry of cursor) names.push(entry.name);
    const original = names[0]!;
    const current = names.at(-1)!;
    const activation = (id: string) => `pending-${Number.parseInt(id, 16)}`;
    const manager = managerFor(root);
    await manager.prepareEnodevRecovery();
    let protectCurrent = false;
    const pending = manager.settleLostEnodevRuns(ACTOR, () => new Set([
      activation(original), ...(protectCurrent ? [activation(current)] : []),
    ]));
    await immediate();
    protectCurrent = true;
    await pending;
    expect(fs.existsSync(path.join(root, original, "unresolved-worker.json"))).toBe(true);
    expect(fs.existsSync(path.join(root, current, "unresolved-worker.json"))).toBe(true);
    const dropped = names.find(id => id !== original && id !== current)!;
    expect(statusAt(path.join(root, dropped))).toMatchObject({ settledLost: true });
  });

  it("does no constructor discovery for an owned idle actor, then settles dropped work through ready maintenance", async () => {
    const fixture = await ownedFixture();
    history(fixture.runRoot, 512);
    const directory = run(fixture.runRoot, idOf(512), "dropped", fixture.actor.id);
    const manager = managerFor(fixture.root, fixture.runRoot);
    const spawn = vi.spyOn(manager, "spawn").mockRejectedValue(new Error("idle actor must not spawn"));
    const preparation = vi.spyOn(manager, "prepareEnodevRecovery");
    let ready = false;
    const restored = fixture.makeActors(manager, () => ready, true);
    expect(preparation).not.toHaveBeenCalled();
    await immediate();
    expect(preparation).not.toHaveBeenCalled();
    ready = true;
    restored.resumeAfterRelease();
    await immediate(); // first idle turn must not perform full discovery (or any run-file reads)
    expect(preparation).not.toHaveBeenCalled();
    await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(false), { timeout: 5_000 });
    expect(statusAt(directory)).toMatchObject({ status: "completed", settledLost: true });
    expect(spawn).not.toHaveBeenCalled();
  });

  it("preserves the original unknown predecessor observation and counts it once across idle reload/resume", async () => {
    const fixture = await ownedFixture();
    const store = new ActorRegistryStore(fixture.actorRoot);
    store.write(store.snapshot().actors.map(row => ({ ...row, adoptedFrom: ["session:old-owner"] })) as unknown as Record<string, unknown>[]);
    const key = createHash("sha256").update(["session:old-owner", "durable"].join("\0")).digest("hex").slice(0, 16);
    const predecessor = path.join(fixture.actorRoot, fixture.actor.id, `queue-${key}.json`);
    fs.mkdirSync(path.dirname(predecessor), { recursive: true });
    fs.writeFileSync(predecessor, "{ torn");
    const directory = run(fixture.runRoot, idOf(1), "unknown-activation", fixture.actor.id);
    const manager = managerFor(fixture.root, fixture.runRoot);
    const count = vi.spyOn(manager, "countUnknownEnodevStarts");
    let ready = false;
    const restored = fixture.makeActors(manager, () => ready, true);
    // Repairing the input after first-load observation is not permission to change this host's unknown start.
    fs.writeFileSync(predecessor, JSON.stringify({ format: 1, items: [] }));
    ready = true;
    restored.resumeAfterRelease();
    await vi.waitFor(() => expect(markerAt(directory).unknownStarts).toBe(1));
    await count.mock.results[0]!.value;
    for (let pass = 0; pass < 3; pass++) {
      store.write(store.snapshot().actors.map(row => ({ ...row, updatedAt: Date.now() + pass })) as unknown as Record<string, unknown>[]);
      restored.pauseForRelease();
      restored.resumeAfterRelease();
      await immediate();
      await immediate();
    }
    expect(markerAt(directory).unknownStarts).toBe(1);
    expect(statusAt(directory).status).toBe("running");
    expect(count).toHaveBeenCalledOnce();
  });

  it("freshly fences stop-only directories and stop added inside an existing directory after a negative scan", async () => {
    const root = temp();
    const existing = run(root, idOf(1), "existing", ACTOR, false, false);
    const manager = managerFor(root);
    await manager.prepareEnodevRecovery(); // intentionally negative, including the existing directory
    const spawn = vi.spyOn(manager, "spawn").mockRejectedValue(new Error("must remain fenced"));
    persistRunStopIntent(existing, "explicit");
    const stopped = await manager.run({ task: "never launch", actorId: ACTOR, activationId: "existing" });
    expect(stopped).toMatchObject({ id: idOf(1), status: "stopped" });
    expect(isRecoveredDurableStop(stopped)).toBe(true);
    const added = run(root, idOf(2), "new-stop", ACTOR, false, false);
    persistRunStopIntent(added, "deadline");
    expect(await manager.run({ task: "never launch", actorId: ACTOR, activationId: "new-stop" }))
      .toMatchObject({ id: idOf(2), status: "timed_out" });
    // Completed answers still win over a fresh explicit stop, including after the negative candidate scan.
    const answered = run(root, idOf(3), "answer-wins", ACTOR, false, true);
    persistRunStopIntent(answered, "explicit");
    expect(await manager.run({ task: "never launch", actorId: ACTOR, activationId: "answer-wins" }))
      .toMatchObject({ id: idOf(3), status: "completed", text: "saved:answer-wins", enodevRecovered: true });
    expect(manager.noteEnodevDelivered(idOf(3))).toBe(true);
    expect(manager.commitEnodevRecovery(idOf(3))).toBe(true);
    expect(await manager.run({ task: "still stopped", actorId: ACTOR, activationId: "existing" }))
      .toMatchObject({ id: idOf(1), status: "stopped" });
    expect(spawn).not.toHaveBeenCalled();
  });

  it("closes the root cursor before an unknown-identity stop join that host close leaves in custody", async () => {
    const root = temp();
    const directory = run(root, idOf(1), "identity-unknown", ACTOR, false, false);
    const snapshot = { id: idOf(1), actorId: ACTOR, transport: "process", status: "running" };
    persistRunStopIntent(directory, "explicit", snapshot);
    const manager = managerFor(root);
    const open = fs.promises.opendir;
    let closes = 0;
    vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
      const cursor = await open(...args);
      if (String(args[0]) === root) {
        const close = cursor.close.bind(cursor);
        vi.spyOn(cursor, "close").mockImplementation((async () => { closes++; await close(); }) as typeof cursor.close);
      }
      return cursor;
    });
    const watch = fs.watch;
    let observing!: () => void;
    const observed = new Promise<void>(resolve => { observing = resolve; });
    vi.spyOn(fs, "watch").mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      const watcher = watch(...args);
      if (String(args[0]) === directory) observing();
      return watcher;
    }) as typeof fs.watch);
    let settled = false;
    void manager.run({ task: "unknown launch identity", actorId: ACTOR, activationId: "identity-unknown" })
      .then(() => { settled = true; }, () => { settled = true; });
    await observed;
    expect(closes).toBe(1);
    expect(settled).toBe(false);
    await manager.close();
    await immediate();
    expect(settled).toBe(false); // abandon observation, not durable stop custody
    expect(statusAt(directory).status).toBe("running");
  });

  it("revokes asynchronous candidate recovery before delivery or settlement, without cancelling shared discovery", async () => {
    const root = temp();
    for (let index = 1; index <= 192; index++) run(root, idOf(index), `activation-${index}`);
    const manager = managerFor(root);
    await manager.prepareEnodevRecovery();
    let owned = true;
    const release = vi.spyOn(manager, "spawn").mockRejectedValue(new Error("must not spawn"));
    const pending = manager.run({ task: "revoke", actorId: ACTOR, activationId: "activation-192" }, undefined, undefined, () => owned);
    await immediate();
    owned = false;
    await expect(pending).rejects.toThrow("recovery interrupted");
    expect(release).not.toHaveBeenCalled();
    expect(markerAt(path.join(root, idOf(192)))).toBeDefined();
    expect(manager.commitEnodevRecovery(idOf(192))).toBe(false);
  });
});

const hostedRunner = () => {
  const calls = { starts: [] as string[], attaches: [] as string[] };
  const adapter: FabricHostedRunner = {
    id: "runsboot-native-hosted", label: "Native recovery fixture", kind: "hosted",
    capabilities: { recursiveFabric: false, steer: false, followUp: false, persistentSessions: false, kernels: false,
      handoff: false, modelDiscovery: false, imageInput: false, compaction: false, questions: false, sleep: false, writePolicy: false },
    prepare: context => ({ id: context.idempotencyKey }),
    start: (_locator, context) => { calls.starts.push(context.id); },
    attach: (_locator, context) => { calls.attaches.push(context.id); },
    liveness: () => "running",
    stop: () => ({ confirmed: true }),
  };
  registrations.push(registerAgentRunner(adapter));
  return { adapter, calls };
};

describe("standalone hosted recovery lifecycle contract", () => {
  it.each(["signal", "manager close"])("interrupts paused recovery on %s rather than resolving a successful partial sweep", async cancellation => {
    const root = temp();
    history(root, 192);
    const manager = managerFor(root);
    manager.pauseForRelease();
    const abort = new AbortController();
    let opened!: () => void;
    const opening = new Promise<void>(resolve => { opened = resolve; });
    const open = fs.promises.opendir;
    vi.spyOn(fs.promises, "opendir").mockImplementation(async (...args: Parameters<typeof fs.promises.opendir>) => {
      const cursor = await open(...args);
      if (String(args[0]) === root) opened();
      return cursor;
    });
    const pending = manager.recoverHostedRuns(undefined, { signal: abort.signal });
    const rejected = expect(pending).rejects.toThrow();
    await opening;
    await immediate();
    if (cancellation === "signal") abort.abort();
    else await manager.close();
    await rejected;
  });

  it("streams a fresh no-arg sweep, obeys shouldRecover, and never adopts the same hosted ID twice", async () => {
    const root = temp();
    const runner = hostedRunner();
    const first = managerFor(root);
    const kept = await first.spawn({ task: "recover", runner: runner.adapter.id, residency: "durable" });
    const foreign = await first.spawn({ task: "skip", runner: runner.adapter.id, residency: "durable" });
    await first.close();
    const second = managerFor(root);
    const readdir = fs.readdirSync;
    vi.spyOn(fs, "readdirSync").mockImplementation(((...args: Parameters<typeof fs.readdirSync>) => {
      if (String(args[0]) === root) throw new Error("synchronous root enumeration");
      return readdir(...args);
    }) as typeof fs.readdirSync);
    const options = { shouldRecover: (id: string) => id !== foreign.id, maxEntries: 1, budgetMs: 2 };
    const recovered = await Promise.all([second.recoverHostedRuns(undefined, options), second.recoverHostedRuns(undefined, options)]);
    expect(recovered.flat()).toEqual([kept.id]);
    expect(runner.calls.attaches).toEqual([kept.id]);
    expect(runner.calls.starts).toEqual([kept.id, foreign.id]);
    expect(await second.recoverHostedRuns(undefined, options)).toEqual([]);
    await expect(second.recoverHostedRuns(undefined, { canRecover: () => false })).rejects.toThrow("recovery interrupted");
  });
});
