import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, ActorPreparationError } from "../src/actors/manager.js";
import { ActorRegistryStore, ActorRegistryUpdateVetoedError } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { configureFabricDiagnostics, fabricDiagnosticsFile, flushFabricDiagnostics } from "../src/core/diagnostics.js";
import { MeshStore } from "../src/mesh/store.js";

// Keep reason: accepted-work data safety / smarty-dev#14378 and #14215. The registry's
// staging, lock, update/expiry, manager admission/catches and disk queue stay real.
// Only prepared-generation validation and its monotonic deadline are controlled.
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  await flushFabricDiagnostics();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

const setup = (options: NonNullable<ConstructorParameters<typeof ActorManager>[6]> = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-veto-retention-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("veto-retention", { id: "session:veto-retention", name: "main", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), meshCursorPath: path.join(root, "cursor.json"), persistent: true,
      meshRetentionSweepPath: false, preparationTimeoutMs: 10_000, preparationRetryMs: 60_000, ...options,
    });
  cleanups.push(async () => {
    // The only recursive deletion names the mkdtemp directory this test created.
    vi.restoreAllMocks();
    await actors.close();
    await agents.close();
    fs.rmSync(root, { recursive: true, force: true });
  });
  return { actors, agents, mesh, root };
};

const queue = (sessionFile: string) => {
  const directory = path.dirname(sessionFile);
  return fs.readdirSync(directory).filter(file => file.startsWith("queue-")).flatMap(file =>
    (JSON.parse(fs.readFileSync(path.join(directory, file), "utf8")) as { items: Array<{
      id: string; payload: { id?: string }; attempts: number; preparationAttempts?: number;
      activationId?: string; batchIds?: string[];
    }> }).items);
};

// Subscribe to the manager's final settlement emission, not a sleep/poll loop.
const settled = (actors: ActorManager, id: string, observed: () => boolean) => new Promise<void>(resolve => {
  const check = () => {
    if (!observed() || actors.inFlightCount() !== 0) return;
    unsubscribe();
    resolve();
  };
  const unsubscribe = actors.subscribe(check);
  check();
});

const registryVeto = (refuse: (rows: readonly Record<string, unknown>[]) => boolean) => {
  const now = performance.now.bind(performance);
  let elapsed = 0;
  vi.spyOn(performance, "now").mockImplementation(() => now() + elapsed);
  const prepare = ActorRegistryStore.prototype.prepare;
  const update = ActorRegistryStore.prototype.update;
  const errors: unknown[] = [];
  vi.spyOn(ActorRegistryStore.prototype, "prepare").mockImplementation(function (this: ActorRegistryStore, rows, options, snapshot) {
    const prepared = prepare.call(this, rows, options, snapshot);
    if (!refuse(rows)) return prepared;
    return { ...prepared, valid: () => { elapsed += 5_001; return false; }, vetoReason: () => "generation" as const };
  });
  vi.spyOn(ActorRegistryStore.prototype, "update").mockImplementation(async function (this: ActorRegistryStore, select, label) {
    try { return await update.call(this, select, label); }
    catch (error) { errors.push(error); throw error; }
  });
  return { errors };
};

const publish = (mesh: MeshStore, data: unknown = { incident: 14378 }) => mesh.publish({ topic: "github.veto-retention",
  from: { id: "subscription-fixture", name: "subscription-fixture", kind: "actor" }, data });
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const outputs = (actors: ActorManager, id: string) => actors.messages(id).filter(message => message.direction === "out" && message.runId && !message.error);
const create = (actors: ActorManager, responseMode: "text" | "directive" = "text") => actors.create({
  name: "veto-retention", instructions: "Reply deterministically.", topics: ["github.veto-retention"],
  residency: "durable", responseMode, coalesce: false, model: "provider/test",
});

describe("real registry preparation and callerless subscription retention", () => {
  it("CONTROL: initial registry veto keeps the accepted subscription event durable (no worker)", async () => {
    const { actors, agents, mesh, root } = setup();
    const actor = await create(actors);
    const run = vi.spyOn(agents, "run");
    const veto = registryVeto(rows => rows.some(row => row.id === actor.id && row.status === "preparing"));
    const done = settled(actors, actor.id, () => veto.errors.length > 0);
    const event = await publish(mesh);
    await done;
    expect(veto.errors.some(error => error instanceof ActorRegistryUpdateVetoedError)).toBe(true);
    expect(actors.messages(actor.id).find(message => (message.data as { phase?: string })?.phase === "registry")?.data)
      .toMatchObject({ errorType: "ActorPreparationError", attempts: 1 });
    expect(run).not.toHaveBeenCalled();
    expect(actors.status(actor.id).queued).toBe(1);
    expect(queue(actor.sessionFile!)).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }),
      attempts: 0, preparationAttempts: 1 })]);
    const cursor = JSON.parse(fs.readFileSync(path.join(root, "cursor.json"), "utf8"));
    expect(cursor.last.sequence).toBeGreaterThanOrEqual(event.sequence);
  });

  it("CONTROL: four real initial registry vetoes classify transient and keep the original event", async () => {
    const { actors, agents, mesh } = setup({ preparationRetryMs: 10 });
    const actor = await create(actors);
    const run = vi.spyOn(agents, "run");
    let refuse = true;
    const veto = registryVeto(rows => refuse && rows.some(row => row.id === actor.id && row.status === "preparing"));
    const done = settled(actors, actor.id, () => (queue(actor.sessionFile!)[0]?.preparationAttempts ?? 0) >= 4);
    const event = await publish(mesh);
    await done;
    expect(veto.errors.length).toBeGreaterThanOrEqual(4);
    expect(veto.errors.every(error => error instanceof ActorRegistryUpdateVetoedError)).toBe(true);
    expect(run).not.toHaveBeenCalled();
    expect(actors.status(actor.id).activationBlocked).toMatchObject({ code: "registry-update-vetoed", class: "transient" });
    expect(queue(actor.sessionFile!)).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }),
      attempts: 0, preparationAttempts: 4 })]);
    const recovered = settled(actors, actor.id, () => outputs(actors, actor.id).length === 1);
    refuse = false;
    await recovered;
    await pause(100);
    expect(run).toHaveBeenCalledTimes(1);
    expect(run.mock.calls[0]![0].task).toContain(event.id);
    expect(queue(actor.sessionFile!)).toEqual([]);
    expect(actors.status(actor.id).activationBlocked).toBeUndefined();
  });

  it("REGRESSION: finalization registry veto preserves typed transient evidence after a completed activation", async () => {
    const { actors, agents, mesh } = setup();
    const actor = await create(actors);
    const run = vi.spyOn(agents, "run");
    // No initial/advisory failure: the veto is armed only after real worker output.
    let completed = false;
    actors.subscribe(() => { completed ||= actors.messages(actor.id).some(message => message.direction === "out" && !message.error); });
    const veto = registryVeto(rows => completed && rows.some(row => row.id === actor.id && row.status === "idle"));
    const done = settled(actors, actor.id, () => veto.errors.length > 0);
    await publish(mesh);
    await done;
    expect(run).toHaveBeenCalledTimes(1);
    expect(actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1);
    expect(veto.errors.some(error => error instanceof ActorRegistryUpdateVetoedError)).toBe(true);
    expect(actors.messages(actor.id).some(message => message.source === "preparation" &&
      (message.data as { phase?: string })?.phase === "registry")).toBe(true);
    expect(queue(actor.sessionFile!)).toEqual([]); // completed, not an event-loss claim
    expect(actors.status(actor.id).activationBlocked).toMatchObject({
      code: "registry-update-vetoed", class: "transient", reason: "actor registry validation veto (generation)",
    });
  });

  it.each(["text", "directive"] as const)("REGRESSION: permanent pre-launch binding failure retains accepted %s event without worker execution", async responseMode => {
    let unavailable = false;
    let sessionFile: string | undefined;
    let acceptedAtBinding: ReturnType<typeof queue> = [];
    const { actors, agents, mesh, root } = setup({ preparationRetryMs: 5, resolvePiModel: async model => {
      if (unavailable) {
        acceptedAtBinding = queue(sessionFile!);
        throw new Error("model no longer available");
      }
      return model;
    } });
    const actor = await create(actors, responseMode);
    sessionFile = actor.sessionFile!;
    const run = vi.spyOn(agents, "run");
    unavailable = true;
    const done = settled(actors, actor.id, () => actors.messages(actor.id).some(message => message.error?.includes("model no longer available")));
    const event = await publish(mesh);
    await done;
    expect(run).not.toHaveBeenCalled();
    expect(actors.messages(actor.id).filter(message => message.direction === "in" && message.source === "mesh:github.veto-retention")).toHaveLength(1);
    expect(acceptedAtBinding).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }), attempts: 0 })]);
    const cursor = JSON.parse(fs.readFileSync(path.join(root, "cursor.json"), "utf8"));
    expect(cursor.last.sequence).toBeGreaterThanOrEqual(event.sequence);
    const retained = queue(actor.sessionFile!);
    expect(retained).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }), attempts: 0, preparationAttempts: 1 })]);
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, activationBlocked: { class: "hard", code: "failing-preparation" } });
    const block = actors.status(actor.id).activationBlocked;
    // Repair alone and unrelated accepted input are not authorization to hot-retry.
    unavailable = false;
    const newer = await publish(mesh, { later: true });
    await pause(150); // multiple preparation-retry and real monitor intervals
    expect(run).not.toHaveBeenCalled();
    expect(queue(actor.sessionFile!)[0]).toEqual(retained[0]);
    expect(queue(actor.sessionFile!).map(item => item.payload.id)).toEqual([event.id, newer.id]);
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    const recovered = settled(actors, actor.id, () => outputs(actors, actor.id).length === 2);
    await actors.resetSession(actor.id);
    await recovered;
    await pause(50);
    expect(run).toHaveBeenCalledTimes(2);
    expect(run.mock.calls.filter(([request]) => request.task.includes(event.id))).toHaveLength(1);
    expect(queue(actor.sessionFile!)).toEqual([]);
    expect(actors.status(actor.id).activationBlocked).toBeUndefined();
  });

  // catch: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/6829#issuecomment-6966581717
  it.each([false, true])("re-apply clears only the preparation block and activates retained work (unchanged instructions=%s)", async unchanged => {
    let rejectBroken = false;
    const { actors, mesh, root } = setup({ preparationRetryMs: 5, resolvePiModel: model => {
      if (rejectBroken && model === "provider/test") throw new Error("configured model no longer available");
      return model;
    } });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "diagnostics"));
    configureFabricDiagnostics("reapply-preparation");
    const actor = await create(actors);
    rejectBroken = true;
    const blocked = settled(actors, actor.id, () => actors.status(actor.id).activationBlocked !== undefined);
    const event = await publish(mesh);
    await blocked;
    const block = actors.status(actor.id).activationBlocked!;
    expect(block).toMatchObject({ code: "failing-preparation", class: "hard", reason: expect.stringContaining("configured model no longer available") });
    const retained = queue(actor.sessionFile!);
    expect(retained).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }), attempts: 0, preparationAttempts: 1 })]);
    expect(outputs(actors, actor.id)).toEqual([]);
    // The re-apply repairs the project model before refreshing its instructions.
    await actors.setModel(actor.id, "provider/fixed", "project");
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    const recovered = settled(actors, actor.id, () => outputs(actors, actor.id).length === 1);
    const applied = await actors.setInstructions(actor.id, unchanged ? "Reply deterministically." : "Reply using the repaired config.");
    expect(applied.activationBlocked).toBeUndefined();
    await recovered;
    expect(actors.status(actor.id)).toMatchObject({ id: actor.id, status: "idle", queued: 0, model: "provider/fixed" });
    expect(actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(outputs(actors, actor.id)).toHaveLength(1);
    expect(queue(actor.sessionFile!)).toEqual([]);
    // Read the real private diagnostic sink, not logger call counts.
    await flushFabricDiagnostics();
    const log = fs.readFileSync(fabricDiagnosticsFile("reapply-preparation"), "utf8");
    expect(log).toContain(`actor ${actor.id} re-apply cleared activation block: code=failing-preparation; reason=${block.reason}`);
    const registry = new ActorRegistryStore(path.join(root, "actors")).snapshot();
    expect(registry.actors.find(row => row.id === actor.id)).not.toHaveProperty("activationBlocked");
  });

  // Setting a block writes no diagnostics file: the activation path stays free of new file I/O
  // (mesh-retry-gates' virtual-fs gate). The reason is on the actor record while blocked.
  it("records a preparation block's reason on the actor when it is set, before any re-apply", async () => {
    let unavailable = false;
    const { actors, mesh, root } = setup({ resolvePiModel: model => {
      if (unavailable) throw new Error("extension preparation rejected configured model");
      return model;
    } });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "diagnostics"));
    configureFabricDiagnostics("preparation-block");
    const actor = await create(actors);
    unavailable = true;
    const done = settled(actors, actor.id, () => actors.status(actor.id).activationBlocked !== undefined);
    await publish(mesh);
    await done;
    const block = actors.status(actor.id).activationBlocked!;
    expect(block).toMatchObject({ code: "failing-preparation", reason: expect.stringContaining("extension preparation rejected configured model") });
  });

  it("re-apply preserves a non-preparation safety block even when its text mentions preparation", async () => {
    const { actors, agents } = setup();
    const actor = await create(actors);
    vi.spyOn(agents, "run").mockRejectedValue(new Error("security policy denied preparation"));
    await expect(actors.ask(actor.id, "safety failure")).rejects.toThrow("security policy denied preparation");
    await settled(actors, actor.id, () => true);
    const block = actors.status(actor.id).activationBlocked!;
    expect(block).toMatchObject({ class: "hard" });
    expect(block.code).toBe("security-policy");
    const applied = await actors.setInstructions(actor.id, "Re-applied configuration.");
    expect(applied.activationBlocked).toEqual(block);
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
  });

  it("a refused re-apply does not clear a preparation block", async () => {
    let unavailable = false;
    const { actors, mesh } = setup({ resolvePiModel: model => {
      if (unavailable) throw new Error("configured model no longer available");
      return model;
    } });
    const actor = await create(actors);
    unavailable = true;
    const done = settled(actors, actor.id, () => actors.status(actor.id).activationBlocked !== undefined);
    await publish(mesh);
    await done;
    const block = actors.status(actor.id).activationBlocked;
    await expect(actors.setInstructions(actor.id, "Repaired.", () => { throw new Error("apply refused"); })).rejects.toThrow("apply refused");
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
  });

  it("a failed re-apply registry commit restores the preparation block and does not log a clear", async () => {
    let unavailable = false;
    const { actors, mesh, root } = setup({ resolvePiModel: model => {
      if (unavailable) throw new Error("configured model no longer available");
      return model;
    } });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "diagnostics"));
    configureFabricDiagnostics("refused-reapply");
    const actor = await create(actors);
    unavailable = true;
    const done = settled(actors, actor.id, () => actors.status(actor.id).activationBlocked !== undefined);
    await publish(mesh);
    await done;
    const block = actors.status(actor.id).activationBlocked;
    const retained = queue(actor.sessionFile!);
    unavailable = false;
    await actors.setModel(actor.id, "provider/fixed", "project");
    vi.spyOn(ActorRegistryStore.prototype, "update").mockRejectedValueOnce(new Error("apply registry commit refused"));
    await expect(actors.setInstructions(actor.id, "Repaired.")).rejects.toThrow("apply registry commit refused");
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    expect(queue(actor.sessionFile!)).toEqual(retained);
    // Join the rollback's save before reading the actual persistent record.
    await actors.setModel(actor.id, "provider/fixed", "project");
    expect(new ActorRegistryStore(path.join(root, "actors")).snapshot().actors.find(row => row.id === actor.id)?.activationBlocked).toEqual(block);
    await flushFabricDiagnostics();
    const file = fabricDiagnosticsFile("refused-reapply");
    expect(fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "").not.toContain("re-apply cleared");
  });

  // catch: https://github.com/Smarty-Pants-Inc/pi-fabric/pull/2017
  it.each([false, true])("a failed re-apply presence commit restores the block, streak and retained work without a clear (unchanged instructions=%s)", async unchanged => {
    let unavailable = false;
    const { actors, agents, mesh, root } = setup({ presenceRetryMs: 20, resolvePiModel: model => {
      if (unavailable) throw new Error("configured model no longer available");
      return model;
    } });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "diagnostics"));
    configureFabricDiagnostics("refused-presence-reapply");
    const actor = await create(actors);
    unavailable = true;
    const done = settled(actors, actor.id, () => actors.status(actor.id).activationBlocked !== undefined);
    await publish(mesh);
    await done;
    const block = actors.status(actor.id).activationBlocked;
    const retained = queue(actor.sessionFile!);
    const registryRow = () => new ActorRegistryStore(path.join(root, "actors")).snapshot().actors.find(row => row.id === actor.id)!;
    const streak = registryRow().failureStreak;
    const run = vi.spyOn(agents, "run");
    const put = mesh.put.bind(mesh);
    const refused = new Error("injected actor presence publication refused");
    let refuse = true;
    let failures = 0;
    vi.spyOn(mesh, "put").mockImplementation(async request => {
      if (refuse && request.key === `actors/veto-retention/${actor.id}`) {
        failures++;
        throw refused;
      }
      return put(request);
    });
    const instructions = unchanged ? "Reply deterministically." : "Repaired despite failed presence publication.";
    // Leave the cause unavailable: even without a worker, another preparation attempt
    // would change the original durable queue and block, as in the independent judge row.
    await expect.soft(actors.setInstructions(actor.id, instructions)).rejects.toThrow(refused.message);
    await pause(180);
    expect(failures).toBeGreaterThan(0);
    expect.soft(actors.status(actor.id).activationBlocked).toEqual(block);
    expect.soft(queue(actor.sessionFile!)).toEqual(retained);
    expect.soft(run).not.toHaveBeenCalled();
    expect.soft(registryRow().activationBlocked).toEqual(block);
    expect.soft(registryRow().failureStreak).toEqual(streak);
    await flushFabricDiagnostics();
    const file = fabricDiagnosticsFile("refused-presence-reapply");
    const diagnostic = fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "";
    expect.soft(diagnostic).not.toContain("re-apply cleared");
    expect.soft(diagnostic).toContain(`actor ${actor.id} re-apply publication refused`);
    // A second explicit attempt during retry backoff is not a successful publication.
    await expect(actors.setInstructions(actor.id, instructions)).rejects.toThrow(refused.message);
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    expect(queue(actor.sessionFile!)).toEqual(retained);
    // Refusing a foreground acknowledgement must not poison later best-effort writes.
    refuse = false;
    unavailable = false;
    await actors.setModel(actor.id, "provider/test", "project");
    await pause(180);
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    expect(queue(actor.sessionFile!)).toEqual(retained);
    expect(mesh.get(`actors/veto-retention/${actor.id}`)?.value).toMatchObject({ activationBlocked: block });
  });

  it.each(["refused", "deferred"] as const)("a failed re-apply shared presence commit retains its block when publication is %s", async failure => {
    let unavailable = false;
    let failing = false;
    let actors!: ActorManager;
    let mesh!: MeshStore;
    const state = setup({ resolvePiModel: model => {
      if (unavailable) throw new Error("configured model no longer available");
      return model;
    }, presencePublisher: { refresh: async () => {
      if (failing) {
        if (failure === "refused") throw new Error("Actor presence publication refused");
        return; // The resident publisher may defer its shared write without rejecting.
      }
      const batch = actors.presenceBatch(false);
      await mesh.writeBatch({ ops: batch.ops, identity: { id: "session:veto-retention", name: "main", kind: "main" } });
      batch.committed();
    }, schedule() {} } });
    ({ actors, mesh } = state);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(state.root, "diagnostics"));
    configureFabricDiagnostics("refused-shared-presence-reapply");
    const actor = await create(actors);
    unavailable = true;
    const done = settled(actors, actor.id, () => actors.status(actor.id).activationBlocked !== undefined);
    await publish(mesh);
    await done;
    const block = actors.status(actor.id).activationBlocked;
    const retained = queue(actor.sessionFile!);
    failing = true;
    await expect.soft(actors.setInstructions(actor.id, "Repaired.")).rejects.toThrow(`Actor presence publication ${failure}`);
    await pause(180);
    expect.soft(actors.status(actor.id).activationBlocked).toEqual(block);
    expect.soft(queue(actor.sessionFile!)).toEqual(retained);
    await flushFabricDiagnostics();
    const file = fabricDiagnosticsFile("refused-shared-presence-reapply");
    const diagnostic = fs.existsSync(file) ? fs.readFileSync(file, "utf8") : "";
    expect.soft(diagnostic).not.toContain("re-apply cleared");
    expect.soft(diagnostic).toContain(`actor ${actor.id} re-apply publication refused`);
    // Ordinary setters remain best-effort even while this shared publisher refuses.
    unavailable = false;
    await expect(actors.setModel(actor.id, "provider/test", "project")).resolves.toMatchObject({ activationBlocked: block });
    failing = false;
    await actors.setModel(actor.id, "provider/test", "project");
    expect(mesh.get(`actors/veto-retention/${actor.id}`)?.value).toMatchObject({ activationBlocked: block });
  });

  // catch: https://github.com/Smarty-Pants-Inc/smarty-dev/issues/21355
  it.each([false, true])("a re-apply refuses a committed shared batch that omits its actor (unchanged instructions=%s)", async unchanged => {
    let available = true, unavailable = false, omit = false;
    let actors!: ActorManager, mesh!: MeshStore;
    let omitted: { ops: unknown[]; actorIds: string[] } | undefined;
    const state = setup({ canManageActor: () => available, resolvePiModel: model => {
      if (unavailable) throw new Error("configured model no longer available");
      return model;
    }, presencePublisher: { refresh: async () => {
      if (omit) available = false;
      try {
        const batch = actors.presenceBatch(false);
        if (omit) omitted = { ops: batch.ops, actorIds: batch.actorIds };
        await mesh.writeBatch({ ops: batch.ops, identity: { id: "session:veto-retention", name: "main", kind: "main" } });
        batch.committed();
      } finally { available = true; }
    }, schedule() {} } });
    ({ actors, mesh } = state);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(state.root, "diagnostics"));
    configureFabricDiagnostics("omitted-presence-reapply");
    const actor = await create(actors);
    unavailable = true;
    const done = settled(actors, actor.id, () => actors.status(actor.id).activationBlocked !== undefined);
    await publish(mesh);
    await done;
    const block = actors.status(actor.id).activationBlocked;
    const retained = queue(actor.sessionFile!);
    const instructions = unchanged ? "Reply deterministically." : "Recovery after omitted presence.";
    omit = true;
    await expect(actors.setInstructions(actor.id, instructions)).rejects.toThrow("Actor presence publication deferred");
    await pause(180);
    expect(omitted).toEqual({ ops: [], actorIds: [] });
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    expect(queue(actor.sessionFile!)).toEqual(retained);
    await flushFabricDiagnostics();
    const diagnostic = fs.readFileSync(fabricDiagnosticsFile("omitted-presence-reapply"), "utf8");
    expect(diagnostic).not.toContain("re-apply cleared");
    expect(diagnostic).toContain(`actor ${actor.id} re-apply publication refused`);
    // A refused acknowledgement leaves ordinary callers best-effort and the
    // serialized chain usable; a later real actor commit can authorize recovery.
    unavailable = false;
    await actors.setModel(actor.id, "provider/test", "project");
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    expect(new ActorRegistryStore(path.join(state.root, "actors")).snapshot().actors
      .find(row => row.id === actor.id)?.activationBlocked).toEqual(block);
    omit = false;
    const recovered = settled(actors, actor.id, () => outputs(actors, actor.id).length === 1);
    await actors.setInstructions(actor.id, instructions);
    await recovered;
    expect(actors.status(actor.id)).toMatchObject({ status: "idle", queued: 0 });
    expect(actors.status(actor.id).activationBlocked).toBeUndefined();
    expect(queue(actor.sessionFile!)).toEqual([]);
  });

  it("retains a hard-preparation batch while failed and successful ask probes bypass it without absorbing members", async () => {
    let unavailable = false;
    const { actors, agents, mesh } = setup({ preparationRetryMs: 5, resolvePiModel: async model => {
      if (unavailable) throw new Error("model no longer available");
      return model;
    } });
    const actor = await actors.create({ name: "batched-veto", instructions: "Reply deterministically.",
      topics: ["github.veto-retention"], residency: "durable", responseMode: "text", coalesce: false,
      model: "provider/test", batchWindowMs: 200, batchMax: 2 });
    await actors.ask(actor.id, "prior conversation must survive probes");
    await settled(actors, actor.id, () => true);
    const outputCountBeforeProbes = outputs(actors, actor.id).length;
    const run = vi.spyOn(agents, "run");
    unavailable = true;
    const done = settled(actors, actor.id, () => queue(actor.sessionFile!).filter(item => item.preparationAttempts === 1).length === 2);
    const first = await publish(mesh, { batch: 1 });
    const second = await publish(mesh, { batch: 2 });
    await done;
    const retained = queue(actor.sessionFile!);
    const ids = retained.map(item => item.id);
    expect(retained.map(item => item.payload.id)).toEqual([first.id, second.id]);
    expect(retained).toEqual(ids.map(id => expect.objectContaining({ id, attempts: 0, preparationAttempts: 1,
      activationId: ids[0], batchIds: ids })));
    expect(run).not.toHaveBeenCalled();
    const sessionHeader = () => fs.readFileSync(actor.sessionFile!, "utf8").split("\n")[0];
    const backups = () => fs.readdirSync(path.dirname(actor.sessionFile!)).filter(file => file.startsWith("session.jsonl.") && file.endsWith(".bak"));
    const headerBeforeProbes = sessionHeader();
    const conversationBeforeProbes = fs.readFileSync(actor.sessionFile!, "utf8");
    expect(conversationBeforeProbes).toContain("prior conversation must survive probes");
    const backupsBeforeProbes = backups();
    await expect(actors.ask(actor.id, "failed explicit probe")).rejects.toThrow("model no longer available");
    await settled(actors, actor.id, () => true);
    expect(queue(actor.sessionFile!)).toEqual(retained);
    expect(actors.status(actor.id).activationBlocked?.class).toBe("hard");
    expect(sessionHeader()).toBe(headerBeforeProbes);
    expect(fs.readFileSync(actor.sessionFile!, "utf8")).toBe(conversationBeforeProbes);
    expect(backups()).toEqual(backupsBeforeProbes);
    await pause(100);
    expect(run).not.toHaveBeenCalled();
    unavailable = false;
    const recovered = settled(actors, actor.id, () => outputs(actors, actor.id).length === outputCountBeforeProbes + 2);
    await actors.ask(actor.id, "healthy explicit probe");
    await recovered;
    expect(run).toHaveBeenCalledTimes(2);
    expect(run.mock.calls[0]![0].task).toContain("healthy explicit probe");
    expect(run.mock.calls[0]![0].task).not.toContain(first.id);
    expect(run.mock.calls[0]![0].task).not.toContain(second.id);
    const batch = run.mock.calls[1]![0].task;
    expect(batch).toContain(first.id);
    expect(batch).toContain(second.id);
    expect(batch).toContain(JSON.stringify({ activationId: ids[0], batchIds: ids }));
    expect(fs.readFileSync(actor.sessionFile!, "utf8").startsWith(conversationBeforeProbes)).toBe(true);
    expect(sessionHeader()).toBe(headerBeforeProbes);
    expect(backups()).toEqual(backupsBeforeProbes);
    expect(queue(actor.sessionFile!)).toEqual([]);
    expect(actors.status(actor.id).activationBlocked).toBeUndefined();
  });

  it("holds non-binding hard preparation after its retry budget until an explicit reset", async () => {
    let unavailable = true;
    const { actors, agents, mesh } = setup({ preparationRetryMs: 1, acquireCapabilityView: async () => {
      if (unavailable) throw new Error("401 Unauthorized");
      return { satisfied: true, missing: [], optionalMissing: [],
        view: { id: "repaired", digest: "repaired", semanticDigest: "repaired", bindings: {} }, release: async () => {} };
    } });
    const actor = await actors.create({ name: "hard-capability", instructions: "Reply deterministically.",
      topics: ["github.veto-retention"], residency: "durable", responseMode: "text", coalesce: false,
      requires: ["demo.echo"] });
    const run = vi.spyOn(agents, "run");
    const done = settled(actors, actor.id, () => queue(actor.sessionFile!)[0]?.preparationAttempts === 4);
    const event = await publish(mesh);
    await done;
    const retained = queue(actor.sessionFile!);
    expect(retained).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }), attempts: 0, preparationAttempts: 4 })]);
    expect(actors.status(actor.id).activationBlocked?.class).toBe("hard");
    unavailable = false;
    await pause(100);
    expect(run).not.toHaveBeenCalled();
    expect(queue(actor.sessionFile!)).toEqual(retained);
    const recovered = settled(actors, actor.id, () => outputs(actors, actor.id).length === 1);
    await actors.resetSession(actor.id);
    await recovered;
    expect(run).toHaveBeenCalledTimes(1);
    expect(queue(actor.sessionFile!)).toEqual([]);
  });

  it("does not trust veto-shaped binding text or automatically retry its hard preparation", async () => {
    let unavailable = false;
    const { actors, agents, mesh } = setup({ preparationRetryMs: 5, resolvePiModel: async model => {
      if (unavailable) throw new Error("Actor registry update was vetoed by its validation and not committed; retry");
      return model;
    } });
    const actor = await create(actors);
    const run = vi.spyOn(agents, "run");
    unavailable = true;
    const done = settled(actors, actor.id, () => queue(actor.sessionFile!)[0]?.preparationAttempts === 1);
    await publish(mesh);
    await done;
    expect(actors.status(actor.id).activationBlocked).toMatchObject({ class: "hard", code: "failing-preparation" });
    await pause(100);
    expect(queue(actor.sessionFile!)[0]?.preparationAttempts).toBe(1);
    expect(run).not.toHaveBeenCalled();
  });

  it.each(["direct typed", "false-registry wrapped typed"] as const)("P1: a %s veto thrown by binding stays hard until explicit reset", async variant => {
    let unavailable = false;
    const { actors, agents, mesh, root } = setup({ preparationRetryMs: 5, resolvePiModel: async model => {
      if (unavailable) {
        const veto = new ActorRegistryUpdateVetoedError("generation");
        throw variant === "direct typed" ? veto : new ActorPreparationError("session:veto-retention", "registry", veto);
      }
      return model;
    } });
    const actor = await create(actors);
    const run = vi.spyOn(agents, "run");
    unavailable = true;
    const done = settled(actors, actor.id, () => queue(actor.sessionFile!)[0]?.preparationAttempts === 1);
    const event = await publish(mesh, { finding: "P1" });
    await done;
    expect(actors.messages(actor.id).find(message => message.error?.includes("Actor registry update was vetoed"))?.data)
      .toMatchObject({ errorType: "ActorPreparationError", phase: "binding", attempts: 1 });
    const retained = queue(actor.sessionFile!);
    const block = actors.status(actor.id).activationBlocked;
    expect(retained).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }),
      attempts: 0, preparationAttempts: 1 })]);
    expect(JSON.parse(fs.readFileSync(path.join(root, "cursor.json"), "utf8")).last.sequence)
      .toBeGreaterThanOrEqual(event.sequence);
    expect(run).not.toHaveBeenCalled();
    // Neither a repaired resolver nor a newer callerless observation grants recovery.
    unavailable = false;
    const newer = await publish(mesh, { finding: "P1", later: true });
    await pause(150);
    expect(run).not.toHaveBeenCalled();
    expect(block).toMatchObject({ class: "hard", code: "failing-preparation" });
    expect(actors.status(actor.id).activationBlocked).toEqual(block);
    expect(queue(actor.sessionFile!).map(item => item.payload.id)).toEqual([event.id, newer.id]);
    // A restart must not promote the persisted binding wrapper's veto-shaped text
    // into legacy registry evidence and undo the phase-aware hard classification.
    await actors.close();
    await agents.close();
    const nextAgents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const nextRun = vi.spyOn(nextAgents, "run");
    const next = new ActorManager("veto-retention", { id: "session:veto-retention", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, nextAgents, () => {}, {
        actorRoot: path.join(root, "actors"), meshCursorPath: path.join(root, "cursor.json"), persistent: true,
        meshRetentionSweepPath: false, preparationRetryMs: 5, resolvePiModel: async model => model,
      });
    cleanups.push(async () => { await next.close(); await nextAgents.close(); });
    await pause(150);
    expect(nextRun).not.toHaveBeenCalled();
    expect(next.status(actor.id).activationBlocked).toMatchObject({ class: "hard", code: "failing-preparation" });
    expect(queue(actor.sessionFile!).map(item => item.payload.id)).toEqual([event.id, newer.id]);
    const recovered = settled(next, actor.id, () => outputs(next, actor.id).length === 2);
    await next.resetSession(actor.id);
    await recovered;
    await pause(50);
    expect(nextRun).toHaveBeenCalledTimes(2);
    expect(nextRun.mock.calls.filter(([request]) => request.task.includes(event.id))).toHaveLength(1);
    expect(nextRun.mock.calls.filter(([request]) => request.task.includes(newer.id))).toHaveLength(1);
    expect(queue(actor.sessionFile!)).toEqual([]);
    expect(next.status(actor.id).activationBlocked).toBeUndefined();
  });

  it("P2: a failed hard-preparation queue checkpoint holds the original zero-attempt item after owner recreation", async () => {
    let unavailable = false;
    let failQueueWrites = false;
    let acceptedBytes = "";
    let queueFile = "";
    let sessionFile = "";
    let durableFailures = 0;
    const failedWrites: string[] = [];
    const { actors, agents, mesh, root } = setup({ preparationRetryMs: 5, resolvePiModel: async model => {
      if (unavailable) {
        queueFile = path.join(path.dirname(sessionFile), fs.readdirSync(path.dirname(sessionFile))
          .find(file => file.startsWith("queue-") && file.endsWith(".json"))!);
        acceptedBytes = fs.readFileSync(queueFile, "utf8");
        failQueueWrites = true; // Arm only AFTER durable subscription acceptance and before requeue.
        throw new Error("model no longer available");
      }
      return model;
    } });
    const actor = await create(actors);
    sessionFile = actor.sessionFile!;
    // Fail the real atomic queue write only; registry and cursor writes stay real.
    const open = fs.openSync;
    const fault = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const name = String(file);
      if (failQueueWrites && name.startsWith(`${queueFile}.`) && flags === "w") {
        failedWrites.push(name);
        durableFailures++;
        throw Object.assign(new Error("injected queue-directory EACCES at hard-preparation requeue"), { code: "EACCES" });
      }
      return open(file, flags, mode);
    });
    // Soft finally/monitor checkpoints must see the same unavailable queue directory,
    // or they could heal the durable requeue failure before owner recreation.
    const write = fs.writeFileSync;
    const softFault = vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      if (failQueueWrites && typeof file === "string" && file.startsWith(`${queueFile}.`)) {
        failedWrites.push(file);
        throw Object.assign(new Error("injected queue-directory EACCES during soft checkpoint"), { code: "EACCES" });
      }
      return write(file, data, options);
    });
    const runBefore = vi.spyOn(agents, "run");
    unavailable = true;
    const done = settled(actors, actor.id, () => failedWrites.length > 0);
    const event = await publish(mesh, { finding: "P2" });
    await done;
    expect(durableFailures).toBeGreaterThan(0);
    expect(failedWrites.length).toBeGreaterThanOrEqual(durableFailures);
    expect(fs.readFileSync(queueFile, "utf8")).toBe(acceptedBytes);
    const original = queue(actor.sessionFile!);
    expect(original).toEqual([expect.objectContaining({ payload: expect.objectContaining({ id: event.id }),
      attempts: 0, preparationAttempts: 0 })]);
    expect(runBefore).not.toHaveBeenCalled();
    const persisted = JSON.parse(fs.readFileSync(path.join(root, "actors", "actors.json"), "utf8"));
    expect(persisted.actors.find((row: { id: string }) => row.id === actor.id)).toMatchObject({
      rootId: "session:veto-retention", activationBlocked: { class: "hard", code: "failing-preparation" },
    });
    expect(JSON.parse(fs.readFileSync(path.join(root, "cursor.json"), "utf8")).last.sequence)
      .toBeGreaterThanOrEqual(event.sequence);
    await actors.close();
    await agents.close();
    expect(fs.readFileSync(queueFile, "utf8")).toBe(acceptedBytes);
    fault.mockRestore();
    softFault.mockRestore();
    const nextAgents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const run = vi.spyOn(nextAgents, "run");
    const next = new ActorManager("veto-retention", { id: "session:veto-retention", name: "main", kind: "main" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, nextAgents, () => {}, {
        actorRoot: path.join(root, "actors"), meshCursorPath: path.join(root, "cursor.json"), persistent: true,
        meshRetentionSweepPath: false, preparationRetryMs: 5, resolvePiModel: async model => model,
      });
    cleanups.push(async () => { await next.close(); await nextAgents.close(); });
    await pause(150);
    expect(run).not.toHaveBeenCalled();
    expect(queue(actor.sessionFile!)).toEqual(original);
    expect(next.status(actor.id)).toMatchObject({ queued: 1,
      activationBlocked: { class: "hard", code: "failing-preparation" } });
    const recovered = settled(next, actor.id, () => outputs(next, actor.id).length === 2);
    await next.ask(actor.id, "healthy explicit P2 recovery probe");
    await recovered;
    await pause(50);
    expect(run).toHaveBeenCalledTimes(2);
    expect(run.mock.calls[0]![0].task).toContain("healthy explicit P2 recovery probe");
    expect(run.mock.calls[0]![0].task).not.toContain(event.id);
    expect(run.mock.calls.filter(([request]) => request.task.includes(event.id))).toHaveLength(1);
    expect(queue(actor.sessionFile!)).toEqual([]);
    expect(next.status(actor.id).activationBlocked).toBeUndefined();
  });

  it.each(["text", "directive"] as const)("keeps a real launched %s failure terminal across reset (no event replay)", async responseMode => {
    const { actors, agents, mesh } = setup({ preparationRetryMs: 5 });
    const actor = await create(actors, responseMode);
    const run = vi.spyOn(agents, "run");
    const done = settled(actors, actor.id, () => actors.messages(actor.id).some(message => message.error?.includes("Structured agent output was invalid")));
    const event = await publish(mesh, { marker: "FAIL_DIRECTIVE" });
    await done;
    expect(run).toHaveBeenCalledTimes(1);
    expect(run.mock.calls[0]![0].task).toContain(event.id);
    expect(queue(actor.sessionFile!)).toEqual([]);
    const recovered = settled(actors, actor.id, () => outputs(actors, actor.id).length === 1);
    await actors.resetSession(actor.id);
    await publish(mesh, { healthy: true });
    await recovered;
    await pause(50);
    expect(run).toHaveBeenCalledTimes(2);
    expect(run.mock.calls.filter(([request]) => request.task.includes(event.id))).toHaveLength(1);
    expect(queue(actor.sessionFile!)).toEqual([]);
  });
});
