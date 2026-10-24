/**
 * smarty-dev#16357 + #16331: an actor's model-exception lane is basename(root Main cwd).
 * It is captured once at creation and used only while the bound root's own record is
 * unavailable; an unresolvable lane is a distinct, retried `lane-unresolved` refusal.
 * Every case where the fresh root lane resolves keeps exactly main's allow/deny outcome.
 *
 * Non-hosted: a fake participant directory (the same `get(id, now, { fresh })` read the
 * hosts wire into `rootSessionCwd`), a private temp directory and the fake worker.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { admitModelException, type ModelExceptionEntry } from "../src/agents/model-policy-log.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { modelExceptionGrant, withRootExceptionMetadata } from "./helpers/model-exceptions.js";

const hasPosixTrustMetadata = process.platform !== "win32" && typeof process.getuid === "function";
const ROOT_ID = "session:lane-root";
const OTHER_ROOT_ID = "session:other-root";
const identity = { id: ROOT_ID, name: "main", kind: "main" as const, sessionId: "lane-root" };
const opus = "anthropic/claude-opus-5-5";
const safeModel = "provider/safe";
const modelReason = "ORG-2395 review pair exception #16357";
const ACTOR = "opus-reviewer";
const role = `actor:${ACTOR}`;
const host = os.hostname().split(".")[0]!;

type RootRecord = { kind: "root" | "agent"; id: string; cwd: string };

/** Fake participant directory: a stale record is absent from a `{ fresh: true }` read. */
class FakeParticipantDirectory {
  readonly records = new Map<string, RootRecord>();
  readonly stale = new Set<string>();
  failure: Error | undefined;
  /** Fails exactly one later fresh read (1 = the next one), then reads normally again. */
  failReadIn: number | undefined;
  freshReads = 0;
  get(id: string, _now?: number, options: { fresh?: boolean } = {}): RootRecord | undefined {
    if (options.fresh) this.freshReads++;
    if (this.failure) throw this.failure;
    if (options.fresh && this.failReadIn !== undefined && --this.failReadIn === 0) {
      this.failReadIn = undefined;
      throw new MeshReadError();
    }
    if (options.fresh && this.stale.has(id)) return undefined;
    return this.records.get(id);
  }
}

/** A mesh read failure whose message carries a private path that must never be reported. */
class MeshReadError extends Error {
  constructor() { super("EIO reading /home/private/.pi/mesh/participants/session:lane-root.json"); this.name = "MeshReadError"; }
}

const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
});

const waitFor = async (predicate: () => boolean, ms = 10_000): Promise<void> => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for actor state");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function fixture(grants: (rootCwd: string) => ModelExceptionEntry[] = rootCwd => [
  modelExceptionGrant({ issue: "Smarty-Pants-Inc/smarty-dev#16357", model: opus, lane: path.basename(rootCwd), role }),
]) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-exception-lane-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const rootCwd = path.join(root, "lanes", "lane-a");
  const otherCwd = path.join(root, "lanes", "lane-b");
  fs.mkdirSync(rootCwd, { recursive: true });
  fs.mkdirSync(otherCwd, { recursive: true });
  const home = path.join(root, "home");
  fs.mkdirSync(home);
  vi.spyOn(os, "homedir").mockReturnValue(home); // Opus audit rows stay in the fixture.
  const grantPath = path.join(root, "model-exceptions.json");
  fs.writeFileSync(grantPath, JSON.stringify({ version: 1, grants: grants(rootCwd) }), { mode: 0o600 });
  const participants = new FakeParticipantDirectory();
  participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: rootCwd });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const actorRoot = path.join(root, "actors");
  const denials: unknown[] = [];

  /** One host incarnation; call again for a restart over the same registry. */
  const start = (options: { bindRoot?: boolean; maxConcurrent?: number } = {}) => {
    const agents = new AgentManager(rootCwd, {
      ...DEFAULT_FABRIC_CONFIG.agents, thinking: "medium", modelExceptionsPath: grantPath, budgetUsd: 0,
      ...(options.maxConcurrent !== undefined ? { maxConcurrent: options.maxConcurrent } : {}),
    }, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), mainAgentId: ROOT_ID,
      // Exactly the residency host's binding (src/residency/host.ts), over the fake directory.
      ...(options.bindRoot === false ? {} : { rootSessionCwd: () => {
        const record = participants.get(ROOT_ID, undefined, { fresh: true });
        return record?.kind === "root" && record.id === ROOT_ID && typeof record.cwd === "string" && record.cwd.trim().length > 0
          ? record.cwd : undefined;
      } }),
    });
    const runs: Array<{ actorName: string | undefined; status: string; model: string | undefined }> = [];
    const realRun = agents.run.bind(agents);
    vi.spyOn(agents, "run").mockImplementation(async (...args) => {
      let result: Awaited<ReturnType<AgentManager["run"]>>;
      try { result = await realRun(...args); }
      catch (error) { runs.push({ actorName: args[0].actorName, status: "rejected", model: undefined }); throw error; }
      runs.push({ actorName: args[0].actorName, status: result.status, model: result.model });
      return result;
    });
    const realAdmit = agents.admitModelException.bind(agents);
    vi.spyOn(agents, "admitModelException").mockImplementation((...args) => {
      try { return withRootExceptionMetadata(grantPath, () => realAdmit(...args)); }
      catch (error) { denials.push(error); throw error; }
    });
    const actors = new ActorManager("lane-root", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot, persistent: true, rootId: ROOT_ID, preparationRetryMs: 20 });
    let closed = false;
    const close = async () => { if (closed) return; closed = true; await actors.close(); await agents.close(); };
    cleanups.push(close);
    return { agents, actors, close, runs };
  };
  const registryRecord = (id: string) => new ActorRegistryStore(actorRoot).records().find(record => record.id === id);
  const runsOf = (incarnation: { runs: Array<{ actorName: string | undefined; status: string; model: string | undefined }> }, name = ACTOR) =>
    incarnation.runs.filter(run => run.actorName === name);
  return { root, rootCwd, otherCwd, grantPath, participants, denials, start, registryRecord, runsOf };
}

const denialOf = (error: unknown): string | undefined => (error as { denial?: string } | undefined)?.denial;

describe.runIf(hasPosixTrustMetadata)("actor exception lane captured at creation (smarty-dev#16357)", () => {
  it.each([
    ["stale", (f: ReturnType<typeof fixture>) => { f.participants.stale.add(ROOT_ID); }],
    ["missing", (f: ReturnType<typeof fixture>) => { f.participants.records.delete(ROOT_ID); }],
  ] as const)("prepares with the lane persisted at creation when the root record is %s at preparation", async (_label, makeUnavailable) => {
    const f = fixture();
    const first = f.start();
    const actor = await first.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    // A resident host restart: the lane must survive in the registry, not only in memory.
    await first.close();
    makeUnavailable(f);
    const second = f.start();
    const readsBefore = f.participants.freshReads;
    await second.actors.ask(actor.id, "Review PR 1.");
    expect(f.denials).toEqual([]);
    expect(f.runsOf(second)).toEqual([{ status: "completed", model: opus, actorName: ACTOR }]);
    // The fresh root record was still consulted first; the persisted lane only stood in for it.
    expect(f.participants.freshReads).toBeGreaterThan(readsBefore);
    expect(f.registryRecord(actor.id)?.exceptionLane).toEqual({ rootId: ROOT_ID, lane: "lane-a" });
  });

  it("reports lane-unresolved for a lookup error, retries with backoff, and prepares once the root is fresh", async () => {
    const f = fixture();
    const host1 = f.start();
    // No persisted lane: the root lookup failed at creation (as for any actor created before this fix).
    f.participants.failure = new MeshReadError();
    const actor = await host1.actors.create({ name: ACTOR, instructions: "Review.", model: safeModel });
    expect(f.registryRecord(actor.id)?.exceptionLane).toBeUndefined();
    // ORG-2395 re-pin while the root is fresh, then the root's Main goes into a long blocking call.
    f.participants.failure = undefined;
    await host1.actors.setModel(actor.id, opus, "project", undefined, modelReason);
    f.participants.failure = new MeshReadError();
    host1.actors.tell(actor.id, "Review PR 2.");

    // Past the three quick requeues the block is transient (lane-unresolved), never a hard park.
    await waitFor(() => host1.actors.status(actor.id).activationBlocked !== undefined);
    const blocked = host1.actors.status(actor.id);
    expect(blocked.activationBlocked).toMatchObject({ code: "lane-unresolved", class: "transient" });
    // Retrying, like a preparation timeout: the event stays queued, nothing is dropped.
    expect(blocked.status).toBe("failing-preparation");
    expect(blocked.queued).toBe(1);
    expect(f.runsOf(host1)).toEqual([]);
    // #16331: a distinct reason with the error class, never lane-mismatch, paths or the raw message.
    expect(f.denials.length).toBeGreaterThanOrEqual(4);
    expect(f.denials.every(error => denialOf(error) === "lane-unresolved")).toBe(true);
    expect(blocked.lastError).toContain("lane-unresolved; root session lookup failed (MeshReadError)");
    expect(blocked.lastError).not.toContain("lane-mismatch");
    expect(blocked.lastError).not.toContain("/home/private");
    expect(blocked.lastError).not.toContain(f.rootCwd);

    // The root's record is fresh again: the next bounded retry prepares and runs the queued event.
    f.participants.failure = undefined;
    await waitFor(() => f.runsOf(host1).some(run => run.status === "completed"));
    expect(f.runsOf(host1)).toEqual([expect.objectContaining({ status: "completed", model: opus })]);
    await waitFor(() => host1.actors.status(actor.id).status === "idle");
    expect(host1.actors.status(actor.id)).toMatchObject({ queued: 0 });
    expect(host1.actors.status(actor.id).activationBlocked).toBeUndefined();
  });

  it.each([
    // A free permit launches inline: the refusal rejects agents.run.
    ["immediately", false, "rejected"],
    // A run that waited for a permit settles as an unlaunched failed receipt (launchDenial).
    ["after a permit wait", true, "failed"],
  ] as const)("retries, never drops, an activation whose launch admission %s found the root unavailable", async (_label, permitWait, refused) => {
    const f = fixture();
    const host1 = f.start(permitWait ? { maxConcurrent: 1 } : {});
    // No persisted lane (created while the root was unreadable), then re-pinned while it was fresh.
    f.participants.failure = new MeshReadError();
    const actor = await host1.actors.create({ name: ACTOR, instructions: "Review.", model: safeModel });
    f.participants.failure = undefined;
    await host1.actors.setModel(actor.id, opus, "project", undefined, modelReason);
    const holder = permitWait ? await host1.agents.spawn({ task: "HANG", model: safeModel, transport: "process" }) : undefined;
    // Binding admission reads a fresh root; the launch admission (after any permit wait) does not.
    f.participants.failReadIn = 2;
    host1.actors.tell(actor.id, "Review PR 7.");
    if (holder) {
      await waitFor(() => host1.actors.status(actor.id).status === "waiting");
      await host1.agents.stop(holder.id);
    }
    await waitFor(() => f.runsOf(host1).some(run => run.status === "completed"));
    // The refused launch started no worker; the same event then runs exactly once.
    const runs = f.runsOf(host1);
    expect(runs).toHaveLength(2);
    expect(runs[0]).toMatchObject({ actorName: ACTOR, status: refused });
    expect(runs[1]).toEqual({ actorName: ACTOR, status: "completed", model: opus });
    expect(f.denials.map(denialOf)).toEqual(["lane-unresolved"]);
    await waitFor(() => host1.actors.status(actor.id).status === "idle");
    expect(host1.actors.status(actor.id)).toMatchObject({ queued: 0 });
  });

  it("denies a real lane mismatch exactly as before, without consulting the persisted lane", async () => {
    const f = fixture();
    const host1 = f.start();
    // Created under lane-a (which has a grant): with this fix that is also the actor's persisted lane.
    const actor = await host1.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    // The bound root now resolves to a lane with no grant: the fresh lane wins over the persisted one,
    // so every assertion below holds identically on main (which has no persisted lane at all).
    f.participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: f.otherCwd });
    await expect(host1.actors.ask(actor.id, "Review PR 3.")).rejects.toThrow(/lane-mismatch/);
    expect(f.denials.map(denialOf)).toEqual(["lane-mismatch"]);
    // Callerless work keeps main's hard preparation block: no retry, no run.
    host1.actors.tell(actor.id, "Review PR 4.");
    await waitFor(() => host1.actors.status(actor.id).activationBlocked?.code === "failing-preparation");
    await pause(300);
    expect(host1.actors.status(actor.id)).toMatchObject({ status: "queued", queued: 1,
      activationBlocked: expect.objectContaining({ code: "failing-preparation", class: "hard" }) });
    expect(f.denials.map(denialOf)).toEqual(["lane-mismatch", "lane-mismatch"]);
    expect(f.runsOf(host1)).toEqual([]);
  });

  it("leaves a model that needs no exception unaffected while the root is unavailable", async () => {
    const f = fixture();
    const host1 = f.start();
    f.participants.failure = new MeshReadError();
    const actor = await host1.actors.create({ name: "safe-reviewer", instructions: "Review.", model: safeModel });
    await host1.actors.ask(actor.id, "Review PR 5.");
    f.participants.failure = undefined;
    f.participants.stale.add(ROOT_ID);
    await host1.actors.ask(actor.id, "Review PR 6.");
    expect(f.denials).toEqual([]);
    expect(f.runsOf(host1, "safe-reviewer")).toEqual([
      expect.objectContaining({ status: "completed", model: safeModel }),
      expect.objectContaining({ status: "completed", model: safeModel }),
    ]);
  });
});

/**
 * The decision table behind Paul's rule (never add a lockout): with a resolvable lane the
 * persisted lane is never consulted, so the outcome is exactly main's; an unresolvable lane
 * may only become "use this root's persisted lane" or a retryable lane-unresolved refusal.
 */
describe.runIf(hasPosixTrustMetadata)("exception lane decision table (smarty-dev#16357, #16331)", () => {
  type Outcome = { allowed: true; grant: string } | { allowed: false; denial: string | undefined };
  const outcome = (admit: () => Readonly<ModelExceptionEntry>): Outcome => {
    try { return { allowed: true, grant: admit().id }; }
    catch (error) { return { allowed: false, denial: denialOf(error) }; }
  };
  const tableFixture = () => {
    const f = fixture(rootCwd => [
      modelExceptionGrant({ id: "grant-lane-a", issue: "Smarty-Pants-Inc/smarty-dev#16357", model: opus, lane: path.basename(rootCwd), role }),
      modelExceptionGrant({ id: "grant-other-host", issue: "Smarty-Pants-Inc/smarty-dev#16357", model: opus, lane: "lane-c", role, host: "other-host" }),
    ]);
    const { agents } = f.start();
    const ours = (persisted?: { rootId: string; lane: string }) => outcome(() => persisted
      ? agents.admitModelException(opus, modelReason, "pi", role, "medium", persisted)
      : agents.admitModelException(opus, modelReason, "pi", role, "medium"));
    // main's #exceptionLane(): the fresh root basename, else undefined (every failure folded into it).
    const mainOracle = (freshCwd: string | undefined) => outcome(() => withRootExceptionMetadata(f.grantPath, () => admitModelException({
      path: f.grantPath, reason: modelReason, model: opus, role, host, thinking: "medium",
      lane: freshCwd && path.isAbsolute(freshCwd) && path.basename(freshCwd) ? path.basename(freshCwd) : undefined,
    }).entry));
    return { f, ours, mainOracle };
  };
  const persistedVariants = [
    undefined,
    { rootId: ROOT_ID, lane: "lane-a" },
    { rootId: ROOT_ID, lane: "lane-b" },
    { rootId: ROOT_ID, lane: "lane-c" },
    { rootId: OTHER_ROOT_ID, lane: "lane-a" },
  ];

  it("keeps every resolvable-lane outcome identical to main, whatever lane was persisted", () => {
    const { f, ours, mainOracle } = tableFixture();
    const rows: string[] = [];
    for (const freshCwd of [f.rootCwd, f.otherCwd, path.join(f.root, "lanes", "lane-c")]) {
      f.participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: freshCwd });
      const main = mainOracle(freshCwd);
      for (const persisted of persistedVariants) {
        const ours1 = ours(persisted);
        rows.push(`${path.basename(freshCwd)} persisted=${persisted ? `${persisted.rootId}/${persisted.lane}` : "none"} main=${JSON.stringify(main)} new=${JSON.stringify(ours1)}`);
        expect(ours1).toEqual(main);
      }
    }
    // Spot-check the table is not vacuous: one allow and both denial kinds are represented.
    expect(rows.some(row => row.startsWith("lane-a ") && row.includes('"allowed":true'))).toBe(true);
    expect(rows.some(row => row.startsWith("lane-b ") && row.includes('"denial":"lane-mismatch"'))).toBe(true);
    expect(rows.some(row => row.startsWith("lane-c ") && row.includes('"denial":"host-mismatch"'))).toBe(true);
  });

  it("turns an unresolvable lane only into this root's persisted lane or a lane-unresolved refusal", () => {
    const { f, ours, mainOracle } = tableFixture();
    const unresolvable: Array<[string, () => void]> = [
      ["stale record", () => { f.participants.stale.add(ROOT_ID); }],
      ["missing record", () => { f.participants.records.delete(ROOT_ID); }],
      ["lookup error", () => { f.participants.failure = new MeshReadError(); }],
      ["relative cwd", () => { f.participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: "lanes/lane-a" }); }],
    ];
    for (const [label, makeUnresolvable] of unresolvable) {
      f.participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: f.rootCwd });
      f.participants.stale.clear();
      f.participants.failure = undefined;
      makeUnresolvable();
      // main denied every one of these forever as lane-mismatch.
      expect(mainOracle(undefined), label).toEqual({ allowed: false, denial: "lane-mismatch" });
      for (const persisted of persistedVariants) {
        const result = ours(persisted);
        const sameRoot = persisted?.rootId === ROOT_ID;
        // Never wider: exactly the outcome a fresh read of the persisted lane would give, and only for this root.
        const expected = sameRoot ? mainOracle(path.join(f.root, "lanes", persisted.lane)) : { allowed: false, denial: "lane-unresolved" };
        expect(result, `${label} persisted=${JSON.stringify(persisted)}`).toEqual(expected);
      }
    }
  });

  it("keeps lane-mismatch for a host with no root binding at all, even with a persisted lane", () => {
    const f = fixture();
    const { agents } = f.start({ bindRoot: false });
    const denied = outcome(() => agents.admitModelException(opus, modelReason, "pi", role, "medium", { rootId: ROOT_ID, lane: "lane-a" }));
    expect(denied).toEqual({ allowed: false, denial: "lane-mismatch" });
  });

  it("refuses a task spawn with lane-unresolved while the root record is unavailable (#16331)", async () => {
    const f = fixture(rootCwd => [
      modelExceptionGrant({ issue: "Smarty-Pants-Inc/smarty-dev#16357", model: opus, lane: path.basename(rootCwd) }),
    ]);
    const { agents } = f.start();
    f.participants.failure = new MeshReadError();
    await expect(agents.spawn({ task: "bounded task", model: opus, modelReason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "lane-unresolved",
        message: expect.stringContaining("root session lookup failed (MeshReadError)") });
    expect(agents.list()).toEqual([]);
    f.participants.failure = undefined;
    const handle = await agents.spawn({ task: "bounded task", model: opus, modelReason, transport: "process" });
    expect(await agents.wait(handle.id)).toMatchObject({ status: "completed", model: opus });
  });
});
