import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentManager } from "../src/agents/manager.js";

// smarty-dev#1439: a long-lived actor's session grows until compaction fails. resetSession
// and actors.maxSessionBytes start its next run on a fresh session and keep its mailbox.

const roots: string[] = [];
const managers: Array<{ close(): Promise<void> }> = [];

const waitFor = async (predicate: () => boolean, timeoutMs = process.env.CI ? 10_000 : 3_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for actor state");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

interface SessionRun { task: string; prior: string[] }

const setup = (options: { maxSessionBytes?: number; canManageActor?: (id: string) => boolean | undefined } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reset-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    runRoot: path.join(root, "runs"),
  });
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"),
    persistent: true,
    ...(options.maxSessionBytes !== undefined ? { maxSessionBytes: options.maxSessionBytes } : {}),
    ...(options.canManageActor ? { canManageActor: options.canManageActor } : {}),
  });
  managers.push(actors, agents);
  // The fake worker appends each run's turns to its --session file, as Pi does: a run's
  // prior context is what the file held when it started. `gate` holds a run open.
  const runs: SessionRun[] = [];
  let gate: Promise<void> | undefined;
  const run = agents.run.bind(agents);
  vi.spyOn(agents, "run").mockImplementation(async (request, signal) => {
    const file = request.sessionFile!;
    const prior = fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean) : [];
    runs.push({ task: request.task, prior });
    await gate;
    return run(request, signal);
  });
  return {
    actors, agents, runs, root,
    hold: () => {
      let release!: () => void;
      gate = new Promise((resolve) => { release = resolve; });
      return () => { gate = undefined; release(); };
    },
  };
};

const backups = (sessionFile: string): string[] =>
  fs.readdirSync(path.dirname(sessionFile))
    .filter((name) => name.startsWith("session.jsonl.") && name.endsWith(".bak"))
    .sort();

const resets = (actors: ActorManager, id: string) =>
  actors.messages(id, 50).filter((message) => (message.data as { sessionReset?: unknown } | undefined)?.sessionReset);

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("actor session reset (smarty-dev#1439)", () => {
  it("resets an idle actor: the next run has no prior context and keeps the definition and mailbox", async () => {
    const { actors, runs } = setup();
    const actor = await actors.create({ name: "keeper", instructions: "Remember words.", topics: ["words"] });
    await actors.ask(actor.id, "REMEMBER PELICAN");
    await actors.ask(actor.id, "RECALL");
    expect(runs[1]!.prior.length).toBeGreaterThan(0);
    const before = actors.definition(actor.id);
    const messagesBefore = actors.messages(actor.id, 50).length;

    const info = await actors.resetSession(actor.id);
    expect(info).toMatchObject({ id: actor.id, status: "idle" });
    expect(fs.existsSync(actor.sessionFile!)).toBe(false);
    expect(backups(actor.sessionFile!)).toHaveLength(1);
    expect(actors.definition(actor.id)).toEqual(before);
    const [logged] = resets(actors, actor.id);
    expect(logged).toMatchObject({ direction: "out", source: "fabric-host", reason: "session reset (requested)" });
    expect(logged!.data).toMatchObject({ sessionReset: { trigger: "requested", archived: expect.stringMatching(/session\.jsonl\.\d{8}T\d{9}Z\.bak$/) } });
    expect(actors.messages(actor.id, 50)).toHaveLength(messagesBefore + 1);

    await actors.ask(actor.id, "RECALL");
    expect(runs[2]!.prior).toEqual([]);
  });

  it("waits for a run in flight: it finishes on the old session, queued work runs on the new one", async () => {
    const { actors, runs, hold } = setup();
    const actor = await actors.create({ name: "busy", instructions: "Work." });
    await actors.ask(actor.id, "first");
    const release = hold();
    const inFlight = actors.ask(actor.id, "second");
    await waitFor(() => runs.length === 2);
    const queued = actors.ask(actor.id, "third");
    let settled = false;
    const reset = actors.resetSession(actor.id).then((info) => { settled = true; return info; });
    await new Promise((resolve) => setTimeout(resolve, 50));
    // Not interrupted, not yet reset.
    expect(settled).toBe(false);
    expect(runs).toHaveLength(2);
    expect(fs.existsSync(actor.sessionFile!)).toBe(true);
    release();
    await expect(inFlight).resolves.toMatchObject({ direction: "out" });
    await expect(reset).resolves.toMatchObject({ id: actor.id });
    await queued;
    expect(runs[1]!.prior.length).toBeGreaterThan(0);
    expect(runs[2]).toMatchObject({ prior: [] });
    expect(runs[2]!.task).toContain("third");
    // The archive holds both runs of the old session.
    const [backup] = backups(actor.sessionFile!);
    const archived = fs.readFileSync(path.join(path.dirname(actor.sessionFile!), backup!), "utf8");
    expect(archived).toContain("first");
    expect(archived).toContain("second");
    expect(fs.readFileSync(actor.sessionFile!, "utf8")).not.toContain("second");
  });

  it("resets a session past actors.maxSessionBytes at the next run boundary, and 0 disables it", async () => {
    const { actors, runs } = setup({ maxSessionBytes: 64 });
    const actor = await actors.create({ name: "growing", instructions: "Grow." });
    await actors.ask(actor.id, "one");
    const size = fs.statSync(actor.sessionFile!).size;
    expect(size).toBeGreaterThan(64);
    await actors.ask(actor.id, "two");
    expect(runs[1]!.prior).toEqual([]);
    const [logged] = resets(actors, actor.id);
    expect(logged).toMatchObject({ source: "fabric-host", data: { sessionReset: { trigger: "size", bytes: size } } });
    expect(logged!.reason).toMatch(/^session reset \(size limit\)/);

    const disabled = setup({ maxSessionBytes: 0 });
    const other = await disabled.actors.create({ name: "unbounded", instructions: "Grow." });
    await disabled.actors.ask(other.id, "one");
    await disabled.actors.ask(other.id, "two");
    expect(disabled.runs[1]!.prior.length).toBeGreaterThan(0);
    expect(resets(disabled.actors, other.id)).toEqual([]);
  });

  it("leaves a session at or under the limit alone", async () => {
    const { actors, runs } = setup({ maxSessionBytes: 10_000 });
    const actor = await actors.create({ name: "small", instructions: "Stay small." });
    await actors.ask(actor.id, "one");
    await actors.ask(actor.id, "two");
    expect(runs[1]!.prior.length).toBeGreaterThan(0);
    expect(backups(actor.sessionFile!)).toEqual([]);
  });

  it("keeps only the 2 newest backups", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "churn", instructions: "Churn." });
    const archived: string[] = [];
    for (const word of ["a", "b", "c", "d"]) {
      await actors.ask(actor.id, word);
      const info = await actors.resetSession(actor.id);
      expect(info.id).toBe(actor.id);
      const data = resets(actors, actor.id).at(-1)!.data as { sessionReset: { archived: string } };
      archived.push(path.basename(data.sessionReset.archived));
    }
    expect(new Set(archived).size).toBe(4);
    expect(backups(actor.sessionFile!)).toEqual(archived.slice(-2).sort());
    // A reset without a session file archives nothing.
    await actors.resetSession(actor.id);
    expect(resets(actors, actor.id).at(-1)!.data).toMatchObject({ sessionReset: { archived: null, bytes: 0 } });
    expect(backups(actor.sessionFile!)).toEqual(archived.slice(-2).sort());
  });

  it("orders same-millisecond backups by their suffix when it prunes", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "fast", instructions: "Churn." });
    const dir = path.dirname(actor.sessionFile!);
    fs.mkdirSync(dir, { recursive: true });
    const stamp = "20260927T150000000Z";
    for (const name of [`session.jsonl.${stamp}.bak`, `session.jsonl.${stamp}-1.bak`]) fs.writeFileSync(path.join(dir, name), "{}\n");
    fs.writeFileSync(actor.sessionFile!, "{}\n");
    await actors.resetSession(actor.id);
    expect(backups(actor.sessionFile!)).toHaveLength(2);
    expect(backups(actor.sessionFile!)).toContain(`session.jsonl.${stamp}-1.bak`);
    expect(backups(actor.sessionFile!)).not.toContain(`session.jsonl.${stamp}.bak`);
  });

  // review/astra F1 on #101: a pruned name was reused, sorted oldest and deleted at once.
  it("keeps the 2 newest backups when every reset lands in the same millisecond", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "frozen", instructions: "Churn." });
    const archived: string[] = [];
    for (const word of ["alpha", "bravo", "charlie", "delta"]) {
      await actors.ask(actor.id, word);
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(new Date("2026-09-27T15:00:00.000Z"));
      try {
        await actors.resetSession(actor.id);
      } finally {
        vi.useRealTimers();
      }
      const data = resets(actors, actor.id).at(-1)!.data as { sessionReset: { archived: string } };
      expect(fs.existsSync(data.sessionReset.archived)).toBe(true);
      expect(fs.readFileSync(data.sessionReset.archived, "utf8")).toContain(word);
      archived.push(path.basename(data.sessionReset.archived));
    }
    expect(new Set(archived).size).toBe(4);
    const kept = backups(actor.sessionFile!);
    expect(kept).toHaveLength(2);
    expect(kept.sort()).toEqual(archived.slice(-2).sort());
    const dir = path.dirname(actor.sessionFile!);
    const contents = kept.map((name) => fs.readFileSync(path.join(dir, name), "utf8")).join("\n");
    expect(contents).toContain("charlie");
    expect(contents).toContain("delta");
    expect(contents).not.toContain("bravo");
  });

  it("rejects a reset of an actor another host owns", async () => {
    let owns = true;
    const { actors } = setup({ canManageActor: () => owns });
    const actor = await actors.create({ name: "leased", instructions: "Observe." });
    await actors.ask(actor.id, "one");
    owns = false;
    await expect(actors.resetSession(actor.id)).rejects.toThrow("owned by another host");
    expect(fs.existsSync(actor.sessionFile!)).toBe(true);
    expect(backups(actor.sessionFile!)).toEqual([]);
  });

  it("reads actors.maxSessionBytes from config, 20 MiB by default", () => {
    expect(DEFAULT_FABRIC_CONFIG.actors.maxSessionBytes).toBe(20 * 1024 * 1024);
    expect(normalizeFabricConfig({ actors: { maxSessionBytes: 0 } }).actors.maxSessionBytes).toBe(0);
    expect(normalizeFabricConfig({ actors: { maxSessionBytes: 1234.7 } }).actors.maxSessionBytes).toBe(1234);
    expect(normalizeFabricConfig({ actors: { maxSessionBytes: -5 } }).actors.maxSessionBytes).toBe(0);
    expect(normalizeFabricConfig({}).actors.maxSessionBytes).toBe(20 * 1024 * 1024);
  });
});
