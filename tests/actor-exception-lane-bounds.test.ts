/**
 * smarty-dev#16357 follow-up: the four P2s of the independent review of pi-fabric#1553.
 * - P2-1: an unreadable root lane never hides a grant's own terminal denial (revoked, expired, ...).
 * - P2-2: an actor's stored lane stands in only for a root whose record is present and was seen
 *   within the limit; a gone or overly old root is a terminal `root-ended` refusal, and an explicit
 *   reset once the root is back is its tested way out.
 * - P2-3: an actor with no stored lane stores the lane of its first fresh admission.
 * - P2-4: each exception audit row says where its lane came from (`lane_source`).
 *
 * Non-hosted: a fake participant directory wired exactly as the residency host wires
 * `rootSessionCwd` and `rootSessionLastSeenMs`, a private temp directory and the fake worker.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager, EXCEPTION_LANE_FALLBACK_MAX_AGE_MS, rootRecordLastSeenMs } from "../src/agents/manager.js";
import { admitModelException, type ModelExceptionEntry } from "../src/agents/model-policy-log.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { modelExceptionGrant, withRootExceptionMetadata } from "./helpers/model-exceptions.js";

const hasPosixTrustMetadata = process.platform !== "win32" && typeof process.getuid === "function";
const ROOT_ID = "session:lane-root";
const identity = { id: ROOT_ID, name: "main", kind: "main" as const, sessionId: "lane-root" };
const opus = "anthropic/claude-opus-5-5";
const safeModel = "provider/safe";
const issue = "Smarty-Pants-Inc/smarty-dev#16357";
const modelReason = "ORG-2395 review pair exception #16357";
const ACTOR = "opus-reviewer";
const role = `actor:${ACTOR}`;
const host = os.hostname().split(".")[0]!;
const MINUTE = 60_000;

type RootRecord = { kind: "root" | "agent"; id: string; cwd: string };
type LastSeenSource = Parameters<typeof rootRecordLastSeenMs>[0];

/** A stale record is absent from a `{ fresh: true }` read, like the real directory. */
class FakeParticipantDirectory {
  readonly records = new Map<string, RootRecord>();
  readonly stale = new Set<string>();
  /** How long ago a stale record's owner lease lapsed (lastKnown); 1 minute unless set. */
  readonly lapsedMs = new Map<string, number>();
  /** Roots with a clean lineage-close receipt: lineageAlive is false once their record is gone. */
  readonly closed = new Set<string>();
  stalled = false;
  failure: Error | undefined;
  get(id: string, _now?: number, options: { fresh?: boolean } = {}): RootRecord | undefined {
    if (this.failure) throw this.failure;
    if (options.fresh && this.stale.has(id)) return undefined;
    return this.records.get(id);
  }
  lastKnown(id: string): { participant: RootRecord; lapsedMs: number } | undefined {
    if (this.failure) throw this.failure;
    const participant = this.records.get(id);
    return participant && this.stale.has(id) ? { participant, lapsedMs: this.lapsedMs.get(id) ?? MINUTE } : undefined;
  }
  lineageAlive(id: string): boolean {
    if (this.failure) throw this.failure;
    return this.records.has(id) || !this.closed.has(id);
  }
  writeStalled(): Error | undefined {
    return this.stalled ? new Error("Fabric mesh is write-stalled") : undefined;
  }
}

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
const denialOf = (error: unknown): string | undefined => (error as { denial?: string } | undefined)?.denial;

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-exception-bounds-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const rootCwd = path.join(root, "lanes", "lane-a");
  const otherCwd = path.join(root, "lanes", "lane-b");
  fs.mkdirSync(rootCwd, { recursive: true });
  fs.mkdirSync(otherCwd, { recursive: true });
  const home = path.join(root, "home");
  fs.mkdirSync(home);
  vi.spyOn(os, "homedir").mockReturnValue(home); // Opus audit rows stay in the fixture.
  const grantPath = path.join(root, "model-exceptions.json");
  const writeGrants = (grants: ModelExceptionEntry[]): void => {
    fs.writeFileSync(grantPath, JSON.stringify({ version: 1, grants }), { mode: 0o600 });
  };
  const grant = (overrides: Partial<ModelExceptionEntry> = {}): ModelExceptionEntry =>
    modelExceptionGrant({ issue, model: opus, lane: "lane-a", role, thinking: "medium", ...overrides });
  writeGrants([grant({ id: "grant-lane-a" })]);
  const participants = new FakeParticipantDirectory();
  participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: rootCwd });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const actorRoot = path.join(root, "actors");
  const denials: unknown[] = [];

  /** One host incarnation; call again for a restart over the same registry. */
  const start = () => {
    const agents = new AgentManager(rootCwd, {
      ...DEFAULT_FABRIC_CONFIG.agents, thinking: "medium", modelExceptionsPath: grantPath, budgetUsd: 0,
    }, {
      runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), mainAgentId: ROOT_ID,
      // Exactly the residency host's wiring (src/residency/host.ts), over the fake directory.
      rootSessionCwd: () => {
        const record = participants.get(ROOT_ID, undefined, { fresh: true });
        return record?.kind === "root" && record.id === ROOT_ID && typeof record.cwd === "string" && record.cwd.trim().length > 0
          ? record.cwd : undefined;
      },
      rootSessionLastSeenMs: () => rootRecordLastSeenMs(participants as unknown as LastSeenSource, ROOT_ID),
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
  /** Rewrite the registry as a Fabric from before #1553 left it: no stored lanes. */
  const stripStoredLanes = (): void => {
    const store = new ActorRegistryStore(actorRoot);
    store.write(store.records().map(({ exceptionLane: _lane, ...record }) => record));
  };
  const runsOf = (incarnation: { runs: Array<{ actorName: string | undefined; status: string; model: string | undefined }> }, name = ACTOR) =>
    incarnation.runs.filter(run => run.actorName === name);
  const auditRows = (): Array<Record<string, unknown>> => {
    const file = path.join(home, ".local", "state", "smarty", "route", "opus-exceptions.jsonl");
    if (!fs.existsSync(file)) return [];
    return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Record<string, unknown>);
  };
  return { root, rootCwd, otherCwd, grantPath, grant, writeGrants, participants, denials, start, registryRecord,
    stripStoredLanes, runsOf, auditRows };
}

describe.runIf(hasPosixTrustMetadata)("P2-1: lane-independent grant fields decide before an unreadable lane", () => {
  const now = Date.now();
  const iso = (at: number) => new Date(at).toISOString();
  // [label, grant overrides, denial when the lane resolves to lane-a]
  const rows: Array<[string, Partial<ModelExceptionEntry>, string | undefined]> = [
    ["an admitting grant", {}, undefined],
    ["a revoked grant", { status: "revoked" }, "revoked"],
    ["an expired grant", { granted_at: iso(now - 2 * 60 * MINUTE), expires_at: iso(now - MINUTE) }, "expired"],
    ["a grant over the 72 h cap", { granted_at: iso(now - MINUTE), expires_at: iso(now + 72 * 60 * MINUTE) }, "expiry-too-long"],
    ["an unlisted reference", { issue: "Smarty-Pants-Inc/smarty-dev#99999",
      reason_url: "https://github.com/Smarty-Pants-Inc/smarty-dev/issues/99999#issuecomment-1" }, "unlisted"],
    ["a grant for another host", { host: "other-host" }, "host-mismatch"],
    ["a grant for another role", { role: "actor:someone-else" }, "role-mismatch"],
    ["a grant for another model", { model: "anthropic/claude-sonnet-5" }, "model-mismatch"],
    ["a grant below the effort", { thinking: "low" }, "thinking-exceeds"],
  ];

  it.each(rows)("denies %s with its own cause, and only an admitting grant waits on the lane", (_label, overrides, resolved) => {
    const f = fixture();
    f.writeGrants([f.grant(overrides)]);
    const admit = (lane: { lane?: string; laneUnresolved?: string; rootEnded?: string }) => {
      try {
        return { allowed: true, grant: withRootExceptionMetadata(f.grantPath, () => admitModelException({
          path: f.grantPath, reason: modelReason, model: opus, role, host, thinking: "medium", ...lane })).entry.lane };
      } catch (error) { return { allowed: false, denial: denialOf(error) }; }
    };
    // A resolved lane: every outcome exactly as before this change.
    expect(admit({ lane: "lane-a" })).toEqual(resolved ? { allowed: false, denial: resolved } : { allowed: true, grant: "lane-a" });
    expect(admit({ lane: "lane-b" })).toEqual({ allowed: false, denial: resolved === "unlisted" ? "unlisted" : "lane-mismatch" });
    // An unreadable lane: the grant's own terminal cause first; lane-unresolved/root-ended only for a grant that would admit.
    expect(admit({ laneUnresolved: "root session lookup failed (MeshReadError)" }))
      .toEqual({ allowed: false, denial: resolved ?? "lane-unresolved" });
    expect(admit({ laneUnresolved: "root session record is missing or not fresh", rootEnded: "root session has ended" }))
      .toEqual({ allowed: false, denial: resolved ?? "root-ended" });
    // A host with no root binding at all is unchanged.
    expect(admit({})).toEqual({ allowed: false, denial: "lane-mismatch" });
  });

  it("refuses a revoked grant terminally while the root lookup fails: the caller gets it once, callerless work parks", async () => {
    const f = fixture();
    const host1 = f.start();
    // No stored lane: the root lookup failed at creation; the Opus re-pin then ran against a fresh root.
    f.participants.failure = new MeshReadError();
    const actor = await host1.actors.create({ name: ACTOR, instructions: "Review.", model: safeModel });
    f.participants.failure = undefined;
    await host1.actors.setModel(actor.id, opus, "project", undefined, modelReason);
    f.writeGrants([f.grant({ id: "grant-lane-a", status: "revoked" })]);
    f.participants.failure = new MeshReadError();
    const before = f.denials.length;
    await expect(host1.actors.ask(actor.id, "Review PR 8.")).rejects.toThrow(/: revoked/);
    // Terminal on the first refusal: no lane-unresolved requeue.
    expect(f.denials.slice(before).map(denialOf)).toEqual(["revoked"]);
    host1.actors.tell(actor.id, "Review PR 9.");
    await waitFor(() => host1.actors.status(actor.id).activationBlocked !== undefined, 5_000);
    await pause(300);
    expect(host1.actors.status(actor.id)).toMatchObject({ status: "queued", queued: 1,
      activationBlocked: expect.objectContaining({ code: "failing-preparation", class: "hard" }) });
    expect(f.denials.slice(before).map(denialOf)).toEqual(["revoked", "revoked"]);
    expect(host1.actors.status(actor.id).lastError).not.toContain("lane-unresolved");
    expect(f.runsOf(host1)).toEqual([]);
  });
});

describe.runIf(hasPosixTrustMetadata)("P2-2: the stored lane is bounded by the root's presence and last-seen age", () => {
  it("measures the root's last-seen age only from proof", () => {
    expect(EXCEPTION_LANE_FALLBACK_MAX_AGE_MS).toBe(30 * MINUTE);
    const directory = new FakeParticipantDirectory();
    const source = directory as unknown as LastSeenSource;
    const record: RootRecord = { kind: "root", id: ROOT_ID, cwd: "/lanes/lane-a" };
    directory.records.set(ROOT_ID, record);
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBe(0);
    directory.stale.add(ROOT_ID);
    directory.lapsedMs.set(ROOT_ID, 5 * MINUTE);
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBe(5 * MINUTE);
    // Owner host gone or replaced (and NATS, which reports no age): not measurable.
    directory.lapsedMs.set(ROOT_ID, Number.POSITIVE_INFINITY);
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBeUndefined();
    // A write-stalled mesh stops every lease: a lapse proves nothing.
    directory.lapsedMs.set(ROOT_ID, 5 * MINUTE);
    directory.stalled = true;
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBeUndefined();
    directory.stalled = false;
    // Another participant kind under the root's id is not the root.
    directory.records.set(ROOT_ID, { ...record, kind: "agent" });
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBeUndefined();
    // No record: unknown, unless a clean lineage close proves the end.
    directory.records.delete(ROOT_ID);
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBeUndefined();
    directory.closed.add(ROOT_ID);
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBe(Number.POSITIVE_INFINITY);
    // A live record wins over everything.
    directory.records.set(ROOT_ID, record);
    directory.stale.clear();
    expect(rootRecordLastSeenMs(source, ROOT_ID)).toBe(0);
  });

  it.each([
    ["gone, with a clean lineage close", (f: ReturnType<typeof fixture>) => {
      f.participants.records.delete(ROOT_ID); f.participants.closed.add(ROOT_ID);
    }, "root-ended"],
    ["last seen 2 hours ago", (f: ReturnType<typeof fixture>) => {
      f.participants.stale.add(ROOT_ID); f.participants.lapsedMs.set(ROOT_ID, 120 * MINUTE);
    }, "root-ended"],
    ["last seen 31 minutes ago", (f: ReturnType<typeof fixture>) => {
      f.participants.stale.add(ROOT_ID); f.participants.lapsedMs.set(ROOT_ID, 31 * MINUTE);
    }, "root-ended"],
    ["missing, with no close receipt", (f: ReturnType<typeof fixture>) => {
      f.participants.records.delete(ROOT_ID);
    }, "lane-unresolved"],
    ["stale on a write-stalled mesh", (f: ReturnType<typeof fixture>) => {
      f.participants.stale.add(ROOT_ID); f.participants.stalled = true;
    }, "lane-unresolved"],
  ] as const)("never uses the stored lane when the root is %s", async (_label, makeUnavailable, denial) => {
    const f = fixture();
    const first = f.start();
    const actor = await first.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    await first.close();
    expect(f.registryRecord(actor.id)?.exceptionLane).toEqual({ rootId: ROOT_ID, lane: "lane-a" });
    makeUnavailable(f);
    const second = f.start();
    const before = f.denials.length;
    await expect(second.actors.ask(actor.id, "Review PR 1.")).rejects.toThrow(new RegExp(`: ${denial}`));
    const denials = f.denials.slice(before).map(denialOf);
    expect(denials.length).toBeGreaterThan(0);
    expect(denials.every(value => value === denial)).toBe(true);
    // root-ended is terminal: one refusal, never requeued.
    if (denial === "root-ended") expect(denials).toEqual(["root-ended"]);
    expect(f.runsOf(second)).toEqual([]);
  });

  it("keeps the stored lane for a stale root last seen within the limit", async () => {
    const f = fixture();
    const first = f.start();
    const actor = await first.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    await first.close();
    f.participants.stale.add(ROOT_ID);
    f.participants.lapsedMs.set(ROOT_ID, 29 * MINUTE);
    const second = f.start();
    await second.actors.ask(actor.id, "Review PR 2.");
    expect(f.denials).toEqual([]);
    expect(f.runsOf(second)).toEqual([{ actorName: ACTOR, status: "completed", model: opus }]);
  });

  it("parks callerless work hard as root-ended, and a reset once the root is back runs it (the way back)", async () => {
    const f = fixture();
    const first = f.start();
    const actor = await first.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    await first.close();
    f.participants.records.delete(ROOT_ID);
    f.participants.closed.add(ROOT_ID);
    const second = f.start();
    second.actors.tell(actor.id, "Review PR 3.");
    await waitFor(() => second.actors.status(actor.id).activationBlocked !== undefined, 5_000);
    await pause(300);
    expect(second.actors.status(actor.id)).toMatchObject({ status: "queued", queued: 1,
      activationBlocked: expect.objectContaining({ code: "failing-preparation", class: "hard" }) });
    expect(f.denials.map(denialOf)).toEqual(["root-ended"]);
    expect(second.actors.status(actor.id).lastError).toContain("root-ended");
    expect(f.runsOf(second)).toEqual([]);
    // The root's Main is resumed: a parked actor stays parked until an explicit reset.
    f.participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: f.rootCwd });
    f.participants.closed.delete(ROOT_ID);
    await pause(200);
    expect(f.runsOf(second)).toEqual([]);
    await second.actors.resetSession(actor.id);
    await waitFor(() => f.runsOf(second).some(run => run.status === "completed"));
    expect(f.runsOf(second)).toEqual([{ actorName: ACTOR, status: "completed", model: opus }]);
    await waitFor(() => second.actors.status(actor.id).status === "idle");
    expect(second.actors.status(actor.id)).toMatchObject({ queued: 0 });
    expect(second.actors.status(actor.id).activationBlocked).toBeUndefined();
  });

  it("refuses a task spawn as root-ended once its root has ended, and admits it again when the root is back", async () => {
    const f = fixture();
    f.writeGrants([f.grant({ role: "task" })]);
    const { agents } = f.start();
    f.participants.records.delete(ROOT_ID);
    f.participants.closed.add(ROOT_ID);
    await expect(agents.spawn({ task: "bounded task", model: opus, modelReason, transport: "process" }))
      .rejects.toMatchObject({ code: "FABRIC_MODEL_EXCEPTION_DENIED", denial: "root-ended",
        message: expect.stringContaining("root session has ended") });
    expect(agents.list()).toEqual([]);
    f.participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: f.rootCwd });
    f.participants.closed.delete(ROOT_ID);
    const handle = await agents.spawn({ task: "bounded task", model: opus, modelReason, transport: "process" });
    expect(await agents.wait(handle.id)).toMatchObject({ status: "completed", model: opus });
  });
});

describe.runIf(hasPosixTrustMetadata)("P2-3: an actor with no stored lane stores the lane of its first fresh admission", () => {
  it("backfills a pre-change actor on a fresh admission, then uses that lane while the root is stale", async () => {
    const f = fixture();
    const first = f.start();
    const actor = await first.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    await first.close();
    f.stripStoredLanes();
    expect(f.registryRecord(actor.id)).toBeDefined();
    expect(f.registryRecord(actor.id)?.exceptionLane).toBeUndefined();
    const second = f.start();
    await second.actors.ask(actor.id, "Review PR 4.");
    await waitFor(() => f.registryRecord(actor.id)?.exceptionLane !== undefined, 7_000);
    expect(f.registryRecord(actor.id)?.exceptionLane).toEqual({ rootId: ROOT_ID, lane: "lane-a" });
    await second.close();
    // The stored copy now stands in for a stale root, as for an actor created after #1553.
    f.participants.stale.add(ROOT_ID);
    const third = f.start();
    await third.actors.ask(actor.id, "Review PR 5.");
    expect(f.denials).toEqual([]);
    expect(f.runsOf(third)).toEqual([{ actorName: ACTOR, status: "completed", model: opus }]);
  });

  it("backfills an actor whose capture failed at creation when its Opus re-pin reads a fresh root", async () => {
    const f = fixture();
    const host1 = f.start();
    f.participants.failure = new MeshReadError();
    const actor = await host1.actors.create({ name: ACTOR, instructions: "Review.", model: safeModel });
    f.participants.failure = undefined;
    await waitFor(() => f.registryRecord(actor.id) !== undefined);
    expect(f.registryRecord(actor.id)?.exceptionLane).toBeUndefined();
    await host1.actors.setModel(actor.id, opus, "project", undefined, modelReason);
    await waitFor(() => f.registryRecord(actor.id)?.exceptionLane !== undefined, 7_000);
    expect(f.registryRecord(actor.id)?.exceptionLane).toEqual({ rootId: ROOT_ID, lane: "lane-a" });
  });

  it("never replaces a stored lane with a later fresh one", async () => {
    const f = fixture();
    f.writeGrants([f.grant({ id: "grant-lane-a" }), f.grant({ id: "grant-lane-b", lane: "lane-b" })]);
    const host1 = f.start();
    const actor = await host1.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    // The root moved to lane-b (also granted): the fresh lane admits, but the stored lane-a stays.
    f.participants.records.set(ROOT_ID, { kind: "root", id: ROOT_ID, cwd: f.otherCwd });
    await host1.actors.ask(actor.id, "Review PR 6.");
    await pause(200);
    expect(f.registryRecord(actor.id)?.exceptionLane).toEqual({ rootId: ROOT_ID, lane: "lane-a" });
    expect(f.runsOf(host1)).toEqual([{ actorName: ACTOR, status: "completed", model: opus }]);
  });
});

describe.runIf(hasPosixTrustMetadata)("P2-4: the exception audit row records lane_source", () => {
  it("writes lane_source root for a fresh root lane and persisted for the stored lane", async () => {
    const f = fixture();
    const first = f.start();
    const actor = await first.actors.create({ name: ACTOR, instructions: "Review.", model: opus, modelReason });
    await first.actors.ask(actor.id, "Review PR 7.");
    await first.close();
    f.participants.stale.add(ROOT_ID);
    const second = f.start();
    await second.actors.ask(actor.id, "Review PR 8.");
    const rows = f.auditRows().filter(row => row.role === role && typeof row.run_id === "string");
    expect(rows.map(row => row.lane_source)).toEqual(["root", "persisted"]);
    expect(rows.every(row => row.lane === "lane-a" && row.grant_id === "grant-lane-a")).toBe(true);
  });

  it("writes lane_source root for a task spawn", async () => {
    const f = fixture();
    f.writeGrants([f.grant({ id: "grant-task", role: "task" })]);
    const { agents } = f.start();
    const handle = await agents.spawn({ task: "bounded task", model: opus, modelReason, transport: "process" });
    expect(await agents.wait(handle.id)).toMatchObject({ status: "completed", model: opus });
    const rows = f.auditRows().filter(row => row.run_id === handle.id);
    expect(rows).toEqual([expect.objectContaining({ grant_id: "grant-task", lane: "lane-a", lane_source: "root" })]);
  });
});
