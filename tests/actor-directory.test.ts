import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { failActorTreeRemoval } from "./helpers/actor-tree-removal.js";
import { SessionActorOrphans } from "../src/actors/session-orphans.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#8090, follow-up smarty-dev#11335: Linux, Darwin and Windows support the Main publication fence.
// Only unsupported platforms resolve removal with this named refusal pending and the tree kept.
const UNSUPPORTED_PLATFORM = process.platform !== "linux" && process.platform !== "darwin" && process.platform !== "win32";
const REFUSAL = `Root actor tree deletion needs the Main publication fence, which this platform \\(${process.platform}\\) lacks \\(smarty-dev#8090\\); follow-up smarty-dev#11335; the root session:alpha actor tree is kept`;
const UNSUPPORTED_PLATFORM_PENDING = new RegExp(`^removal cleanup failed: ${REFUSAL} \\(a later remove or host start finishes it\\)$`);
const roots: string[] = [];
const directories: ActorDirectory[] = [];
const agents: AgentManager[] = [];

const open = (
  root: string,
  sessionId: string,
  options: { persistent?: boolean; meshCursorPath?: string; canManageActor?: (id: string) => boolean | undefined } = {},
) => {
  const identity: MeshIdentity = {
    id: `session:${sessionId}`,
    name: "main",
    kind: "main",
    sessionId,
  };
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    runRoot: path.join(root, `runs-${sessionId}-${agents.length}`),
  });
  agents.push(manager);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const actorRoots = {
    project: path.join(root, "mesh", "actors"),
    session: path.join(root, "mesh", "actors", sessionId),
  };
  const directory = new ActorDirectory([
    sessionId,
    identity,
    mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
    manager,
    () => {},
    {
      persistent: options.persistent ?? true,
      rootId: identity.id,
      removalRetryMs: 1,
      ...(options.canManageActor ? { canManageActor: options.canManageActor } : {}),
      ...(options.meshCursorPath ? { meshCursorPath: options.meshCursorPath } : {}),
    },
  ], actorRoots, "project");
  directories.push(directory);
  return { directory, actorRoots, mesh, manager };
};

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  await Promise.all(agents.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ActorDirectory", () => {
  // #6087 admission data safety: session mutations must reach the secondary
  // manager, not silently change the project registry or lose its subscriptions.
  it.each(["project", "session"] as const)("forwards setTopics to the %s owner and persists only that registry", async scope => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-topics-"));
    roots.push(root);
    const { directory, actorRoots } = open(root, "alpha");
    const project = await directory.create({ scope: "project", name: "topics-project", instructions: "Work.", topics: ["topics.project"] });
    const session = await directory.create({ scope: "session", name: "topics-session", instructions: "Work.", topics: ["topics.session"] });
    const target = scope === "project" ? project : session;
    const other = scope === "project" ? session : project;
    expect(await directory.setTopics(target.id, ["topics.replaced", "topics.replaced"])).toMatchObject({ id: target.id, scope, topics: ["topics.replaced"] });
    expect(directory.status(target.id).topics).toEqual(["topics.replaced"]);
    expect(directory.status(other.id).topics).toEqual(other.topics);
    const rows = JSON.parse(fs.readFileSync(path.join(actorRoots[scope], "actors.json"), "utf8")).actors;
    expect(rows.find((row: { id: string }) => row.id === target.id).topics).toEqual(["topics.replaced"]);
    expect(await directory.setTopics(target.id, [])).toMatchObject({ id: target.id, scope, topics: [] });
    expect(directory.status(other.id).topics).toEqual(other.topics);
    await directory.close();
    const restored = open(root, "alpha").directory;
    expect(restored.status(target.id)).toMatchObject({ id: target.id, scope, topics: [] });
    expect(restored.status(other.id).topics).toEqual(other.topics);
  });
  it("#6087 routes guest setTopics through the real provider to a secondary session actor and a global template", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-provider-topics-"));
    roots.push(root);
    let owned = true;
    const { directory, mesh, manager } = open(root, "alpha", { canManageActor: () => owned });
    const actor = await directory.create({ scope: "session", name: "topics-guest", instructions: "Work.", topics: ["topics.original"] });
    const globals = new GlobalActorRegistry(root, 64 * 1024);
    const template = globals.create({ name: "topics-template", instructions: "Work.", topics: ["topics.original"] });
    // No participant routing or Main delivery is part of a local setter. These
    // unused ports fail closed if this regression unexpectedly tries either.
    const unused = (): never => { throw new Error("setTopics unexpectedly routed to Main"); };
    const participants = { list: () => [], get: () => undefined, self: unused, peers: () => [], refresh: async () => {}, scheduleRefresh() {} };
    const main = { id: "session:alpha", local: true, matches: (id: string) => id === "session:alpha" || id === "main", info: unused, deliverAgent: unused };
    const lifecycle = new LifecycleBroker(mesh, directory.identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, () => {});
    const provider = new AgentsProvider(manager, directory, globals, main, participants, undefined, lifecycle);
    const context: FabricInvocationContext = { cwd: root, signal: undefined, parentToolCallId: "topics", nestedToolCallId: "topics",
      extensionContext: {} as FabricInvocationContext["extensionContext"], update() {} };
    try {
      const result = await new QuickJsRuntime().execute(
        `const local = await agents.setTopics({ id: ${JSON.stringify(actor.id)}, topics: [] });
         const template = await agents.setTopics({ id: ${JSON.stringify(template.id)}, scope: "global", topics: ["topics.changed"] });
         let rejected = false;
         try { await agents.setTopics({ id: ${JSON.stringify(actor.id)}, topics: ["bad topic"] }); } catch { rejected = true; }
         return { local: local.topics, template: template.topics, rejected };`,
        (ref, args) => provider.invoke(ref.slice("agents.".length), args as Record<string, unknown>, context),
        { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 },
      );
      expect(result.terminationReason).toBe("completed");
      expect(result.value).toEqual({ local: [], template: ["topics.changed"], rejected: true });
      expect(directory.status(actor.id)).toMatchObject({ scope: "session", topics: [], queued: 0 });
      expect(new GlobalActorRegistry(root, 64 * 1024).resolve(template.id)!.topics).toEqual(["topics.changed"]);
      owned = false;
      await expect(provider.invoke("setTopics", { id: actor.id, topics: ["topics.forbidden"] }, context)).rejects.toThrow();
      expect(directory.status(actor.id).topics).toEqual([]);
      expect(manager.list()).toEqual([]);
    } finally { await provider.close(); }
  });

  it("#169 round 2 permits public same-name create while a waiting removal is behind a live run", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-wait-"));
    roots.push(root);
    const { directory, actorRoots, manager } = open(root, "alpha");
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const runId = "d".repeat(32);
    vi.spyOn(manager, "run").mockImplementation(async (_request, _signal, onSpawned) => {
      onSpawned?.({ id: runId } as never);
      await gate;
      return { id: runId, status: "stopped", text: "", usage: {} } as never;
    });
    const actor = await directory.create({ name: "replacement", instructions: "Work." });
    if (UNSUPPORTED_PLATFORM) fs.writeFileSync(path.join(actorRoots.project, actor.id, "kept-11335.txt"), "history\n");
    directory.tell(actor.id, "go");
    await vi.waitFor(() => expect(directory.status(actor.id).inFlightRun?.id).toBe(runId));
    let removed = false;
    // pi-fabric#1572 round 5: the tell in flight is live work; dropQueued archives it instead of refusing the removal.
    const waiting = directory.remove(actor.id, { dropQueued: true }).then(() => { removed = true; });
    await vi.waitFor(() => expect(directory.status(actor.id).status).toBe("stopped"));
    const creating = directory.create({ name: "replacement", instructions: "Successor." });
    try {
      const result = await Promise.race([creating, new Promise<"blocked">((resolve) => setTimeout(resolve, 300, "blocked"))]);
      expect(result).not.toBe("blocked");
      expect(removed).toBe(false);
      expect(directory.status("replacement").id).not.toBe(actor.id);
      release();
      await waiting;
      if (UNSUPPORTED_PLATFORM) {
        expect(directory.pendingRemovals()).toContainEqual(expect.objectContaining({ id: actor.id, state: expect.stringMatching(UNSUPPORTED_PLATFORM_PENDING) }));
        expect(directory.list().filter((entry) => !entry.removal)).toHaveLength(1);
        expect(fs.readFileSync(path.join(actorRoots.project, actor.id, "kept-11335.txt"), "utf8")).toBe("history\n");
        return;
      }
      expect(directory.list()).toHaveLength(1);
    } finally { release(); await Promise.allSettled([waiting, creating]); }
  });

  it.each(["project", "session"] as const)("#169 round 1 retries an exact %s cleanup-only id after exhausted retries and reports it without reviving it", async (scope) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-cleanup-"));
    roots.push(root);
    const { directory, actorRoots, manager } = open(root, "alpha");
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const runId = "b".repeat(32);
    vi.spyOn(manager, "run").mockImplementation(async (_request, _signal, onSpawned) => {
      onSpawned?.({ id: runId } as never);
      await gate;
      return { id: runId, status: "stopped", text: "", usage: {} } as never;
    });
    const actor = await directory.create({ scope, name: "reviewer", instructions: "Work." });
    directory.tell(actor.id, "go");
    await vi.waitFor(() => expect(directory.status(actor.id).inFlightRun?.id).toBe(runId));
    const dir = path.join(actorRoots[scope], actor.id);
    const marker = path.join(actorRoots[scope], `removal-${actor.id}.json`);
    let failures = 0;
    const fail = failActorTreeRemoval(dir, () => { failures++; throw new Error("persistent cleanup failure"); });
    if (UNSUPPORTED_PLATFORM) { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "kept-11335.txt"), "history\n"); }
    try {
      await directory.remove(actor.id, { wait: false, dropQueued: true }); // pi-fabric#1572 round 5: archives the tell in flight
      release();
      await directory.removalSettled(actor.id);
      if (UNSUPPORTED_PLATFORM) expect(failures).toBe(0); // refused before the tree delete: the rm is never reached
      else expect(failures).toBeGreaterThanOrEqual(6);
      expect(directory.pendingRemovals()).toEqual([expect.objectContaining({ id: actor.id, state: expect.stringContaining("cleanup failed") })]);
      if (UNSUPPORTED_PLATFORM) expect(directory.pendingRemovals()[0]!.state).toMatch(UNSUPPORTED_PLATFORM_PENDING);
      expect(directory.list()).toContainEqual(expect.objectContaining({ id: actor.id, scope, status: "stopped", removal: expect.any(Object) }));
      expect(() => directory.status(actor.id)).toThrow(/Unknown Fabric actor/);
      expect(() => directory.tell(actor.id, "must not run")).toThrow();
      expect(directory.listOwned()).toEqual([]);
      const successor = await directory.create({ scope, name: "reviewer", instructions: "Successor." });
      fs.writeFileSync(successor.sessionFile!, "successor\n");
      expect(() => directory.status("reviewer")).not.toThrow();
      await expect(directory.remove(actor.id.slice(0, 12))).rejects.toThrow();
      // Foreign ownership opinions still deny cleanup of the exact id.
      const foreign = open(root, "alpha", { canManageActor: () => false }).directory;
      await foreign.finishPendingRemovals();
      expect(foreign.owns(actor.id)).toBe(false);
      await expect(foreign.remove(actor.id)).rejects.toThrow(/owning host/);
      expect(fs.existsSync(marker)).toBe(true);
      fail.mockRestore();
      if (UNSUPPORTED_PLATFORM) {
        await expect(directory.remove(actor.id)).resolves.toMatchObject({ removed: true, cleaned: false, pending: expect.stringMatching(UNSUPPORTED_PLATFORM_PENDING) });
        expect(fs.readFileSync(path.join(dir, "kept-11335.txt"), "utf8")).toBe("history\n");
        expect(fs.existsSync(marker)).toBe(true);
        expect(directory.status("reviewer").id).toBe(successor.id);
        expect(fs.readFileSync(successor.sessionFile!, "utf8")).toBe("successor\n");
        return;
      }
      await expect(directory.remove(actor.id)).resolves.toEqual({ removed: true });
      expect(directory.pendingRemovals()).toEqual([]);
      expect(fs.existsSync(dir)).toBe(false);
      expect(fs.existsSync(marker)).toBe(false);
      expect(directory.status("reviewer").id).toBe(successor.id);
      expect(fs.readFileSync(successor.sessionFile!, "utf8")).toBe("successor\n");
    } finally { release(); fail.mockRestore(); }
  });

  it("runs project and session actor registries concurrently", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-directory-"));
    roots.push(root);
    const alpha = open(root, "alpha");

    const shared = await alpha.directory.create({
      scope: "project",
      name: "release guardian",
      instructions: "Guard the project release.",
    });
    const privateActor = await alpha.directory.create({
      scope: "session",
      name: "spec supervisor",
      instructions: "Supervise this task only.",
    });
    const idNamedActor = await alpha.directory.create({
      scope: "session",
      name: shared.id,
      instructions: "Exercise exact actor ID routing.",
    });

    expect(alpha.directory.list()).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: shared.id, scope: "project" }),
      expect.objectContaining({ id: privateActor.id, scope: "session" }),
      expect.objectContaining({ id: idNamedActor.id, scope: "session" }),
    ]));
    expect(alpha.directory.status(shared.id)).toMatchObject({ id: shared.id, scope: "project" });
    expect(fs.existsSync(path.join(alpha.actorRoots.project, "actors.json"))).toBe(true);
    expect(fs.existsSync(path.join(alpha.actorRoots.session, "actors.json"))).toBe(true);

    await alpha.directory.setInferenceContext(shared.id, "activation");
    await alpha.directory.setInferenceContext(privateActor.id, "full-history");
    expect(alpha.directory.status(shared.id).inferenceContext).toBe("activation");
    expect(alpha.directory.status(privateActor.id).inferenceContext).toBe("full-history");

    const beta = open(root, "beta");
    expect(beta.directory.list()).toEqual([
      expect.objectContaining({ id: shared.id, scope: "project" }),
    ]);
  });

  it("counts in-flight runs of both scopes for the self-reload busy gate (smarty-dev#2160)", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-directory-"));
    roots.push(root);
    const alpha = open(root, "alpha");
    const supervisor = await alpha.directory.create({ scope: "session", name: "session supervisor", instructions: "HANG" });
    expect(alpha.directory.inFlightCount()).toBe(0);
    // The fake worker reads task payloads, not the actor's system instructions.
    // Keep both runs alive until teardown rather than sampling a brief running state.
    alpha.directory.tell(supervisor.id, "HANG: check the Main");
    await vi.waitFor(() => expect(alpha.directory.status(supervisor.id).status).toBe("running"), { timeout: 5_000 });
    expect(alpha.directory.inFlightCount()).toBe(1);
    const project = await alpha.directory.create({ scope: "project", name: "project guardian", instructions: "HANG" });
    alpha.directory.tell(project.id, "HANG: check the release");
    await vi.waitFor(() => expect(alpha.directory.status(project.id).status).toBe("running"), { timeout: 5_000 });
    expect(alpha.directory.status(supervisor.id).status).toBe("running");
    expect(alpha.directory.inFlightCount()).toBe(2);
  });

  it("retains both scopes' accepted creates as release custody without reporting extra actor runs", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-custody-"));
    roots.push(root);
    const { directory } = open(root, "alpha");
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const reached = new Set<string>();
    const creating = (["project", "session"] as const).map(scope => directory.create({
      scope, name: `${scope} setup`, instructions: "Wait.",
    }, { beforeCommit: async () => { reached.add(scope); await gate; } }));
    try {
      await vi.waitFor(() => expect(reached.size).toBe(2));
      expect(directory.inFlightCount()).toBe(0);
      expect(directory.releaseCustodyCount()).toBe(2);
      release();
      await Promise.all(creating);
      expect(directory.inFlightCount()).toBe(0);
      expect(directory.releaseCustodyCount()).toBe(0);
    } finally { release(); await Promise.allSettled(creating); }
  });

  it.each([false, true])("joins read-triggered orphan custody and prevents checkpoint re-entry (strict: %s)", async strict => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-orphan-checkpoint-"));
    roots.push(root);
    const { directory } = open(root, "alpha");
    const actor = await directory.create({ name: "checkpoint idle", instructions: "Wait." });
    let release!: (value: number) => void;
    const gate = new Promise<number>(resolve => { release = resolve; });
    const reconcile = vi.spyOn(SessionActorOrphans.prototype, "reconcile").mockImplementationOnce(() => gate);
    let checkpoint: Promise<void> | undefined;
    try {
      directory.status(actor.id);
      expect(directory.inFlightCount()).toBe(0);
      expect(directory.releaseCustodyCount()).toBe(1);
      directory.pauseForRelease({ strict });
      let finished = false;
      checkpoint = directory.checkpointForRelease().then(() => { finished = true; });
      directory.status(actor.id);
      directory.list();
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(finished).toBe(false);
      expect(reconcile).toHaveBeenCalledOnce();
      release(0);
      await checkpoint;
      directory.status(actor.id);
      directory.list();
      expect(reconcile).toHaveBeenCalledOnce();
      expect(directory.releaseCustodyCount()).toBe(0);
      expect(directory.releaseCheckpointReceipt()).toMatchObject({ project: { scope: "project" }, session: { scope: "session" } });
    } finally { release(0); await checkpoint?.catch(() => undefined); reconcile.mockRestore(); }
  });

  it("does not write to or delete durable roots from a transient participant runtime", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-directory-"));
    roots.push(root);
    const projectRoot = path.join(root, "mesh", "actors");
    const sentinel = path.join(projectRoot, "keep.txt");
    fs.mkdirSync(projectRoot, { recursive: true });
    fs.writeFileSync(sentinel, "durable");

    const transient = open(root, "child", { persistent: false });
    await transient.directory.create({
      scope: "project",
      name: "temporary project actor",
      instructions: "Stay process-local.",
    });
    await transient.directory.create({
      scope: "session",
      name: "temporary session actor",
      instructions: "Stay process-local too.",
    });
    // On unsupported platforms non-persistent close keeps its actor roots and warns the named refusal; close returns normally.
    const warn = UNSUPPORTED_PLATFORM ? vi.spyOn(console, "warn").mockImplementation(() => {}) : undefined;
    await transient.directory.close();
    if (warn) {
      const kept = new RegExp(`^\\[pi-fabric\\] actor root .+ kept: ${REFUSAL.replace("session:alpha", "session:child")}$`);
      expect(warn.mock.calls.some((args) => kept.test(String(args[0])))).toBe(true);
      warn.mockRestore();
    }

    expect(fs.readFileSync(sentinel, "utf8")).toBe("durable");
    expect(fs.existsSync(path.join(projectRoot, "actors.json"))).toBe(false);
  });

  it("persists independent mesh cursors for concurrent actor scopes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-directory-"));
    roots.push(root);
    const cursor = path.join(root, "actor-mesh-cursor.json");
    const opened = open(root, "alpha", { meshCursorPath: cursor });

    const handle = await opened.manager.spawn({ task: "HANG", transport: "process" });
    await opened.directory.steerRemote(handle.id, "deliver exactly once", "steer");
    const steerFile = path.join(opened.manager.runDirectory(handle.id)!, "steer.jsonl");

    await vi.waitFor(() => {
      expect(fs.existsSync(`${cursor}.project`)).toBe(true);
      expect(fs.existsSync(`${cursor}.session`)).toBe(true);
      expect(fs.existsSync(steerFile)).toBe(true);
    }, { timeout: 5_000 });
    await new Promise((resolve) => setTimeout(resolve, 100));
    const deliveries = fs.readFileSync(steerFile, "utf8")
      .split("\n")
      .filter((line) => line.trim());
    expect(deliveries).toHaveLength(1);
    await opened.manager.stop(handle.id);
  });
});
