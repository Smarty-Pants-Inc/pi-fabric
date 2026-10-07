import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

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
  it("#169 round 2 permits public same-name create while a waiting removal is behind a live run", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-directory-wait-"));
    roots.push(root);
    const { directory, manager } = open(root, "alpha");
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const runId = "d".repeat(32);
    vi.spyOn(manager, "run").mockImplementation(async (_request, _signal, onSpawned) => {
      onSpawned?.({ id: runId } as never);
      await gate;
      return { id: runId, status: "stopped", text: "", usage: {} } as never;
    });
    const actor = await directory.create({ name: "replacement", instructions: "Work." });
    directory.tell(actor.id, "go");
    await vi.waitFor(() => expect(directory.status(actor.id).inFlightRun?.id).toBe(runId));
    let removed = false;
    const waiting = directory.remove(actor.id).then(() => { removed = true; });
    await vi.waitFor(() => expect(directory.status(actor.id).status).toBe("stopped"));
    const creating = directory.create({ name: "replacement", instructions: "Successor." });
    try {
      const result = await Promise.race([creating, new Promise<"blocked">((resolve) => setTimeout(resolve, 300, "blocked"))]);
      expect(result).not.toBe("blocked");
      expect(removed).toBe(false);
      expect(directory.status("replacement").id).not.toBe(actor.id);
      release();
      await waiting;
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
    const rm = fs.rmSync.bind(fs);
    let failures = 0;
    const fail = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (target === dir) { failures++; throw new Error("persistent cleanup failure"); }
      return rm(target, options);
    });
    try {
      await directory.remove(actor.id, { wait: false });
      release();
      await directory.removalSettled(actor.id);
      expect(failures).toBeGreaterThanOrEqual(6);
      expect(directory.pendingRemovals()).toEqual([expect.objectContaining({ id: actor.id, state: expect.stringContaining("cleanup failed") })]);
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
    await transient.directory.close();

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
