import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { WorktreeManager } from "../src/agents/worktree-manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

// Keep reason: PR #1726 data-safety and real cleanup/actor lifecycle regression.
// Native git removal is hosted-only. Substitute only that boundary with an
// owned synthetic directory; worker, exit, settlement and actor cleanup are real.
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of closers.splice(0).reverse()) await close(); }
  finally {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});
const setup = (retainRuns: boolean) => {
  // This fixture tests exit/cleanup, not delegated systemd task-scope admission.
  vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
  vi.stubEnv("XDG_RUNTIME_DIR", undefined);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "retained-original-cleanup-"));
  roots.push(root);
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns, model: "fixture/model" }, {
    workerPath: path.resolve("tests/fixtures/retained-original-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  closers.push(() => agents.close());
  return { root, agents };
};
const originalBytes = (run: string) => fs.readFileSync(path.join(run, "events.jsonl.original"));
const sha256 = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");

describe("settlement and cleanup with a retained original", () => {
  it("releases an offline durable handle but keeps the original run log", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "retained-original-durable-cleanup-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const identity = { id: "session:retained-original", name: "main", kind: "main", sessionId: "retained-original" } as const;
    const config = {
      format: RESIDENT_HOST_FORMAT, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
      meshRoot, actorRoot: path.join(meshRoot, "actors"), sessionActorRoot: path.join(meshRoot, "actors", identity.sessionId),
      residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("tests/fixtures/retained-original-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    } as ResidentHostConfig;
    const id = "a".repeat(32);
    const run = path.join(config.residencyRoot, "runs", id);
    const agents = path.join(config.residencyRoot, "agents");
    fs.mkdirSync(run, { recursive: true });
    fs.mkdirSync(agents, { recursive: true });
    const result = { id, name: "durable worker", status: "completed", text: "done", task: "work", runner: "pi",
      transport: "process", sessionId: "2147483647", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2,
      turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(result));
    const original = Buffer.from("exact synthetic durable trace\r\n");
    fs.writeFileSync(path.join(run, "events.jsonl"), "compacted reader view\n");
    fs.writeFileSync(path.join(run, "events.jsonl.original"), original);
    const metadataPath = path.join(agents, `${id}.json`);
    fs.writeFileSync(metadataPath, JSON.stringify({ format: RESIDENT_HOST_FORMAT, rootId: identity.id, id, runDirectory: run,
      handle: result, createdAt: 1, updatedAt: 2 }));
    const client = new ResidencyClient({ config, mesh: new MeshStore(meshRoot, 64 * 1024, 100),
      participants: { self: () => identity } as never, mainAgent: { id: identity.id, local: true } as never });
    try {
      await expect(client.cleanupAgent(id)).resolves.toEqual({ cleaned: true });
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.readFileSync(path.join(run, "events.jsonl.original"))).toEqual(original);
      expect(fs.existsSync(metadataPath)).toBe(false);
    } finally { await client.close(); }
  });

  it.each([false, true])("cleans the worktree and forgets the handle, retaining raw bytes (retainRuns=%s)", async retainRuns => {
    const { root, agents } = setup(retainRuns);
    const worktree = path.join(root, "synthetic-worktree");
    fs.mkdirSync(worktree);
    fs.writeFileSync(path.join(worktree, "fixture.txt"), "synthetic worktree");
    vi.spyOn(WorktreeManager.prototype, "create").mockResolvedValue({ gitRoot: root, path: worktree, cwd: worktree, branch: "fixture" });
    const cleanup = vi.spyOn(WorktreeManager.prototype, "cleanup").mockImplementation(async () => {
      fs.rmSync(worktree, { recursive: true });
      return true;
    });
    const result = await agents.run({ task: "synthetic run", transport: "process", worktree: true });
    expect(result.status, result.error).toBe("completed");
    const run = agents.runDirectory(result.id)!;
    const original = originalBytes(run);
    expect(original.length).toBeGreaterThan(8 * 1024);
    await expect(agents.cleanup(result.id, true)).resolves.toEqual({ cleaned: true });
    expect(cleanup).toHaveBeenCalledWith(result.id, true);
    expect(fs.existsSync(worktree)).toBe(false);
    expect(agents.list()).toEqual([]);
    expect(originalBytes(run)).toEqual(original);
    await agents.close();
    expect(originalBytes(run)).toEqual(original);
  });

  it("keeps nested originals when explicit cleanup forgets the parent handle", async () => {
    const { agents } = setup(false);
    const result = await agents.run({ task: "synthetic parent", transport: "process" });
    const run = agents.runDirectory(result.id)!;
    const nested = path.join(run, "nested", "child");
    fs.mkdirSync(nested, { recursive: true });
    for (const name of ["events.jsonl", "events.jsonl.original"]) fs.renameSync(path.join(run, name), path.join(nested, name));
    fs.copyFileSync(path.join(run, "status.json"), path.join(nested, "status.json"));
    const bytes = originalBytes(nested);
    await expect(agents.cleanup(result.id)).resolves.toEqual({ cleaned: true });
    expect(agents.list()).toEqual([]);
    expect(originalBytes(nested)).toEqual(bytes);
  });

  it("keeps originals through managed-temp unsuccessful-prompt pruning on close", async () => {
    const { root } = setup(false);
    const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, model: "fixture/model" }, {
      workerPath: path.resolve("tests/fixtures/retained-original-worker.mjs"),
    });
    closers.push(() => agents.close());
    const result = await agents.run({ task: "SYNTHETIC_FAILURE", transport: "process" });
    expect(result.status).toBe("failed");
    const run = agents.runDirectory(result.id)!;
    roots.push(path.dirname(run));
    const bytes = originalBytes(run);
    const childRunRoot = path.join(run, "nested");
    const childManager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, model: "fixture/model" }, {
      workerPath: path.resolve("tests/fixtures/retained-original-worker.mjs"), runRoot: childRunRoot,
    });
    const child = await childManager.run({ task: "synthetic nested child", transport: "process" });
    expect(child.status).toBe("completed");
    const childDirectory = childManager.runDirectory(child.id)!;
    const childOriginal = originalBytes(childDirectory);
    const childSha = sha256(childOriginal);
    expect(childOriginal.length).toBeGreaterThan(8 * 1024);
    await childManager.close();
    await agents.close();
    expect(originalBytes(run)).toEqual(bytes);
    expect(sha256(fs.readFileSync(path.join(childDirectory, "events.jsonl.original")))).toBe(childSha);
    expect(fs.readFileSync(path.join(childDirectory, "events.jsonl.original"))).toEqual(childOriginal);
    expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"))).toMatchObject({ status: "failed" });
  });

  it.each([false, true])("releases completed actor handles and slot custody with retained raw bytes (retainRuns=%s)", async retainRuns => {
    const { root, agents } = setup(retainRuns);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const actors = new ActorManager("fixture", { id: "session:fixture", name: "owner", kind: "main", sessionId: "fixture" },
      mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: false, resolvePiModel: async model => model,
      });
    closers.push(() => actors.close());
    const actor = await actors.create({ name: "fixture", instructions: "Reply to synthetic messages.", model: "fixture/model",
      transport: "process", responseMode: "text" });
    const saved: Array<{ run: string; bytes: Buffer }> = [];
    for (let turn = 0; turn < 2; turn++) {
      await actors.ask(actor.id, `synthetic turn ${turn}`);
      await vi.waitFor(() => expect(actors.status(actor.id).status).toBe("idle"));
      const id = actors.status(actor.id).lastRunId!;
      const run = path.join(root, "runs", id);
      saved.push({ run, bytes: originalBytes(run) });
      expect(agents.list(), "completed actor handle must be forgotten").toEqual([]);
      expect(actors.readLog(actor.id, { type: "all" }).run).toBeDefined();
    }
    // Actor removal's slot-protection proof must not mistake retained trace
    // files for a live run. Whole actor trees are archived, not deleted.
    await expect(actors.remove(actor.id)).resolves.toMatchObject({ removed: true });
    await actors.close();
    await agents.close();
    for (const savedRun of saved) expect(originalBytes(savedRun.run)).toEqual(savedRun.bytes);
  }, 60_000);
});
