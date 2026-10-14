import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { removeActorOffline } from "../src/actors/remove-offline.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
import {
  actorTreeDeletionHooks, deleteRootActorTree, mainPublicationFenced, mainPublicationFencePath,
} from "../src/topology/main-publication-fence.js";
import { mainDirectory } from "./helpers/dead-root-fixture.js";

// smarty-dev#8090 platform ruling (round 1031, round 4): WHERE the Main publication fence cannot be held, root actor
// tree deletion refuses. Every non-Linux platform lacks the fence (no portable primitive; follow-up smarty-dev#11335),
// so off Linux deleteRootActorTree refuses before any hook, check or tree delete runs, and the tree bytes are kept.
// Recorded calls only (#4664): every rm of a directory at or under the actor root is
// recorded and NOT performed; metadata lock directories really release. The platform is simulated inside the test
// process (Decision C: recorded-callback supplemental, not E2E; native Windows/macOS hosted runs are pending).
type Call = { at: string; fenced: boolean; path?: string };
const METADATA_LOCK = /\.lock(\.dead\.[0-9a-f]+)?$/;
const guardActorTrees = (roots: string[], fenced: () => boolean, calls: Call[]) => {
  const inTree = (file: unknown) => {
    const resolved = path.resolve(String(file));
    if (METADATA_LOCK.test(path.basename(resolved))) return false;
    return roots.some(root => resolved === path.resolve(root) || resolved.startsWith(path.resolve(root) + path.sep)) &&
      fs.existsSync(resolved) && fs.statSync(resolved).isDirectory();
  };
  const record = (file: unknown) => { calls.push({ at: "rm", path: path.resolve(String(file)), fenced: fenced() }); };
  const rmSync = fs.rmSync.bind(fs), rmdirSync = fs.rmdirSync.bind(fs) as (file: fs.PathLike, options?: fs.RmDirOptions) => void, rm = fs.promises.rm.bind(fs.promises);
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { if (inTree(file)) record(file); else rmSync(file, options); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(((file: fs.PathLike, options?: fs.RmDirOptions) => {
    if (inTree(file)) record(file); else rmdirSync(file, options);
  }) as typeof fs.rmdirSync);
  vi.spyOn(fs.promises, "rm").mockImplementation(async (file, options) => { if (inTree(file)) record(file); else await rm(file, options); });
};

// Compare the full fixture tree, including nested binary history, not only its session file.
const treeBytes = (root: string): Array<[string, string]> => {
  const entries: Array<[string, string]> = [];
  const visit = (directory: string) => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const file = path.join(directory, entry.name);
      entries.push([path.relative(root, file), entry.isDirectory() ? "directory" : fs.readFileSync(file).toString("hex")]);
      if (entry.isDirectory()) visit(file);
    }
  };
  visit(root);
  return entries;
};

const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const simulate = (platform: NodeJS.Platform) => Object.defineProperty(process, "platform", { ...platformDescriptor, value: platform });

afterEach(() => {
  Object.defineProperty(process, "platform", platformDescriptor);
  delete actorTreeDeletionHooks.beforeCheck;
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

const OFF_LINUX_REFUSAL = /Root actor tree deletion needs the Main publication fence, which this platform \((win32|darwin)\) lacks \(smarty-dev#8090\); follow-up smarty-dev#11335/;

describe("actor-tree removal off Linux refuses: no Main publication fence there (smarty-dev#8090, follow-up #11335)", () => {
  it.each(["win32", "darwin"] as const)("simulated %s: an accepted remove and a non-persistent close both refuse; no hook, check or tree delete runs and the tree is kept", async platform => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), `fabric-platform-${platform}-`));
    const actorRoot = path.join(root, "actors");
    const rootId = `session:platform-${platform}`;
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 1, transport: "process", sessionExport: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    const actors = new ActorManager(`platform-${platform}`, { id: rootId, name: "main", kind: "main", sessionId: `platform-${platform}` }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, { actorRoot, rootId });
    const main = mainDirectory(mesh.root, rootId);
    const fenceFile = mainPublicationFencePath(mesh.root, rootId);
    const fenced = () => mainPublicationFenced(mesh.root, rootId);
    const calls: Call[] = [];
    try {
      const actor = await actors.create({ name: "kept-history", instructions: "Work.", responseMode: "text" });
      const dir = path.join(actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      fs.mkdirSync(path.join(dir, "history"));
      fs.writeFileSync(path.join(dir, "history", "run.bin"), Buffer.from([0, 255, 13, 10, 42]));
      const originalTree = treeBytes(dir);
      guardActorTrees([actorRoot], fenced, calls);
      actorTreeDeletionHooks.beforeCheck = () => { calls.push({ at: "beforeCheck", fenced: fenced() }); };
      actorTreeDeletionHooks.beforeDelete = () => { calls.push({ at: "beforeDelete", fenced: fenced() }); };
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      simulate(platform);

      const result = await actors.remove(actor.id);
      expect(result).toMatchObject({ removed: true, cleaned: false });
      expect((result as { pending?: string }).pending).toMatch(OFF_LINUX_REFUSAL);
      expect(calls).toEqual([]); // no hook, no check, no tree delete
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(fs.existsSync(fenceFile)).toBe(false); // no fence file is ever made off Linux
      expect(treeBytes(dir)).toEqual(originalTree);
      const retainedRoot = treeBytes(actorRoot);

      await actors.close(); // non-persistent close: the whole actor root delete refuses too
      expect(calls).toEqual([]);
      expect(warn.mock.calls.some(args => OFF_LINUX_REFUSAL.test(String(args[0])))).toBe(true);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(fs.existsSync(actorRoot)).toBe(true);
      expect(treeBytes(dir)).toEqual(originalTree);
      expect(treeBytes(actorRoot)).toEqual(retainedRoot);
    } finally {
      await actors.close().catch(() => undefined); // even on a failed assertion: guards installed, before main/agents/mocks/root
      Object.defineProperty(process, "platform", platformDescriptor);
      await main.close();
      await agents.close();
      vi.restoreAllMocks(); // the tree guards stay installed through remove, close and the finishers above
      fs.rmSync(root, { recursive: true, force: true }); // own parent fixture teardown
    }
  }, 60_000);

  it.each(["win32", "darwin"] as const)("simulated %s: the helper refuses before hooks, check and delete, and makes no fence file", async platform => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-platform-helper-"));
    try {
      const hooks: string[] = [];
      actorTreeDeletionHooks.beforeCheck = () => { hooks.push("beforeCheck"); };
      actorTreeDeletionHooks.beforeDelete = () => { hooks.push("beforeDelete"); };
      simulate(platform);
      const remove = vi.fn(), check = vi.fn();
      await expect(deleteRootActorTree(meshRoot, "session:helper", remove, check)).rejects.toThrow(OFF_LINUX_REFUSAL);
      await expect(deleteRootActorTree(meshRoot, "session:helper", remove)).rejects.toThrow(/root session:helper actor tree is kept/);
      expect(hooks).toEqual([]);
      expect(check).not.toHaveBeenCalled();
      expect(remove).not.toHaveBeenCalled();
      expect(fs.existsSync(path.dirname(mainPublicationFencePath(meshRoot, "session:helper")))).toBe(false);
    } finally {
      Object.defineProperty(process, "platform", platformDescriptor);
      fs.rmSync(meshRoot, { recursive: true, force: true });
    }
  });

  it.each(["win32", "darwin"] as const)("simulated %s: an offline --main-stopped removal refuses first, before the startup claim, host.lock or any tree move", async platform => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-platform-offline-"));
    const directory = path.join(meshRoot, "resident");
    fs.mkdirSync(directory);
    const config = { meshRoot, rootId: "session:offline" } as unknown as ResidentHostConfig; // nothing gets past the early guard
    try {
      const hooks: string[] = [];
      actorTreeDeletionHooks.beforeCheck = () => { hooks.push("beforeCheck"); };
      actorTreeDeletionHooks.beforeDelete = () => { hooks.push("beforeDelete"); };
      simulate(platform);
      await expect(removeActorOffline(directory, config, "some-actor", { mainStoppedAudit: {} as never }))
        .rejects.toThrow(OFF_LINUX_REFUSAL);
      await expect(removeActorOffline(directory, config, "some-actor", { dryRun: true })).rejects.toThrow(OFF_LINUX_REFUSAL);
      expect(fs.readdirSync(directory)).toEqual([]); // no host-fence-establish.lock, no host.lock touched
      expect(fs.readdirSync(meshRoot)).toEqual(["resident"]); // no fence directory, archive or record
      expect(hooks).toEqual([]); // no tree move ran
    } finally {
      Object.defineProperty(process, "platform", platformDescriptor);
      fs.rmSync(meshRoot, { recursive: true, force: true });
    }
  });
});

describe.skipIf(process.platform !== "linux")("Linux keeps the kernel fence (smarty-dev#8090)", () => {
  it("holds the real flock across check, await and delete, and a Main publication inside is refused", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-platform-linux-"));
    const rootId = "session:platform-linux";
    const main = mainDirectory(meshRoot, rootId);
    const fenced = () => mainPublicationFenced(meshRoot, rootId);
    const seen: Array<[string, boolean]> = [];
    let refused: unknown;
    try {
      await deleteRootActorTree(meshRoot, rootId, async () => { seen.push(["remove", fenced()]); }, async () => {
        seen.push(["check", fenced()]);
        refused = await main.refresh().then(() => "published", error => error);
        seen.push(["awaited", fenced()]);
      });
      expect(seen).toEqual([["check", true], ["awaited", true], ["remove", true]]);
      expect(refused).toMatchObject({ code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      expect(fenced()).toBe(false);
      await expect(main.refresh()).resolves.toBeUndefined();
    } finally { await main.close(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
  }, 60_000);

  it("fails closed without flock: the removal and its check are never called", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-platform-noflock-"));
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-platform-nopath-"));
    try {
      vi.stubEnv("PATH", empty);
      const remove = vi.fn(), check = vi.fn();
      await expect(deleteRootActorTree(meshRoot, "session:noflock", remove, check)).rejects.toThrow(/flock|fence/i);
      expect(check).not.toHaveBeenCalled();
      expect(remove).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllEnvs();
      fs.rmSync(empty, { recursive: true, force: true });
      fs.rmSync(meshRoot, { recursive: true, force: true });
    }
  });
});
