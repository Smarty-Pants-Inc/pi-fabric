import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import {
  actorTreeDeletionHooks, deleteRootActorTree, mainPublicationFenced, mainPublicationFencePath,
} from "../src/topology/main-publication-fence.js";
import { mainDirectory } from "./helpers/dead-root-fixture.js";

// smarty-dev#8090 platform ruling (fabric-store, round 1031): the Main publication fence is Linux-only. Off Linux an
// actor-tree removal still routes through deleteRootActorTree, with main's existing unfenced path on that platform
// (no new Windows primitive; residual smarty-dev#7858): same check-then-delete order, no fence taken, and no Main
// publication blocked by it. Recorded calls only (#4664): every rm of a directory at or under the actor root is
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

const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const simulate = (platform: NodeJS.Platform) => Object.defineProperty(process, "platform", { ...platformDescriptor, value: platform });

afterEach(() => {
  Object.defineProperty(process, "platform", platformDescriptor);
  delete actorTreeDeletionHooks.beforeCheck;
  delete actorTreeDeletionHooks.beforeDelete;
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe("actor-tree removal off Linux takes main's unfenced path through the shared helper (smarty-dev#8090, #7858)", () => {
  it.each(["win32", "darwin"] as const)("simulated %s: an accepted remove is cleaned and close removes the root, both unfenced, in check-then-delete order; a Main publishes meanwhile", async platform => {
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
    const published: unknown[] = [];
    const publish = async () => { published.push(await main.refresh().then(() => "published", error => error)); };
    try {
      const actor = await actors.create({ name: "kept-history", instructions: "Work.", responseMode: "text" });
      const dir = path.join(actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      guardActorTrees([actorRoot], fenced, calls);
      actorTreeDeletionHooks.beforeCheck = async () => { calls.push({ at: "beforeCheck", fenced: fenced() }); await publish(); };
      actorTreeDeletionHooks.beforeDelete = async () => { calls.push({ at: "beforeDelete", fenced: fenced() }); await publish(); };
      simulate(platform);

      const result = await actors.remove(actor.id);
      expect(result).toEqual({ removed: true });
      expect(calls).toEqual([
        { at: "beforeCheck", fenced: false }, { at: "beforeDelete", fenced: false }, { at: "rm", path: path.resolve(dir), fenced: false }]);
      expect(published).toEqual(["published", "published"]);
      expect(fs.existsSync(fenceFile)).toBe(false); // no fence file is ever made off Linux

      // A leftover fence file (e.g. a copied mesh) does not block an off-Linux Main: the fence is Linux-only.
      fs.mkdirSync(path.dirname(fenceFile), { recursive: true }); fs.writeFileSync(fenceFile, "{}");
      expect(fenced()).toBe(false);
      await expect(main.refresh()).resolves.toBeUndefined();
      fs.rmSync(fenceFile, { force: true });

      calls.length = 0;
      await actors.close(); // idempotent: the finally closes again while the guards are still installed
      expect(calls.filter(call => call.at === "rm")).toEqual([{ at: "rm", path: path.resolve(actorRoot), fenced: false }]);
      expect(calls.map(call => call.at)).toEqual(["beforeCheck", "beforeDelete", "rm"]);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n"); // recorded, never performed
    } finally {
      await actors.close().catch(() => undefined); // even on a failed assertion: guards installed, before main/agents/mocks/root
      Object.defineProperty(process, "platform", platformDescriptor);
      await main.close();
      await agents.close();
      vi.restoreAllMocks(); // the tree guards stay installed through remove, close and the finishers above
      fs.rmSync(root, { recursive: true, force: true }); // own parent fixture teardown
    }
  }, 60_000);

  it.each(["win32", "darwin"] as const)("simulated %s: the helper keeps the order check, then delete, and a failed check deletes nothing", async platform => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-platform-helper-"));
    try {
      simulate(platform);
      const order: string[] = [];
      await deleteRootActorTree(meshRoot, "session:helper", () => { order.push("remove"); }, () => { order.push("check"); });
      expect(order).toEqual(["check", "remove"]);
      const remove = vi.fn();
      await expect(deleteRootActorTree(meshRoot, "session:helper", remove, () => { throw new Error("veto"); })).rejects.toThrow("veto");
      expect(remove).not.toHaveBeenCalled();
      expect(fs.existsSync(path.dirname(mainPublicationFencePath(meshRoot, "session:helper")))).toBe(false);
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
