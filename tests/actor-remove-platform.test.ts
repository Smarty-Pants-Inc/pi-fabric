import { spawn } from "node:child_process";
import { hostedOnly } from "../scripts/hosted-only.mjs";
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
  actorTreeDeletionHooks, assertMainPublicationFencePlatform, deleteRootActorTree, mainPublicationFenced,
  mainPublicationFencePath, takeMainPublicationFence,
} from "../src/topology/main-publication-fence.js";
import { mainDirectory } from "./helpers/dead-root-fixture.js";

// smarty-dev#8090: WHERE the Main publication fence cannot be held, root actor tree deletion refuses before
// any hook, check or tree delete. #11335 adds Darwin/Windows support; aix/freebsd remain unsupported.
// Recorded calls only (#4664): every rm of a directory at or under the actor root is recorded and NOT
// performed; metadata lock directories really release. Unsupported-platform simulation tests the early guard,
// never foreign numeric open flags. The support assertion below is not evidence of native platform behavior.
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

const UNSUPPORTED_REFUSAL = /Root actor tree deletion needs the Main publication fence, which this platform \((aix|freebsd)\) lacks \(smarty-dev#8090\); follow-up smarty-dev#11335/;

describe("Main publication fence platform support (#11335; assertion only, not native locking)", () => {
  it.each(["darwin", "win32"] as const)("simulated %s: assertMainPublicationFencePlatform accepts a supported platform without I/O", platform => {
    simulate(platform);
    expect(() => assertMainPublicationFencePlatform("session:platform-support")).not.toThrow();
  });
});

describe("actor-tree removal on unsupported platforms refuses (smarty-dev#8090, follow-up #11335)", () => {
  it.each(["aix", "freebsd"] as const)("simulated %s: an accepted remove and a non-persistent close both refuse; no hook, check or tree delete runs and the tree is kept", async platform => {
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
      expect((result as { pending?: string }).pending).toMatch(UNSUPPORTED_REFUSAL);
      expect(calls).toEqual([]); // no hook, no check, no tree delete
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(fs.existsSync(fenceFile)).toBe(false); // no fence file is ever made on unsupported platforms
      expect(treeBytes(dir)).toEqual(originalTree);
      const retainedRoot = treeBytes(actorRoot);

      await actors.close(); // non-persistent close: the whole actor root delete refuses too
      expect(calls).toEqual([]);
      expect(warn.mock.calls.some(args => UNSUPPORTED_REFUSAL.test(String(args[0])))).toBe(true);
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

  it.each(["aix", "freebsd"] as const)("simulated %s: the helper refuses before hooks, check and delete, and makes no fence file", async platform => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-platform-helper-"));
    try {
      const hooks: string[] = [];
      actorTreeDeletionHooks.beforeCheck = () => { hooks.push("beforeCheck"); };
      actorTreeDeletionHooks.beforeDelete = () => { hooks.push("beforeDelete"); };
      simulate(platform);
      const remove = vi.fn(), check = vi.fn();
      await expect(deleteRootActorTree(meshRoot, "session:helper", remove, check)).rejects.toThrow(UNSUPPORTED_REFUSAL);
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

  it.each(["aix", "freebsd"] as const)("simulated %s: an offline --main-stopped removal refuses first, before the startup claim, host.lock or any tree move", async platform => {
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
        .rejects.toThrow(UNSUPPORTED_REFUSAL);
      await expect(removeActorOffline(directory, config, "some-actor", { dryRun: true })).rejects.toThrow(UNSUPPORTED_REFUSAL);
      expect(fs.readdirSync(directory)).toEqual([]); // no host-fence-establish.lock, no host.lock touched
      expect(fs.readdirSync(meshRoot)).toEqual(["resident"]); // no fence directory, archive or record
      expect(hooks).toEqual([]); // no tree move ran
    } finally {
      Object.defineProperty(process, "platform", platformDescriptor);
      fs.rmSync(meshRoot, { recursive: true, force: true });
    }
  });
});

// Linux runs locally; Darwin/Windows need a real CI host. No process.platform override in native tests.
const nativeFencePlatform = process.platform === "linux" ||
  (["darwin", "win32"].includes(process.platform) && hostedOnly("off-Linux native fence lifecycle"));
const within = async <T>(result: Promise<T>, milliseconds: number, message: () => string): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([result, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(message())), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
};

describe.runIf(nativeFencePlatform)("real-platform fence lifecycle (#11335)", () => {
  it("native publication fence", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-publication-fence-"));
    const rootId = "session:native-publication-fence";
    const main = mainDirectory(meshRoot, rootId); // real ParticipantDirectory, not a publication stub
    const file = mainPublicationFencePath(meshRoot, rootId);
    let release: (() => void) | undefined;
    // Inline equivalent of the existing jiti process fixture, but only lock/probe/publication/death:
    // no actor manager, tree deletion helper, removal callback, or child release/exit handler.
    const child = spawn(process.execPath, ["--input-type=module", "-e", `
      // Bound the child even if its parent fails; install before the first dynamic import.
      setTimeout(() => process.exit(2), 30000);
      const { createJiti } = await import("jiti");
      const { pathToFileURL } = await import("node:url");
      const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
      const { takeMainPublicationFence } = await jiti.import("./src/topology/main-publication-fence.ts");
      const [meshRoot, rootId] = process.argv.slice(1);
      await takeMainPublicationFence(meshRoot, rootId);
      process.send({ event: "ready", pid: process.pid, platform: process.platform });
    `, meshRoot, rootId], { cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe", "ipc"] });
    let stderr = "";
    child.stderr!.on("data", chunk => { stderr = (stderr + String(chunk)).slice(-8192); });
    const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    });
    exited.catch(() => undefined);
    try {
      const ready = new Promise<unknown>((resolve, reject) => {
        child.once("message", resolve);
        child.once("error", reject);
        child.once("close", (code, signal) => reject(new Error(`Fence child exited before ready (${code}/${signal}): ${stderr}`)));
      });
      expect(await within(ready, 15_000, () => `Fence child readiness timed out: ${stderr}`))
        .toEqual({ event: "ready", pid: child.pid, platform: process.platform });
      expect(child.exitCode).toBeNull(); expect(child.signalCode).toBeNull();
      expect(fs.existsSync(file)).toBe(true);
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(true);
      // Retain any unexpectedly successful acquisition so even a failing test releases its own fd.
      await expect(takeMainPublicationFence(meshRoot, rootId).then(unlock => { release = unlock; }))
        .rejects.toThrow(/fence|lock|sharing|busy|EACCES|EPERM/i);
      await expect(main.refresh()).rejects.toMatchObject({ code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(true);
      expect(child.exitCode).toBeNull(); expect(child.signalCode).toBeNull();
      expect(child.kill("SIGKILL")).toBe(true); // only our child; Windows uses forced termination
      const death = await within(exited, 10_000, () => `Fence child did not exit after termination: ${stderr}`);
      expect(death.code === 0).toBe(false);
      expect(child.exitCode !== null || child.signalCode !== null).toBe(true);
      // No graceful release/unlink: the record remains, but only the live kernel holder fenced Main.
      expect(fs.existsSync(file)).toBe(true);
      const audit = JSON.parse(fs.readFileSync(file, "utf8"));
      // Counterexample: a still-live PID and far-future timestamp must not replace the kernel lock.
      fs.writeFileSync(file, JSON.stringify({ ...audit, pid: process.pid, createdAt: "2999-01-01T00:00:00.000Z" }));
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(false);
      await expect(main.refresh()).resolves.toBeUndefined();
      release = await takeMainPublicationFence(meshRoot, rootId);
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(true);
      release(); release(); // idempotent close on all supported platforms
      release = undefined;
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(false);
      await expect(main.refresh()).resolves.toBeUndefined();
    } finally {
      try {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
        await within(exited, 10_000, () => `Fence child cleanup did not confirm exit: ${stderr}`);
      } finally { release?.(); await main.close(); }
      // Private fixture teardown only, after actual child exit; never a real actor/history tree.
      fs.rmSync(meshRoot, { recursive: true, force: true });
    }
  }, 45_000);

  it("fails closed on a native fence-path metadata error", () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-fence-error-"));
    const rootId = "session:native-fence-error", file = mainPublicationFencePath(meshRoot, rootId);
    const lstat = fs.lstatSync.bind(fs);
    try {
      const fault = vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
        if (String(args[0]) === file) throw Object.assign(new Error("fence metadata denied"), { code: "EACCES" });
        return lstat(...args);
      }) as typeof fs.lstatSync);
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(true);
      fault.mockRestore();
      expect(mainPublicationFenced(meshRoot, rootId)).toBe(false); // genuine ENOENT is not a fence
    } finally { vi.restoreAllMocks(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
  });

  it("refuses a non-regular native fence file without changing it", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-fence-directory-"));
    const rootId = "session:native-fence-directory", file = mainPublicationFencePath(meshRoot, rootId);
    let release: (() => void) | undefined;
    try {
      fs.mkdirSync(file, { recursive: true });
      await expect(takeMainPublicationFence(meshRoot, rootId).then(unlock => { release = unlock; })).rejects.toThrow();
      expect(fs.lstatSync(file).isDirectory()).toBe(true);
      expect(fs.readdirSync(file)).toEqual([]);
    } finally { release?.(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
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
