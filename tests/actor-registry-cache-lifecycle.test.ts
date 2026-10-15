import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { mainPublicationFencePath } from "../src/topology/main-publication-fence.js";

describe("ActorManager registry-cache lifecycle (#7791)", () => {
  const evictsOnClose = async (persistent: boolean, options: { emulateWindows?: boolean; recordOnly?: boolean } = {}) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cache-lifecycle-"));
    const actorRoot = path.join(root, "actors");
    fs.mkdirSync(actorRoot);
    const file = path.join(actorRoot, "actors.json");
    fs.writeFileSync(file, '{"format":1,"actors":[]}');
    // A racy generation would miss even without close-time eviction.
    const old = new Date(Date.now() - 5_000);
    fs.utimesSync(file, old, old);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const identity = { id: "session:cache-lifecycle", name: "main", kind: "main" as const, sessionId: "cache-lifecycle" };
    const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("cache-lifecycle", identity, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {}, {
      actorRoot, persistent, canConsumeMesh: () => false, reapDeadSessionPresence: false,
      meshRetentionSweepPath: false, closeGraceMs: 0,
    });
    const fenceFile = mainPublicationFencePath(mesh.root, identity.id);
    const alias = new ActorRegistryStore(path.join(actorRoot, "."));
    const stamp = fs.statSync(file, { bigint: true });
    const registryFds = new Set<number>();
    const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
    const hostPlatform = process.platform;
    let restorePlatform: (() => void) | undefined;
    let fenceOpened = false;
    const open = fs.openSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation(((name: fs.PathLike, flags: string | number, mode?: number) => {
      // The Windows flag is only emulated on a non-Windows host; the source still takes its portable branch.
      const hostFlags = hostPlatform !== "win32" && typeof flags === "number" && (flags & 0x10000000) !== 0
        ? flags & ~0x10000000 : flags;
      const fd = Reflect.apply(open, fs, [name, hostFlags, mode]);
      const resolved = path.resolve(String(name));
      if (resolved === path.resolve(file)) registryFds.add(fd);
      if (options.emulateWindows && resolved === path.resolve(fenceFile)) {
        fenceOpened = true;
        restorePlatform?.(); // Spoof only through the synchronous portable open; this is not native Windows proof.
      }
      return fd;
    }) as typeof fs.openSync);
    const close = fs.closeSync.bind(fs);
    vi.spyOn(fs, "closeSync").mockImplementation(((fd: number) => {
      registryFds.delete(fd);
      return Reflect.apply(close, fs, [fd]);
    }) as typeof fs.closeSync);
    const fstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, statOptions?: { bigint?: boolean }) =>
      statOptions?.bigint === true && registryFds.has(fd)
        ? stamp : Reflect.apply(fstat, fs, [fd, statOptions])) as typeof fs.fstatSync);
    const before = alias.read();
    const remove = fs.rmSync.bind(fs);
    let removed = false;
    vi.spyOn(fs, "rmSync").mockImplementation((name, rmOptions) => {
      if (String(name) === actorRoot) {
        removed = true;
        // The cache must already be released at the removal boundary, not just later in close.finally.
        expect(alias.read()).not.toBe(before);
        if (options.recordOnly) return; // #4664: the emulated probe records product removal, never performs it.
      }
      remove(name, rmOptions);
    });
    if (options.emulateWindows) {
      let active = true;
      Object.defineProperty(process, "platform", { ...platformDescriptor, value: "win32" });
      restorePlatform = () => {
        if (!active) return;
        active = false;
        Object.defineProperty(process, "platform", platformDescriptor);
      };
    }
    try {
      await actors.close();
      expect(removed).toBe(!persistent);
      if (!persistent && !options.recordOnly) {
        expect(fs.existsSync(actorRoot)).toBe(false);
        fs.mkdirSync(actorRoot);
        fs.writeFileSync(file, '{"format":1,"actors":[]}');
      }
      if (!persistent && options.recordOnly) {
        expect(fenceOpened).toBe(true); // The emulated Windows path reached the fence before the recorded remove.
        expect(fs.existsSync(actorRoot)).toBe(true);
        fs.writeFileSync(file, '{"format":1,"actors":[]}'); // Emulate the post-removal replacement without deleting.
      }
      const parse = vi.spyOn(JSON, "parse");
      expect(alias.read()).not.toBe(before); // Forced identical identity makes cache eviction observable.
      expect(parse).toHaveBeenCalledTimes(1);
      await actors.close(); // Idempotent close receipt.
    } finally {
      restorePlatform?.();
      vi.restoreAllMocks();
      await actors.close();
      await agents.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  };
  it.each([true])("evicts on close (persistent=%s), including before temporary-root removal", persistent => evictsOnClose(persistent));
  // The fence is supported on Linux, Darwin, and Windows; only genuinely unsupported platforms stay gated.
  it.skipIf(!["linux", "darwin", "win32"].includes(process.platform)).each([false])("evicts on close (persistent=%s), including before temporary-root removal", persistent => evictsOnClose(persistent));
  it("records an emulated Windows fence and cache eviction before root removal (not native Windows proof)", () =>
    evictsOnClose(false, { emulateWindows: true, recordOnly: true }));
});
