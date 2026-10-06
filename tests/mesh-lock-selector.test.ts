import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig, normalizeFabricConfig, saveFabricConfig } from "../src/config.js";
import { MeshStore, type MeshStoreOptions } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { main, runBridge } from "../src/mesh-bridge.js";

const roots: string[] = [];
const root = (): string => {
  const value = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-selector-"));
  roots.push(value);
  return value;
};
const store = (options: MeshStoreOptions = {}): MeshStore => new MeshStore(root(), 65536, 100, options);
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true });
});

describe("startup mesh lock selector", () => {
  it.each([undefined, 1] as const)("%s keeps the B68 v1 wire with exclusive publication and detached release", async (lockProtocol) => {
    const mesh = store(lockProtocol === undefined ? {} : { lockProtocol });
    const lock = path.join(mesh.root, ".lock");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const rename = vi.spyOn(fs, "renameSync");
    const remove = vi.spyOn(fs, "rmSync");
    const write = vi.spyOn(fs, "writeFileSync");
    let token = "";
    await mesh.exclusive(() => {
      const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
      expect(owner).toMatch(new RegExp(`^[^\\n]+\\n${process.pid}\\n[0-9]+\\n$`));
      expect(mkdir).toHaveBeenCalledWith(lock, { mode: 0o700 });
      token = owner.split("\n")[0]!;
      expect(write).toHaveBeenCalledWith(path.join(lock, "owner"), owner, {
        encoding: "utf8", flag: "wx", mode: 0o600,
      });
    });
    expect(mesh.lockProtocol).toBe(1);
    const released = `${lock}.released.${token}`;
    expect(rename).toHaveBeenCalledWith(lock, released);
    expect(remove).toHaveBeenCalledWith(released, { recursive: true, force: true });
    expect(remove).not.toHaveBeenCalledWith(lock, { recursive: true, force: true });
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("new v1 cannot overwrite a fresh empty canonical created by B68 between attempts", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const mesh = store({ lockTimeoutMs: 100 });
    const lock = path.join(mesh.root, ".lock");
    const nativeMkdir = fs.mkdirSync.bind(fs);
    let inserted = false;
    let inode = 0;
    vi.spyOn(fs, "mkdirSync").mockImplementation((file, options) => {
      if (!inserted && String(file) === lock) {
        inserted = true;
        nativeMkdir(lock, { mode: 0o700 }); // actual B68 creator's first instruction
        inode = fs.statSync(lock).ino;
      }
      return nativeMkdir(file, options);
    });
    const operation = vi.fn();
    const pending = mesh.exclusive(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await pending).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(operation).not.toHaveBeenCalled();
    expect(fs.statSync(lock).ino).toBe(inode);
    expect(fs.readdirSync(lock)).toEqual([]);
  });

  it.each([undefined, 1, 2] as const)("protocol %s release fences on the entire complete owner record", async (lockProtocol) => {
    const mesh = store(lockProtocol === undefined ? {} : { lockProtocol });
    const lock = path.join(mesh.root, ".lock");
    let changed = "";
    await mesh.exclusive(() => {
      const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
      changed = owner.split("\n")[0] + "\nchanged\n";
      fs.writeFileSync(path.join(lock, "owner"), changed);
    });
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(changed);
  });

  it.each([1, 2] as const)("protocol %s immediately recovers dead holders and retains typed bounded backoff", async (lockProtocol) => {
    vi.useFakeTimers({ now: 1_000_000 });
    const mesh = store({ lockProtocol, lockTimeoutMs: 100 });
    const lock = path.join(mesh.root, ".lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `dead\n999999999\n${Date.now()}\n`);
    const timers = vi.spyOn(globalThis, "setTimeout");
    await expect(mesh.exclusive(() => "recovered")).resolves.toBe("recovered");
    expect(timers).not.toHaveBeenCalled();
    expect(fs.readdirSync(mesh.root).filter((name) => name.startsWith(".lock.dead."))).toHaveLength(1);
    fs.mkdirSync(lock);
    const live = `live\n${process.pid}\n${Date.now()}\n`;
    fs.writeFileSync(path.join(lock, "owner"), live);
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    const pending = mesh.exclusive(() => { throw new Error("must not enter"); }).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    expect(await pending).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    expect(timers.mock.calls.map(([, wait]) => wait)).toEqual([...Array(8).fill(10), 19, 1]);
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(live);
  });

  it("normalizes, persists and captures selection once, including the control-seen constructor", async () => {
    expect(DEFAULT_FABRIC_CONFIG.mesh.lockProtocol).toBe(1);
    expect(normalizeFabricConfig({}).mesh.lockProtocol).toBe(1);
    const cwd = root();
    const agentDir = root();
    const location = { cwd, agentDir, projectTrusted: true };
    expect(loadFabricConfig(location).mesh.lockProtocol).toBe(1);
    saveFabricConfig({ ...location, scope: "global" }, { mesh: { lockProtocol: 1 } });
    expect(loadFabricConfig(location).mesh.lockProtocol).toBe(1);
    expect(JSON.parse(fs.readFileSync(path.join(agentDir, "fabric.json"), "utf8")).mesh.lockProtocol).toBe(1);
    saveFabricConfig({ ...location, scope: "global" }, { mesh: { lockProtocol: 2 } });
    const config = loadFabricConfig(location);
    expect(config.mesh.lockProtocol).toBe(2);
    expect(JSON.parse(fs.readFileSync(path.join(agentDir, "fabric.json"), "utf8")).mesh.lockProtocol).toBe(2);
    const mesh = store(config.mesh);
    config.mesh.lockProtocol = 1;
    saveFabricConfig({ ...location, scope: "global" }, { mesh: { lockProtocol: 1 } });
    expect(loadFabricConfig(location).mesh.lockProtocol).toBe(1);
    expect(mesh.lockProtocol).toBe(2);
    const stagedWrite = vi.spyOn(fs, "writeFileSync");
    await mesh.exclusive(() => "unchanged protocol");
    expect(stagedWrite.mock.calls.some(([file]) => String(file).includes(".lock.pending."))).toBe(true);
    const protocolReads = vi.spyOn(mesh, "lockProtocol", "get");
    const plane = new FabricControlPlane(mesh, { id: "test", name: "test", kind: "main" }, { enabled: false, hostId: "test" });
    expect(protocolReads).toHaveBeenCalledOnce();
    await plane.close();
  });

  it.each([0, 3, null, "2", false, 1.5])("rejects unsupported selection %s in config and store construction", (invalid) => {
    expect(() => normalizeFabricConfig({ mesh: { lockProtocol: invalid } })).toThrow("mesh.lockProtocol must be 1 or 2");
    expect(() => store({ lockProtocol: invalid as 1 })).toThrow("mesh.lockProtocol must be 1 or 2");
  });

  it("rejects invalid persisted selection", () => {
    const cwd = root();
    const agentDir = root();
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ mesh: { lockProtocol: 3 } }));
    expect(() => loadFabricConfig({ cwd, agentDir, projectTrusted: true })).toThrow("mesh.lockProtocol must be 1 or 2");
  });

  it.each(["run", "agent"])("standalone %s rejects invalid protocol before any transport or service", async (mode) => {
    const flags = ["--mesh", root(), "--lock-protocol", "3"];
    const args = mode === "agent" ? ["--peer", "remote"] : ["--name", "local", "--remote", "remote", "--cursor", path.join(root(), "cursor")];
    const beforeTerm = process.listeners("SIGTERM");
    const beforeInt = process.listeners("SIGINT");
    try {
      await expect(main([mode, ...flags, ...args])).rejects.toThrow("--lock-protocol must be 1 or 2");
    } finally {
      for (const listener of process.listeners("SIGTERM")) if (!beforeTerm.includes(listener)) process.removeListener("SIGTERM", listener);
      for (const listener of process.listeners("SIGINT")) if (!beforeInt.includes(listener)) process.removeListener("SIGINT", listener);
    }
  });

  it.each([undefined, "1", "2"])("standalone run accepts startup selection %s without launching a transport", async (selected) => {
    const flags = new Map([["mesh", root()], ["name", "local"], ["remote", "remote"], ["cursor", path.join(root(), "cursor")]]);
    if (selected !== undefined) flags.set("lock-protocol", selected);
    const controller = new AbortController();
    controller.abort();
    await expect(runBridge(flags, ["unused-transport"], controller.signal)).resolves.toBe(0);
  });
});
