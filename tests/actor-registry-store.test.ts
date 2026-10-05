import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

const roots: string[] = [];
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-registry-test-"));
  roots.push(root);
  const actorRoot = path.join(root, "actors");
  const registryPath = path.join(actorRoot, "actors.json");
  const lockPath = `${registryPath}.lock`;
  return { store: new ActorRegistryStore(actorRoot), actorRoot, registryPath, lockPath };
};

const installLock = (lockPath: string, pid: number, createdAt: number) => {
  fs.mkdirSync(lockPath, { recursive: true });
  fs.writeFileSync(path.join(lockPath, "owner"), `previous\n${pid}\n${createdAt}\n`);
};

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("ActorRegistryStore", () => {
  it("round-trips format 1 records and fingerprints atomic replacements", async () => {
    const { store, lockPath } = setup();
    expect(store.fingerprint()).toBeUndefined();
    expect(store.records()).toEqual([]);
    const first = { id: "first", rootId: "remote", extra: { preserved: true } };
    await store.withLock(() => {
      expect(fs.existsSync(lockPath)).toBe(true);
      store.write([first]);
    });
    const before = store.fingerprint();
    expect(before).toBeTypeOf("string");
    await store.withLock(() => store.write([...store.records(), { id: "second" }]));
    expect(store.read()).toEqual({ format: 1, actors: [first, { id: "second" }] });
    expect(store.fingerprint()).not.toBe(before);
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it("#169 security S2 durably preserves a foreign pending decision through an ordinary replacement", async () => {
    const { store, actorRoot, registryPath } = setup();
    const pending = { id: "foreign", rootId: "remote", removal: { requestedAt: 1, runId: "pending" } };
    await store.withLock(() => store.write([{ id: "local" }]));
    await store.withLock(() => store.write([pending, { id: "local" }], { durable: true }));
    const descriptors = new Map<number, string>();
    const events: string[] = [];
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      return fd;
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const file = descriptors.get(fd)!;
      events.push(file.startsWith(`${registryPath}.`) ? "file" : file);
      sync(fd);
    });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === registryPath) events.push("rename");
      rename(from, to);
    });
    await store.withLock(() => store.write([pending, { id: "local", nice: 7 }], { durable: false }));
    expect(events.slice(0, 2)).toEqual(["file", "rename"]);
    if (process.platform !== "win32") {
      expect(events[2]).toBe(actorRoot);
      expect(events.at(-1)).toBe(path.parse(actorRoot).root);
    }
    expect(store.records()).toEqual([pending, { id: "local", nice: 7 }]);
  });

  it.each(process.platform === "win32" ? ["file"] : ["file", "directory"])("#169 security S2 durably rolls back an accepted decision after a failed %s barrier", async (barrier) => {
    const { store, actorRoot, registryPath } = setup();
    const pending = { id: "pending", removal: { requestedAt: 1 } };
    await store.withLock(() => store.write([{ id: "pending" }]));
    await store.withLock(() => store.write([pending], { durable: true }));
    const descriptors = new Map<number, string>();
    const events: string[] = [];
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    const rename = fs.renameSync.bind(fs);
    let fail = true;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      return fd;
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const file = descriptors.get(fd)!;
      const event = file.startsWith(`${registryPath}.`) ? "file" : file;
      if (fail && event === (barrier === "file" ? "file" : actorRoot)) {
        fail = false;
        events.push("failed");
        throw new Error("replacement barrier unavailable");
      }
      events.push(event);
      sync(fd);
    });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === registryPath) events.push("rename");
      rename(from, to);
    });
    await expect(store.withLock(() => store.write([pending, { id: "new" }]))).rejects.toThrow("replacement barrier unavailable");
    expect(store.records()).toEqual([pending]);
    const rollback = events.slice(events.indexOf("failed") + 1);
    expect(rollback.slice(0, 2)).toEqual(["file", "rename"]);
    if (process.platform !== "win32") {
      expect(rollback[2]).toBe(actorRoot);
      expect(rollback.at(-1)).toBe(path.parse(actorRoot).root);
    }
  });

  it.skipIf(process.platform === "win32")("does not skip a durable registry write after an ancestor symlink replacement", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-registry-link-"));
    roots.push(root);
    const physical = path.join(root, "physical"), alias = path.join(root, "alias");
    const actorRoot = path.join(alias, "actors"), physicalActorRoot = path.join(physical, "actors");
    fs.mkdirSync(physical, { recursive: true });
    fs.symlinkSync(physical, alias, "dir");
    const store = new ActorRegistryStore(actorRoot);
    const actor = { id: "same", rootId: "owner" };
    store.write([actor], { durable: true });

    // Rebind the ancestor to the same physical directory. The leaf inode/stamp and
    // bytes are unchanged, but the namespace link still needs a fresh durable receipt.
    fs.unlinkSync(alias);
    fs.symlinkSync(physical, alias, "dir");
    const descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    let parentBarrierAttempted = false;
    let fail = true;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      return fd;
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const file = descriptors.get(fd);
      if (file && fs.realpathSync(file) === physicalActorRoot) {
        parentBarrierAttempted = true;
        if (fail) {
          fail = false;
          throw new Error("ancestor parent barrier unavailable");
        }
      }
      sync(fd);
    });
    expect(() => store.write([actor], { durable: true })).toThrow("ancestor parent barrier unavailable");
    expect(parentBarrierAttempted).toBe(true);
  });

  it("preserves unknown record fields and filters only invalid record identities", () => {
    const { store, actorRoot, registryPath } = setup();
    fs.mkdirSync(actorRoot);
    for (const raw of ["bad json", "null", "[]", '{"actors":{}}']) {
      fs.writeFileSync(registryPath, raw);
      expect(store.records()).toEqual([]);
    }
    const record = { id: "", futureField: [1, 2] };
    fs.writeFileSync(registryPath, JSON.stringify({ actors: [null, [], 1, {}, { id: 1 }, record] }));
    expect(store.records()).toEqual([record]);
  });

  it("strict custody distinguishes an absent registry from unreadable or malformed evidence", () => {
    const { store, actorRoot, registryPath } = setup();
    expect(store.records({ strict: true })).toEqual([]);
    fs.mkdirSync(actorRoot);
    for (const raw of ["bad json", "null", "[]", '{"actors":{}}', '{"actors":[{"id":"actor"}]}',
      '{"actors":[{"id":"actor","rootId":"root","ownershipToken":7}]}']) {
      fs.writeFileSync(registryPath, raw);
      expect(() => store.records({ strict: true })).toThrow();
    }
    const row = { id: "actor", rootId: "root", ownershipToken: "generation", future: true };
    fs.writeFileSync(registryPath, JSON.stringify({ format: 1, actors: [row] }));
    expect(store.records({ strict: true })).toEqual([row]);
    const read = fs.readFileSync.bind(fs);
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === registryPath) throw Object.assign(new Error("unreadable"), { code: "EACCES" });
      return (read as (...args: unknown[]) => unknown)(file, ...args);
    }) as typeof fs.readFileSync);
    expect(() => store.records({ strict: true })).toThrow("unreadable");
  });

  it("releases its lock on a throwing callback and propagates the original error", async () => {
    const { store, lockPath } = setup();
    const error = new Error("write failed");
    await expect(store.withLock(() => { throw error; })).rejects.toBe(error);
    expect(fs.existsSync(lockPath)).toBe(false);
    await expect(store.withLock(() => 42)).resolves.toBe(42);
  });

  it("never releases a replacement owner's lock", async () => {
    const { store, lockPath } = setup();
    await store.withLock(() => {
      fs.writeFileSync(path.join(lockPath, "owner"), "replacement\n1\n0\n");
    });
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toBe("replacement\n1\n0\n");
  });

  it("recovers a stale lock only when its owning process is gone", async () => {
    const { store, lockPath } = setup();
    installLock(lockPath, 123456, Date.now() - 30_001);
    vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("gone"), { code: "ESRCH" }); });
    await expect(store.withLock(() => "recovered")).resolves.toBe("recovered");
    expect(process.kill).toHaveBeenCalledWith(123456, 0);
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it.each([false, true])("recovers a fresh dead holder (incarnation=%s) without waiting for lock age", async (incarnation) => {
    const { store, actorRoot, lockPath } = setup();
    installLock(lockPath, 123456, Date.now());
    if (incarnation) fs.appendFileSync(path.join(lockPath, "owner"), "123\n");
    vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("gone"), { code: "ESRCH" }); });
    await expect(store.withLock(() => "recovered")).resolves.toBe("recovered");
    expect(fs.existsSync(lockPath)).toBe(false);
    const fences = fs.readdirSync(actorRoot).filter(name => name.startsWith("actors.json.lock.dead."));
    expect(fences).toHaveLength(1);
    expect(fs.readFileSync(path.join(actorRoot, fences[0]!, "owner"), "utf8")).toContain("123456\n");
  });

  it.skipIf(process.platform !== "linux")("reclaims a fresh lock only after proving a different PID incarnation", async () => {
    const { store, lockPath } = setup();
    installLock(lockPath, process.pid, Date.now());
    fs.appendFileSync(path.join(lockPath, "owner"), "0\n");
    await expect(store.withLock(() => "recovered")).resolves.toBe("recovered");
  });

  it.each(["EPERM", "EIO", "torn identity", "unreadable incarnation", "malformed owner"])("protects a live or unknown holder with %s", async (failure) => {
    vi.useFakeTimers();
    const { store, lockPath } = setup();
    installLock(lockPath, process.pid, Date.now() - 30_001);
    if (failure === "EPERM" || failure === "EIO") {
      vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error(failure), { code: failure }); });
    } else if (failure === "torn identity") {
      fs.appendFileSync(path.join(lockPath, "owner"), "0");
    } else if (failure === "unreadable incarnation") {
      fs.appendFileSync(path.join(lockPath, "owner"), "0\n");
      const read = fs.readFileSync.bind(fs);
      vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
        if (String(file) === `/proc/${process.pid}/stat`) throw Object.assign(new Error("unreadable"), { code: "EIO" });
        return read(file, options);
      });
    } else fs.writeFileSync(path.join(lockPath, "owner"), `holder\n${process.pid}\ninvalid\n0\n`);
    const operation = vi.fn();
    const result = expect(store.withLock(operation)).rejects.toThrow("Timed out waiting for the Fabric actor registry lock");
    await vi.advanceTimersByTimeAsync(5_000);
    await result;
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lockPath)).toBe(true);
  });

  it("a paused dead-holder reaper cannot rename over a successor", async () => {
    vi.useFakeTimers();
    const { store, lockPath } = setup();
    installLock(lockPath, 123456, Date.now());
    const kill = process.kill.bind(process);
    vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
      if (pid === 123456) throw Object.assign(new Error("gone"), { code: "ESRCH" });
      return kill(pid, signal);
    });
    const rename = fs.renameSync.bind(fs);
    let fence: string | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === lockPath && !fence) {
        fence = String(to);
        // A faster reaper moved the old identity; a live writer owns the canonical name.
        rename(from, to);
        installLock(lockPath, process.pid, Date.now());
      }
      rename(from, to); // Must fail: the retained fence is nonempty.
    });
    const operation = vi.fn(() => "acquired");
    const pending = store.withLock(operation);
    await vi.advanceTimersByTimeAsync(20);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(lockPath, "owner"), "utf8")).toContain(`\n${process.pid}\n`);
    expect(fs.existsSync(fence!)).toBe(true);
    fs.rmSync(lockPath, { recursive: true });
    await vi.advanceTimersByTimeAsync(10);
    await expect(pending).resolves.toBe("acquired");
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("waits for a live owner without stealing its stale lock", async () => {
    vi.useFakeTimers();
    const { store, lockPath } = setup();
    installLock(lockPath, process.pid, Date.now() - 30_001);
    const operation = vi.fn(() => "acquired");
    const pending = store.withLock(operation);
    await vi.advanceTimersByTimeAsync(20);
    expect(operation).not.toHaveBeenCalled();
    fs.rmSync(lockPath, { recursive: true });
    await vi.advanceTimersByTimeAsync(10);
    await expect(pending).resolves.toBe("acquired");
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("times out on incomplete locks without running the callback", async () => {
    vi.useFakeTimers();
    const { store, lockPath } = setup();
    fs.mkdirSync(lockPath, { recursive: true });
    const operation = vi.fn();
    const result = expect(store.withLock(operation)).rejects.toThrow(
      "Timed out waiting for the Fabric actor registry lock",
    );
    await vi.advanceTimersByTimeAsync(5_000);
    await result;
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lockPath)).toBe(true);
  });
});
