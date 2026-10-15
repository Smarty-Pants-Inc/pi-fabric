import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorRegistryStore } from "../src/actors/registry-store.js";

// #4664: product removal is always recorded and emulated by a private rename.
// No product rm/unlink/rmdir implementation is called, even for fixture paths.
// Only afterEach, after restoring spies, removes this test's own fixture roots.
const roots: string[] = [];
const setup = (start: string | null = "4242") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "owner-label-"));
  roots.push(root);
  const discarded = path.join(root, "discarded");
  fs.mkdirSync(discarded);
  const removals: string[] = [];
  const emulateRemoval = (file: fs.PathLike, force = false): void => {
    const name = path.resolve(String(file));
    if (!roots.some(owned => name.startsWith(`${owned}${path.sep}`))) {
      throw new Error(`Refused unexpected product deletion: ${name}`);
    }
    removals.push(name);
    try { fs.renameSync(name, path.join(discarded, String(removals.length))); }
    catch (error) { if (!force || (error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  };
  vi.spyOn(fs, "rmSync").mockImplementation((file, options) => emulateRemoval(file, options?.force));
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => emulateRemoval(file));
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => emulateRemoval(file));
  // These paths are not used by this synchronous protocol. Fail closed if that changes.
  for (const method of ["rm", "unlink", "rmdir"] as const) {
    vi.spyOn(fs, method).mockImplementation(() => { throw new Error(`Unexpected async product ${method}`); });
    vi.spyOn(fs.promises, method).mockImplementation(async () => { throw new Error(`Unexpected product promises.${method}`); });
  }
  const read = fs.readFileSync.bind(fs);
  const procReads: string[] = [];
  vi.spyOn(fs, "readFileSync").mockImplementation((file, options) => {
    const name = String(file);
    if (/^\/proc\/\d+\/stat$/.test(name)) {
      procReads.push(name);
      if (start === null) throw Object.assign(new Error("no incarnation"), { code: "ENOENT" });
      return `1 (fixture process) ${[...Array(19).fill("0"), start].join(" ")}\n`;
    }
    return read(file, options);
  });
  const actorRoot = path.join(root, "actors");
  const registryPath = path.join(actorRoot, "actors.json");
  const lockPath = `${registryPath}.lock`;
  const ownerPath = path.join(lockPath, "owner");
  const store = new ActorRegistryStore(actorRoot);
  return { root, actorRoot, registryPath, lockPath, ownerPath, store, removals, procReads };
};

const ownerWrites = () => {
  const write = fs.writeFileSync.bind(fs);
  const writes: Array<{ file: string; bytes: string }> = [];
  vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
    if (typeof file !== "number" && String(file).endsWith(path.join("actors.json.lock", "owner"))) {
      writes.push({ file: String(file), bytes: String(data) });
    }
    return write(file, data, options);
  });
  return writes;
};

const installOwner = (lockPath: string, bytes: string) => {
  fs.mkdirSync(lockPath, { recursive: true });
  fs.writeFileSync(path.join(lockPath, "owner"), bytes);
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("actor registry owner attribution", () => {
  it("appends field5 without changing identity slots or the existing single owner write", async () => {
    const f = setup();
    const writes = ownerWrites();
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const sync = vi.spyOn(fs, "fsyncSync");
    const open = vi.spyOn(fs, "openSync");
    const kill = vi.spyOn(process, "kill");
    const acquiredAt = Date.now();
    await expect(f.store.withLock(() => {
      const fields = fs.readFileSync(f.ownerPath, "utf8").split("\n");
      expect(fields).toHaveLength(6);
      expect(fields[0]).toMatch(/^[a-f0-9-]{36}$/);
      expect(fields[1]).toBe(String(process.pid));
      expect(Number(fields[2])).toBeGreaterThanOrEqual(acquiredAt);
      expect(fields[3]).toBe(process.platform === "linux" ? "4242" : "");
      expect(fields[4]).toBe("presence-fence");
      expect(fields[5]).toBe("");
      return 42;
    }, undefined, "presence-fence")).resolves.toBe(42);
    expect(writes).toHaveLength(1);
    expect(fs.writeFileSync).toHaveBeenCalledTimes(1);
    expect(mkdir).toHaveBeenCalledTimes(2); // actor root, then canonical lock
    expect(sync).not.toHaveBeenCalled();
    expect(open).not.toHaveBeenCalled();
    expect(kill).not.toHaveBeenCalled();
    expect(f.procReads).toHaveLength(process.platform === "linux" ? 1 : 0); // constructor only
    expect(f.removals).toEqual([f.lockPath]);
    expect(fs.existsSync(f.lockPath)).toBe(false);
  });

  it("keeps an empty fourth slot when the kernel start is unavailable", async () => {
    const f = setup(null);
    const writes = ownerWrites();
    await f.store.withLock(() => undefined, undefined, "no-start");
    expect(writes).toHaveLength(1);
    expect(writes[0]!.bytes.split("\n").slice(3)).toEqual(["", "no-start", ""]);
  });

  it("preserves old unlabeled withLock and withLocks callers with a compact external default", async () => {
    const f = setup();
    const writes = ownerWrites();
    await f.store.withLock(() => 1);
    await f.store.withLock(() => 2, 100);
    await ActorRegistryStore.withLocks([f.store], () => 3);
    expect(writes).toHaveLength(3);
    expect(writes.map(write => write.bytes.split("\n")[4])).toEqual(["external", "external", "external"]);
  });

  it("forwards the same label through sorted deduplicated static recursion", async () => {
    const f = setup();
    const bRoot = path.join(f.root, "b");
    const b = new ActorRegistryStore(bRoot);
    const alias = new ActorRegistryStore(path.join(f.actorRoot, "."));
    const writes = ownerWrites();
    await expect(ActorRegistryStore.withLocks([b, f.store, alias, b], async () => {
      await Promise.resolve();
      expect(fs.readFileSync(f.ownerPath, "utf8").split("\n")[4]).toBe("multi-owner");
      expect(fs.readFileSync(path.join(bRoot, "actors.json.lock", "owner"), "utf8").split("\n")[4]).toBe("multi-owner");
      return "retained";
    }, "multi-owner")).resolves.toBe("retained");
    expect(writes.map(write => write.file)).toEqual([f.ownerPath, path.join(bRoot, "actors.json.lock", "owner")]);
    expect(writes.map(write => write.bytes.split("\n")[4])).toEqual(["multi-owner", "multi-owner"]);
  });

  it("labels default updates and preserves custom attribution on optimistic fallback", async () => {
    const f = setup();
    const writes = ownerWrites();
    await expect(f.store.update(() => ({ actors: [], value: "default" }))).resolves.toBe("default");
    let selections = 0;
    await expect(f.store.update(() => {
      const attempt = ++selections;
      return { actors: [], validate: () => attempt > 1, value: "fallback" };
    }, "manager-update")).resolves.toBe("fallback");
    expect(selections).toBe(2);
    expect(writes.map(write => write.bytes.split("\n")[4])).toEqual(["update", "manager-update", "manager-update"]);
    expect(f.removals).toContain(f.lockPath);
  });

  it.skipIf(process.platform === "win32")("labels the existing deferred barrier retry acquisition", async () => {
    const f = setup();
    const writes = ownerWrites();
    const prepared = f.store.prepare([], { durable: true });
    try {
      await f.store.withLock(() => prepared.commit({ deferBarrier: true }));
      const sync = fs.fsyncSync.bind(fs);
      let fail = true;
      vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        if (fail) { fail = false; throw new Error("retry directory barrier"); }
        sync(fd);
      });
      await prepared.barrier();
      expect(writes.map(write => write.bytes.split("\n")[4])).toEqual(["external", "barrier"]);
    } finally { prepared.dispose(); }
  });

  it("labels downgrade without adding another owner write", async () => {
    const f = setup();
    fs.mkdirSync(f.actorRoot);
    fs.writeFileSync(f.registryPath, '{"format":1,"actors":[]}');
    const writes = ownerWrites();
    await expect(f.store.restoreInlineForDowngrade()).resolves.toBe(0);
    expect(writes).toHaveLength(1);
    expect(writes[0]!.bytes.split("\n")[4]).toBe("downgrade");
  });

  it.each(["", "x".repeat(65), "\n", "trailing\n", "bad\nlabel", "bad\rlabel", "bad\tlabel", "bad\0label", "bad\x7flabel", "bad\x1blabel", "bad\u2028label", "bad\u2029label", null, 7])(
    "rejects invalid public labels before I/O or callbacks: %j", async label => {
      const f = setup();
      const mkdir = vi.spyOn(fs, "mkdirSync");
      const writes = ownerWrites();
      vi.mocked(fs.readFileSync).mockClear();
      const operation = vi.fn(() => 1);
      const select = vi.fn(() => undefined);
      const invalid = label as unknown as string;
      await expect(f.store.withLock(operation, undefined, invalid)).rejects.toThrow(/label/i);
      await expect(Promise.resolve().then(() => ActorRegistryStore.withLocks([f.store], operation, invalid))).rejects.toThrow(/label/i);
      await expect(Promise.resolve().then(() => ActorRegistryStore.withLocks([], operation, invalid))).rejects.toThrow(/label/i);
      await expect(f.store.update(select, invalid)).rejects.toThrow(/label/i);
      expect(mkdir).not.toHaveBeenCalled();
      expect(fs.readFileSync).not.toHaveBeenCalled();
      expect(writes).toEqual([]);
      expect(operation).not.toHaveBeenCalled();
      expect(select).not.toHaveBeenCalled();
    },
  );

  it("accepts the 64-character plain label bound", async () => {
    const f = setup();
    const writes = ownerWrites();
    const label = "x".repeat(64);
    await f.store.withLock(() => undefined, undefined, label);
    expect(writes[0]!.bytes.split("\n")[4]).toBe(label);
  });

  it.each([
    { name: "old3 dead", tail: "", alive: false, recovered: true },
    { name: "old4 dead", tail: "4242\n", alive: false, recovered: true },
    { name: "old3 live", tail: "", alive: true, recovered: false },
    { name: "old4 live incarnation", tail: "4242\n", alive: true, recovered: false },
    { name: "old4 reused PID", tail: "4241\n", alive: true, recovered: true, linux: true },
    { name: "old4 torn incarnation", tail: "4241", alive: true, recovered: false },
    { name: "old4 invalid incarnation", tail: "invalid\n", alive: true, recovered: false },
    { name: "old4 unreadable incarnation", tail: "4241\n", alive: true, recovered: false, unreadable: true },
    { name: "old4 denied liveness", tail: "4242\n", alive: true, recovered: false, denied: true },
    { name: "new5 live incarnation", tail: "4242\nmanager-update\n", alive: true, recovered: false },
    { name: "new5 reused PID", tail: "4241\nmanager-update\n", alive: true, recovered: true, linux: true },
    { name: "new5 dead", tail: "4242\nmanager-update\n", alive: false, recovered: true },
    { name: "new5 empty start and numeric label", tail: "\n0\n", alive: true, recovered: false },
  ])("preserves holder interpretation and identity-bound recovery: $name", async scenario => {
    if (scenario.linux && process.platform !== "linux") return;
    const f = setup();
    const bytes = `previous\n123456\n${Date.now() - 30_001}\n${scenario.tail}`;
    installOwner(f.lockPath, bytes);
    const before = fs.lstatSync(f.lockPath);
    vi.spyOn(process, "kill").mockImplementation(() => {
      if (!scenario.alive || scenario.denied) throw Object.assign(new Error("probe"), { code: scenario.denied ? "EPERM" : "ESRCH" });
      return true;
    });
    if (scenario.unreadable) {
      const read = vi.mocked(fs.readFileSync).getMockImplementation()!;
      vi.mocked(fs.readFileSync).mockImplementation((file, options) => {
        if (String(file) === "/proc/123456/stat") throw Object.assign(new Error("unknown incarnation"), { code: "EIO" });
        return read(file, options);
      });
    }
    const writes = ownerWrites();
    const operation = vi.fn(() => "acquired");
    if (scenario.recovered) {
      await expect(f.store.withLock(operation, 0, "recovery")).resolves.toBe("acquired");
      expect(operation).toHaveBeenCalledTimes(1);
      expect(writes).toHaveLength(1);
      expect(writes[0]!.bytes.split("\n")[4]).toBe("recovery");
      const digest = createHash("sha256").update(`${before.dev}:${before.ino}:${bytes}`).digest("hex");
      const fence = `${f.lockPath}.dead.${digest}`;
      expect(fs.readFileSync(path.join(fence, "owner"), "utf8")).toBe(bytes);
      expect(fs.lstatSync(fence).ino).toBe(before.ino);
      expect(f.removals).toEqual([f.lockPath]); // release only; recovery retains the old fence
    } else {
      await expect(f.store.withLock(operation, 0, "waiter")).rejects.toThrow("Timed out waiting for the Fabric actor registry lock");
      expect(operation).not.toHaveBeenCalled();
      expect(writes).toEqual([]);
      expect(f.removals).toEqual([]);
      expect(fs.readFileSync(f.ownerPath, "utf8")).toBe(bytes);
      expect(fs.lstatSync(f.lockPath).ino).toBe(before.ino);
      if (scenario.tail === "" || scenario.tail === "\n0\n" || scenario.tail === "4241") {
        expect(f.procReads).not.toContain("/proc/123456/stat");
      }
    }
    expect(process.kill).toHaveBeenCalledWith(123456, 0);
  });

  it.each([
    ["src/actors/manager.ts", /\}, "manager-update"\);/],
    ["src/actors/manager.ts", /\}\), 0\), "manager-adoption"\);/],
    ["src/actors/session-orphans.ts", /\}\)\), 0\), "session-orphans-reconcile"\);/],
    ["src/actors/session-orphans.ts", /\}, "session-orphans-ack"\);/],
    ["src/actors/remove-offline.ts", /\}, undefined, "remove-offline"\);/],
    ["src/storage/retention-cli.ts", /\.withLock\(\(\) => prune\(rootId, veto\), undefined, "retention"\)/],
  ])("mechanically inventories the granted call-site label in %s", (file, pattern) => {
    const repo = fileURLToPath(new URL("../", import.meta.url));
    expect((pattern as RegExp).test(fs.readFileSync(path.join(repo, file as string), "utf8"))).toBe(true);
  });
});
