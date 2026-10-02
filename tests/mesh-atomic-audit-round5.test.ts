import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { DurableDirectory } from "../src/core/atomic-write.js";

const roots: string[] = [];
const identity = { id: "session:round5", name: "main", kind: "main" as const };
const root = () => { const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-atomic-")); roots.push(directory); return directory; };
const tick = () => new Promise<void>(resolve => setTimeout(resolve, 5));
const until = async (condition: () => boolean) => { for (let n = 0; !condition(); n++) { if (n > 1000) throw new Error("mesh probe timed out"); await tick(); } };
afterEach(() => { vi.restoreAllMocks(); for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true }); });
const descriptors = () => {
  const files = new Map<number, string>(), open = fs.openSync.bind(fs);
  vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); files.set(fd, String(file)); return fd; });
  return files;
};

const holdLock = (lock: string) => {
  fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, "owner"), `round5-probe\n${process.pid}\n${Date.now()}\n`);
  return () => fs.rmSync(lock, { recursive: true, force: true });
};
describe("#2479 M round-five regressions", () => {
  it.skipIf(process.platform === "win32").each([
    ["delete", false], ["delete", true], ["batch", false], ["batch", true],
  ] as const)("F3 reconfirms cached completion after same-inode ABA during %s lock wait (failure: %s)", async (kind, fail) => {
    const parent = root(), directory = path.join(parent, "mesh"), mesh = new MeshStore(directory, 65536, 100);
    const entry = await mesh.put({ key: "test/key", value: 1, identity });
    const completion = fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8");
    const release = holdLock(path.join(directory, ".lock"));
    let attempts = 0, changed = false, reconfirmed = 0;
    const mkdir = fs.mkdirSync.bind(fs), sync = fs.fsyncSync.bind(fs), files = descriptors();
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: fs.MakeDirectoryOptions | fs.Mode) => {
      if (String(file) === path.join(directory, ".lock")) attempts++;
      return mkdir(file, options as fs.MakeDirectoryOptions & { recursive: true });
    }) as typeof fs.mkdirSync);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (changed && files.get(fd) === parent) {
        reconfirmed++;
        expect(fs.existsSync(path.join(directory, ".lock", "owner"))).toBe(false);
        if (fail) throw new Error("cached completion namespace reconfirmation unavailable");
      }
      sync(fd);
    });
    const fileBarrier = vi.spyOn(fs, "fsync");
    const receipt = kind === "delete" ? mesh.delete({ key: "test/absent" }) : mesh.writeBatch({ identity,
      ops: [{ kind: "put", key: "test/key", value: 2, ifVersion: 999, onConflict: "skip" }],
    });
    const settled = receipt.then(value => ({ value }), error => ({ error }));
    try {
      await until(() => attempts > 0);
      const before = fs.statSync(directory), ancestor = fs.statSync(parent);
      await tick();
      fs.renameSync(directory, path.join(parent, "away")); fs.renameSync(path.join(parent, "away"), directory);
      expect([fs.statSync(directory).dev, fs.statSync(directory).ino]).toEqual([before.dev, before.ino]);
      expect([fs.statSync(parent).dev, fs.statSync(parent).ino]).toEqual([ancestor.dev, ancestor.ino]);
      expect(fs.statSync(parent).ctimeMs).not.toBe(ancestor.ctimeMs);
      changed = true;
    } finally { release(); }
    const result = await settled;
    expect(reconfirmed).toBeGreaterThan(0);
    expect(fileBarrier).not.toHaveBeenCalled(); // Exercise completion reuse, not a new file barrier.
    if (fail) expect(result).toMatchObject({ error: expect.objectContaining({ message: "cached completion namespace reconfirmation unavailable" }) });
    else expect(result).toEqual({ value: kind === "delete" ? { deleted: false } : [{ key: "test/key", applied: false, version: entry.version }] });
    expect(fs.readFileSync(path.join(directory, "state.durability-completion.json"), "utf8")).toBe(completion);
    vi.restoreAllMocks();
    expect(await mesh.delete({ key: "test/absent" })).toEqual({ deleted: false });
  });

  it.each(["size", "io"] as const)("F11 refuses %s read fallback while another store's live mutation awaits checkpoint pinning", async kind => {
    const directory = root(), cap = kind === "size" ? 2048 : 8192;
    const small = new MeshStore(directory, 256, 100, { maxStateBytes: cap });
    const large = new MeshStore(directory, 8192, 100, { maxStateBytes: 65536 });
    const initial = await small.put({ key: "test/initial", value: "checkpoint", identity });
    const checkpoint = fs.readFileSync(path.join(directory, "state.durable.json"), "utf8");
    const release = holdLock(path.join(directory, ".state-durability-lock"));
    let refuseRead = false;
    const stat = fs.statSync.bind(fs);
    vi.spyOn(fs, "statSync").mockImplementation(((file: fs.PathLike, options?: fs.StatOptions) => {
      if (refuseRead && String(file) === path.join(directory, "state.json")) throw Object.assign(new Error("live state I/O unavailable"), { code: "EIO" });
      return stat(file, options);
    }) as typeof fs.statSync);
    const payload = "live".repeat(1024);
    const live = large.put({ key: "test/live", value: payload, identity });
    const liveResult = live.then(value => ({ value }), error => ({ error }));
    let attempt: Promise<{ value: unknown } | { error: unknown }> | undefined;
    let accepted: unknown, refused: unknown;
    try {
      await until(() => fs.readFileSync(path.join(directory, "state.json"), "utf8").includes('"test/live"'));
      expect(fs.statSync(path.join(directory, "state.json")).size > cap).toBe(kind === "size");
      refuseRead = kind === "io";
      expect(fs.readFileSync(path.join(directory, "state.durable.json"), "utf8")).toBe(checkpoint);
      let finished = false;
      attempt = small.put({ key: "test/small", value: 1, identity }).then(value => { finished = true; return { value }; }, error => { finished = true; return { error }; });
      await until(() => finished || fs.readFileSync(path.join(directory, "state.json"), "utf8").includes('"test/small"'));
    } finally {
      refuseRead = false;
      release();
      [accepted, refused] = await Promise.all([liveResult, attempt]);
    }
    expect(refused).toMatchObject({ error: expect.objectContaining({ message: expect.stringContaining(kind === "size" ? `state exceeds ${cap} bytes` : "live state I/O unavailable") }) });
    expect(accepted).toHaveProperty("value.version", initial.version + 1);
    const restarted = new MeshStore(directory, 256, 100, { maxStateBytes: 65536 });
    expect(restarted.get("test/live", { fresh: true })?.value).toBe(payload);
    expect(restarted.get("test/small", { fresh: true })).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(path.join(directory, "state.durable.json"), "utf8")).highWater).toBe(initial.version + 1);
  });

});
