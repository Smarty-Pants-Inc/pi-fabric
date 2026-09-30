import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { writeFileAtomic, writeJsonAtomic, writeJsonAtomicAsync } from "../src/core/atomic-write.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { MeshStore } from "../src/mesh/store.js";

describe("#169 round 4 new-directory durability and async contract", () => {
  it.skipIf(process.platform === "win32")("syncs new nested directories top down and their existing parent before file barriers", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-new-dirs-"));
    const outer = path.join(root, "new");
    const inner = path.join(outer, "nested");
    const target = path.join(inner, "record.json");
    const events: string[] = [];
    const descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    const rename = fs.renameSync.bind(fs);
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      return fd;
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const file = descriptors.get(fd)!;
      events.push(file.startsWith(`${target}.`) ? "file" : file);
      sync(fd);
    });
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      events.push("rename");
      rename(from, to);
    });
    try {
      writeJsonAtomic(target, { value: 1 }, { durable: true });
      expect(events).toEqual([outer, inner, root, "file", "rename", inner]);
      expect(JSON.parse(fs.readFileSync(target, "utf8"))).toEqual({ value: 1 });
    } finally {
      renamed.mockRestore(); synced.mockRestore(); opened.mockRestore();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform === "win32").each(["outer", "inner", "parent"])("fails closed at the new-directory %s barrier", (barrier) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-new-dirs-failure-"));
    const outer = path.join(root, "new");
    const inner = path.join(outer, "nested");
    const target = path.join(inner, "record");
    const failingPath = { outer, inner, parent: root }[barrier as "outer" | "inner" | "parent"];
    const descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      return fd;
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      if (descriptors.get(fd) === failingPath) throw new Error("directory link barrier unavailable");
      sync(fd);
    });
    const closed = vi.spyOn(fs, "closeSync");
    try {
      expect(() => writeFileAtomic(target, "new", { durable: true })).toThrow("directory link barrier unavailable");
      expect(fs.existsSync(target)).toBe(false);
      expect(fs.readdirSync(inner)).toEqual([]);
      expect(closed).toHaveBeenCalledTimes(["outer", "inner", "parent"].indexOf(barrier) + 1);
    } finally {
      closed.mockRestore(); synced.mockRestore(); opened.mockRestore();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps new-directory ordinary sync and async writes unsynced and rejects async durable options at type level", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-new-dirs-ordinary-"));
    const synced = vi.spyOn(fs, "fsyncSync");
    try {
      writeFileAtomic(path.join(root, "sync", "nested", "record"), "ordinary");
      await writeJsonAtomicAsync(path.join(root, "async", "nested", "record"), { ordinary: true }, { space: 2 });
      if (false) {
        // @ts-expect-error Async atomic writes cannot promise stable-storage durability.
        await writeJsonAtomicAsync(path.join(root, "invalid"), {}, { durable: true });
      }
      expect(synced).not.toHaveBeenCalled();
    } finally { synced.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("#169 round 3 durable atomic writes", () => {
  it.each(["file", "json"])("orders %s barriers and leaves ordinary writes, registry saves and mesh puts unsynced", async (kind) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-durable-"));
    const target = path.join(root, "record.json");
    const events: string[] = [];
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    const rename = fs.renameSync.bind(fs);
    const descriptors = new Map<number, string>();
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      if (String(file) === root) expect(flags).toBe(fs.constants.O_RDONLY);
      return fd;
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      events.push(descriptors.get(fd) === root ? "sync-directory" : "sync-file");
      sync(fd);
    });
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      events.push("rename");
      rename(from, to);
    });
    try {
      if (kind === "json") writeJsonAtomic(target, { value: 1 }, { durable: true });
      else writeFileAtomic(target, "payload", { durable: true });
      expect(events).toEqual(process.platform === "win32" ? ["sync-file", "rename"] : ["sync-file", "rename", "sync-directory"]);
      synced.mockClear();
      writeFileAtomic(target, "ordinary");
      writeJsonAtomic(target, { ordinary: true });
      const registry = new ActorRegistryStore(path.join(root, "actors"));
      registry.write([{ id: "actor" }]);
      const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
      await mesh.put({ key: "hot/path", value: "ordinary", identity: { id: "test", name: "main", kind: "main" } });
      expect(synced).not.toHaveBeenCalled();
    } finally {
      renamed.mockRestore(); synced.mockRestore(); opened.mockRestore();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(process.platform === "win32" ? [1] : [1, 2])("throws at barrier %i, closes descriptors and cleans unrenamed temps", (barrier) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-durable-failure-"));
    const target = path.join(root, "record");
    fs.writeFileSync(target, "old");
    const sync = fs.fsyncSync.bind(fs);
    let count = 0;
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      if (++count === barrier) throw new Error("barrier unavailable");
      sync(fd);
    });
    const closed = vi.spyOn(fs, "closeSync");
    try {
      expect(() => writeFileAtomic(target, "new", { durable: true })).toThrow("barrier unavailable");
      expect(closed).toHaveBeenCalledTimes(barrier);
      expect(fs.readdirSync(root)).toEqual(["record"]);
      expect(fs.readFileSync(target, "utf8")).toBe(barrier === 1 ? "old" : "new");
    } finally { synced.mockRestore(); closed.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
