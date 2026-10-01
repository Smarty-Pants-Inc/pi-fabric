import fs from "node:fs";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { renameAtomic, syncDirectoryChain, syncPathNamespace, writeFileAtomic, writeJsonAtomic, writeJsonAtomicAsync } from "../src/core/atomic-write.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { MeshStore } from "../src/mesh/store.js";

const directoryChain = (directory: string): string[] => {
  const chain: string[] = [];
  for (let current = path.resolve(directory); ; current = path.dirname(current)) {
    chain.push(current);
    if (path.dirname(current) === current) return chain;
  }
};

describe("#180 S4 physical directory ancestry", () => {
  it.skipIf(process.platform === "win32").each(["nested-component", "link-target"].flatMap((chain) =>
    ["target-leaf", "target-ancestor", "alias-parent", "alias-ancestor"].map((barrier) => ({ chain, barrier })),
  ))("fails closed and retries $barrier across $chain directory symlinks", ({ chain, barrier }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-symlink-chain-"));
    const aliases = path.join(root, "aliases");
    const intermediate = path.join(root, "intermediate-tree", "leaf");
    const target = path.join(root, "physical", "leaf");
    fs.mkdirSync(aliases);
    fs.mkdirSync(intermediate, { recursive: true });
    fs.mkdirSync(target, { recursive: true });
    const second = path.join(intermediate, "second");
    fs.symlinkSync(path.relative(intermediate, target), second, "dir");
    fs.symlinkSync(path.relative(aliases, chain === "link-target" ? second : intermediate), path.join(aliases, "first"), "dir");
    const directory = chain === "link-target" ? path.join(aliases, "first") : path.join(aliases, "first", "second");
    const failingPath = { "target-leaf": target, "target-ancestor": path.dirname(target),
      "alias-parent": intermediate, "alias-ancestor": path.dirname(intermediate) }[barrier]!;
    const events: string[] = [];
    const descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    let fail = true;
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      return fd;
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const file = descriptors.get(fd)!;
      events.push(file);
      if (fail && file === failingPath) throw new Error("physical namespace barrier unavailable");
      sync(fd);
    });
    try {
      for (let attempt = 0; attempt < 3; attempt++) {
        expect(() => syncDirectoryChain(directory)).toThrow("physical namespace barrier unavailable");
        expect(events).toContain(failingPath);
        events.length = 0;
      }
      fail = false;
      syncDirectoryChain(directory);
      events.push("acknowledged");
      // Both physical targets and the directories containing both symlinks matter.
      expect(events).toEqual([...new Set([...directoryChain(target), ...directoryChain(intermediate), ...directoryChain(aliases)]), "acknowledged"]);
    } finally {
      synced.mockRestore(); opened.mockRestore();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("#180 S4 hop-by-hop namespace confirmation", () => {
  it.skipIf(process.platform === "win32").each([40, 41])("bounds a relative file-link chain at %s hops", (hops) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-hop-limit-"));
    const target = path.join(root, "receipt");
    fs.writeFileSync(target, "receipt");
    for (let hop = hops - 1; hop >= 0; hop--) fs.symlinkSync(hop === hops - 1 ? "receipt" : `link-${hop + 1}`, path.join(root, `link-${hop}`));
    try {
      const confirm = () => syncPathNamespace(path.join(root, "link-0"), fs.statSync(target));
      if (hops === 40) expect(confirm).not.toThrow();
      else expect(confirm).toThrow("Namespace symlink loop or hop limit exceeded");
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32")("detects a relative symlink loop before any barrier", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-hop-loop-"));
    fs.symlinkSync("second", path.join(root, "first"));
    fs.symlinkSync("first", path.join(root, "second"));
    const synced = vi.spyOn(fs, "fsyncSync");
    try {
      expect(() => syncPathNamespace(path.join(root, "first"))).toThrow("Namespace symlink loop or hop limit exceeded");
      expect(synced).not.toHaveBeenCalled();
    } finally { synced.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32")("resolves dot-dot after following a target directory link", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-hop-dotdot-"));
    fs.mkdirSync(path.join(root, "physical", "leaf"), { recursive: true });
    const target = path.join(root, "physical", "receipt");
    fs.writeFileSync(target, "receipt");
    fs.symlinkSync("physical/leaf", path.join(root, "directory"), "dir");
    fs.symlinkSync("directory/../receipt", path.join(root, "alias"));
    try {
      expect(() => syncPathNamespace(path.join(root, "alias"), fs.statSync(target))).not.toThrow();
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32")("refuses a containing directory replaced before its barrier, then retries", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-hop-directory-race-"));
    const directory = path.join(root, "namespace");
    fs.mkdirSync(directory);
    const open = fs.openSync.bind(fs);
    let replaced = false;
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      if (!replaced && String(file) === directory) {
        replaced = true;
        fs.renameSync(directory, path.join(root, "old"));
        fs.mkdirSync(directory);
      }
      return open(file, flags, mode);
    });
    try {
      expect(() => syncDirectoryChain(directory)).toThrow("Namespace directory changed before barrier");
      expect(() => syncDirectoryChain(directory)).not.toThrow();
    } finally { opened.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("#169 security S1 directory-link retries and P3 rename contract", () => {
  const cases = ["outer", "inner", "parent"].flatMap((barrier) =>
    ["same-process", "fresh-process"].map((retry) => ({ barrier, retry })),
  );
  it.skipIf(process.platform === "win32").each(cases)("retries the owed $barrier barrier before acknowledgment in $retry", ({ barrier, retry }) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-owed-barrier-"));
    const outer = path.join(root, "new");
    const inner = path.join(outer, "nested");
    const target = path.join(inner, "record");
    const failingPath = { outer, inner, parent: root }[barrier as "outer" | "inner" | "parent"];
    const descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs);
    const sync = fs.fsyncSync.bind(fs);
    let fail = true;
    const events: string[] = [];
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      descriptors.set(fd, String(file));
      return fd;
    });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const file = descriptors.get(fd)!;
      events.push(file);
      if (fail && file === failingPath) throw new Error("owed directory barrier unavailable");
      sync(fd);
    });
    try {
      expect(() => writeFileAtomic(target, "first", { durable: true })).toThrow("owed directory barrier unavailable");
      expect(fs.statSync(inner).isDirectory()).toBe(true); // Leave the entire failed tree in place.
      events.length = 0;
      if (retry === "same-process") {
        expect(() => writeFileAtomic(target, "retry", { durable: true })).toThrow("owed directory barrier unavailable");
        expect(events).toContain(failingPath);
        events.length = 0;
        fail = false;
        writeFileAtomic(target, "accepted", { durable: true });
        events.push("acknowledged");
        expect(events.indexOf(failingPath)).toBeGreaterThanOrEqual(0);
        expect(events.indexOf(failingPath)).toBeLessThan(events.indexOf("acknowledged"));
      } else {
        // Source TypeScript need not be erasable by Node's strip-only parser. Keep
        // a parameter property in the fresh-process entry so this remains true when
        // production sources gain one (the PR #198 CI failure happened before retry).
        const retryModule = path.join(root, "retry-module.ts");
        fs.writeFileSync(retryModule, `export { writeFileAtomic } from ${JSON.stringify(path.resolve("src/core/atomic-write.ts"))};\nexport class Receipt { constructor(readonly target: string) {} }\n`);
        const child = spawnSync(process.execPath, [path.resolve("tests/fixtures/atomic-write-retry.mjs"),
          retryModule, target, failingPath], { encoding: "utf8", timeout: 5_000 });
        expect(child.error).toBeUndefined();
        expect(child.status, child.stderr).toBe(0);
        const attempts = JSON.parse(child.stdout) as Array<{ acknowledged: boolean; events: string[]; error?: string }>;
        expect(attempts[0]).toMatchObject({ acknowledged: false, error: "owed directory barrier unavailable" });
        expect(attempts[0]!.events).toContain(failingPath);
        expect(attempts[1]!.acknowledged).toBe(true);
        expect(attempts[1]!.events.indexOf(failingPath)).toBeGreaterThanOrEqual(0);
        expect(attempts[1]!.events.indexOf(failingPath)).toBeLessThan(attempts[1]!.events.indexOf("acknowledged"));
      }
      expect(fs.readFileSync(target, "utf8")).toBe("accepted");
    } finally {
      synced.mockRestore(); opened.mockRestore();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("keeps renameAtomic rename-only, retries contention and rejects durable options at type level", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-rename-contract-"));
    const source = path.join(root, "source");
    const target = path.join(root, "target");
    fs.writeFileSync(source, "payload");
    const rename = fs.renameSync.bind(fs);
    let calls = 0;
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (++calls === 1) throw Object.assign(new Error("contended"), { code: "EBUSY" });
      rename(from, to);
    });
    const synced = vi.spyOn(fs, "fsyncSync");
    try {
      renameAtomic(source, target, { renameRetries: 2, renameRetryDelayMs: 0 });
      if (false) {
        // @ts-expect-error Rename-only operations cannot promise stable-storage durability.
        renameAtomic(source, target, { durable: true });
      }
      expect(calls).toBe(2);
      expect(synced).not.toHaveBeenCalled();
      expect(fs.readFileSync(target, "utf8")).toBe("payload");
    } finally {
      synced.mockRestore(); renamed.mockRestore(); fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

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
      expect(events).toEqual([outer, inner, root, "file", "rename", ...directoryChain(inner)]);
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
      const file = descriptors.get(fd)!;
      events.push(file.startsWith(`${target}.`) ? "sync-file" : file);
      sync(fd);
    });
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      events.push("rename");
      rename(from, to);
    });
    try {
      if (kind === "json") writeJsonAtomic(target, { value: 1 }, { durable: true });
      else writeFileAtomic(target, "payload", { durable: true });
      expect(events).toEqual(["sync-file", "rename", ...(process.platform === "win32" ? [] : directoryChain(root))]);
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
