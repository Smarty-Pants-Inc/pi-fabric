import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  captureMeshLegacyDebris, judgeMeshLegacyDebris, MeshLegacyDebrisUnrecoverableError,
  readMeshRecoveryOwner, requireMeshOwnerOnly,
} from "../src/mesh/legacy-debris.js";

// Keep: data safety/trust boundary (#9569 T6). Real filesystem classifier, no native deletion or child processes.
// These are grammar/snapshot proofs, NOT NTFS handle/reparse or native crash evidence.
const scratch = fs.realpathSync(os.tmpdir());
const roots: string[] = [];
const make = (): string => {
  const root = fs.mkdtempSync(path.join(scratch, "fabric-9569-debris-"));
  roots.push(root);
  const lock = path.join(root, ".lock");
  fs.mkdirSync(lock);
  return lock;
};
const UUID = "01234567-89ab-cdef-0123-456789abcdef";
const released = `owner.released.${UUID}`;
const record = (pid = 123): string => `${UUID}\n${pid}\n1\n`;
const inventory = (lock: string) => captureMeshLegacyDebris(lock, fs.lstatSync(lock, { bigint: true }));
afterEach(() => {
  for (const root of roots.splice(0)) {
    if (path.dirname(root) !== scratch || !path.basename(root).startsWith("fabric-9569-debris-")) throw new Error("unsafe test cleanup");
    fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("bounded independent-proof legacy inventory", () => {
  it("captures complete per-file records, exact bigint identities and child-first directories, top L last", () => {
    const lock = make();
    fs.mkdirSync(path.join(lock, "r.old", "r.inner"), { recursive: true });
    fs.writeFileSync(path.join(lock, released), record(123));
    fs.writeFileSync(path.join(lock, "r.old", "owner"), record(456));
    fs.writeFileSync(path.join(lock, "r.old", "r.inner", released), record(789));
    const entries = inventory(lock);
    expect(entries).toHaveLength(6);
    expect(entries.at(-1)!.path).toBe(lock);
    expect(entries.filter(entry => entry.record !== undefined).map(entry => entry.record).sort()).toEqual([record(123), record(456), record(789)].sort());
    for (let i = 0; i < entries.length - 1; i++) {
      const entry = entries[i]!;
      expect(typeof entry.stat.ino).toBe("bigint");
      expect(entries.findIndex(parent => parent.path === path.dirname(entry.path))).toBeGreaterThan(i);
      if (entry.record !== undefined) expect(entry.stat.size).toBe(BigInt(Buffer.byteLength(entry.record)));
    }
  });

  it("empty recognized r.* trees need no borrowed owner proof", async () => {
    const lock = make();
    fs.mkdirSync(path.join(lock, "r.empty", "r.empty2"), { recursive: true });
    const entries = inventory(lock);
    let judged = 0;
    await judgeMeshLegacyDebris(lock, entries, async () => { judged++; return undefined; });
    expect(judged).toBe(0);
    expect(entries.map(entry => entry.record)).toEqual([undefined, undefined, undefined]);
    expect(fs.existsSync(lock)).toBe(true); // snapshot/judgment NEVER mutates
  });

  it.each(["raw", "r.raw", "owner.released.short", "owner.released.01234567", "f", ".recovery-fence", "owner"])(
    "protects unknown direct file %s, even when it contains another owner's valid record", name => {
      const lock = make();
      const file = path.join(lock, name);
      fs.writeFileSync(file, record());
      expect(() => inventory(lock)).toThrow(MeshLegacyDebrisUnrecoverableError);
      expect(fs.readFileSync(file, "utf8")).toBe(record());
    });

  // Win32 normalizes trailing dot/space names before creation; these lexical fixtures require POSIX names.
  it.skipIf(process.platform === "win32").each(["unknown", "r.", "r.bad.", "r.bad "])("protects unknown/illegal directory %s", name => {
    const lock = make();
    fs.mkdirSync(path.join(lock, name));
    expect(() => inventory(lock)).toThrow(MeshLegacyDebrisUnrecoverableError);
    expect(fs.existsSync(path.join(lock, name))).toBe(true);
  });

  it("rejects a raw dependent payload even beside a complete nested owner, before any death judgment", () => {
    const lock = make();
    const dir = path.join(lock, "r.old");
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, "owner"), record());
    fs.writeFileSync(path.join(dir, "payload"), "do not borrow owner authority");
    expect(() => inventory(lock)).toThrow(MeshLegacyDebrisUnrecoverableError);
    expect(fs.readFileSync(path.join(dir, "owner"), "utf8")).toBe(record());
    expect(fs.readFileSync(path.join(dir, "payload"), "utf8")).toBe("do not borrow owner authority");
  });

  it("a torn/empty file is a captured record, NEVER absence or an empty directory", () => {
    const lock = make();
    const file = path.join(lock, released);
    fs.writeFileSync(file, "");
    expect(readMeshRecoveryOwner(lock)).toBeUndefined();
    expect(readMeshRecoveryOwner(lock, file)!.record).toBe("");
    expect(inventory(lock).filter(entry => entry.record !== undefined)).toHaveLength(1);
  });

  it.each([4097, 65536])("protects oversize owner leaf (%i bytes) without an unbounded record read", size => {
    const lock = make();
    const file = path.join(lock, released);
    fs.writeFileSync(file, "x".repeat(size));
    expect(() => inventory(lock)).toThrow(expect.objectContaining({ category: "oversize-record" }));
    expect(fs.statSync(file).size).toBe(size);
  });

  it("rejects NUL/non-roundtripping owner bytes", () => {
    for (const bytes of [Buffer.from([0xff, 0xfe]), Buffer.from("x\0\n")]) {
      const lock = make();
      const file = path.join(lock, released);
      fs.writeFileSync(file, bytes);
      expect(() => inventory(lock)).toThrow(expect.objectContaining({ category: "invalid-record-encoding" }));
      expect(fs.readFileSync(file)).toEqual(bytes);
    }
  });

  it("caps depth at four and retains every too-deep directory", () => {
    const lock = make();
    const deep = path.join(lock, "r.1", "r.2", "r.3", "r.4", "r.5");
    fs.mkdirSync(deep, { recursive: true });
    expect(() => inventory(lock)).toThrow(expect.objectContaining({ category: "too-deep" }));
    expect(fs.existsSync(deep)).toBe(true);
  });

  it("caps the complete inventory at 64 INCLUDING top L", () => {
    const lock = make();
    for (let i = 0; i < 64; i++) fs.mkdirSync(path.join(lock, `r.${i}`));
    expect(() => inventory(lock)).toThrow(expect.objectContaining({ category: "too-many-entries" }));
    expect(fs.readdirSync(lock)).toHaveLength(64);
  });

  it("caps the framed inventory bytes, not just each separate record", () => {
    const lock = make();
    for (let i = 0; i < 17; i++) {
      const dir = path.join(lock, `r.${i}`);
      fs.mkdirSync(dir);
      fs.writeFileSync(path.join(dir, "owner"), "x".repeat(4096));
    }
    expect(() => inventory(lock)).toThrow(expect.objectContaining({ category: "oversize-inventory" }));
  });

  it.skipIf(process.platform === "win32")("rejects symbolic links without reading their target", () => {
    const lock = make();
    const target = path.join(path.dirname(lock), "external");
    fs.writeFileSync(target, "outside");
    fs.symlinkSync(target, path.join(lock, released));
    expect(() => inventory(lock)).toThrow(expect.objectContaining({ category: "link-or-reparse" }));
    expect(fs.readFileSync(target, "utf8")).toBe("outside");
  });

  it("does not judge a filename token: foreign record under a releaser's UUID remains its own authority", async () => {
    const lock = make();
    const foreign = `foreign-full-token\n456\n1\n`;
    fs.writeFileSync(path.join(lock, released), foreign);
    const seen: string[] = [];
    await expect(judgeMeshLegacyDebris(lock, inventory(lock), async entry => {
      seen.push(entry.record!); return "live-owner";
    })).rejects.toMatchObject({ category: "live-owner", path: path.join(lock, released), retryable: false });
    expect(seen).toEqual([foreign]);
    expect(fs.readFileSync(path.join(lock, released), "utf8")).toBe(foreign);
  });

  it("requires independent judgments for mixed leaves, all before ANY mutation", async () => {
    const lock = make();
    fs.mkdirSync(path.join(lock, "r.old"));
    const first = path.join(lock, released), second = path.join(lock, "r.old", "owner");
    fs.writeFileSync(first, record(123)); fs.writeFileSync(second, record(456));
    const seen: string[] = [];
    await expect(judgeMeshLegacyDebris(lock, inventory(lock), async entry => {
      // No leaf may have disappeared while judging another one.
      expect(fs.readFileSync(first, "utf8")).toBe(record(123));
      expect(fs.readFileSync(second, "utf8")).toBe(record(456));
      seen.push(entry.record!);
      return entry.record === record(456) ? "live-owner" : undefined;
    })).rejects.toMatchObject({ category: "live-owner" });
    expect(seen).toEqual([record(123), record(456)]);
  });

  it("an evidence-reader failure is typed, not an unexplained contention timeout", async () => {
    const lock = make();
    fs.writeFileSync(path.join(lock, released), record());
    await expect(judgeMeshLegacyDebris(lock, inventory(lock), async () => { throw new Error("inaccessible evidence"); }))
      .rejects.toMatchObject({ kind: "legacy-debris-unrecoverable", category: "evidence-unavailable:unknown" });
    expect(fs.readFileSync(path.join(lock, released), "utf8")).toBe(record());
  });

  it("C refuses owner plus ANY extras before destroying its complete proof", () => {
    const lock = make();
    fs.writeFileSync(path.join(lock, "owner"), record());
    fs.writeFileSync(path.join(lock, released), record(456));
    expect(() => requireMeshOwnerOnly(lock)).toThrow(expect.objectContaining({ category: "owner-with-extra-entries" }));
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(record());
  });
});
