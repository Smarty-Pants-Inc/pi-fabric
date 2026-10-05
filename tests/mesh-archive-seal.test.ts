import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ArchiveSha256 } from "../src/mesh/archive-sha256.js";
import { ARCHIVE_DIGEST_SLICE_BYTES, archiveFileName, currentBoot, MeshArchive, MeshArchiveRecoveryChanged, MESH_ARCHIVE_CONFIG } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

const roots: string[] = [];
const from = { id: "session:archive-seal", name: "archive", kind: "main" as const };
const day = "2026/09/27";
const at = Date.parse("2026-09-27T23:59:00Z");
const event = (sequence: number, text = "é🐈"): MeshEvent => ({ id: `event-${sequence}`, sequence, topic: "ops.owner", kind: "message", from, text, createdAt: at });
const entry = (sequence: number, text?: string) => { const e = event(sequence, text); return { event: e, line: JSON.stringify(e) }; };
const fixture = (options: { maxEventBytes?: number } = {}) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "archive-seal-")); roots.push(base);
  const root = path.join(base, "mesh"), dir = path.join(base, "archive");
  fs.mkdirSync(root); fs.mkdirSync(dir);
  fs.writeFileSync(path.join(root, MESH_ARCHIVE_CONFIG), JSON.stringify({ version: 1, dir }));
  fs.writeFileSync(path.join(dir, "BOOT"), currentBoot());
  const archive = new MeshArchive(dir, root);
  const store = new MeshStore(root, options.maxEventBytes ?? 4096, 500);
  const target = path.join(dir, day, archiveFileName("ops.owner"));
  const sealPath = path.join(dir, day, "SEAL.json");
  const seal = () => JSON.parse(fs.readFileSync(sealPath, "utf8"));
  return { root, dir, archive, store, target, sealPath, seal };
};
const close = (f: ReturnType<typeof fixture>, sequence: number) => {
  const e = entry(sequence); e.event.createdAt = at + 86400_000; e.line = JSON.stringify(e.event);
  f.archive.catchUp([e]);
};
// This is the historical #seal algorithm, not a new digest-format oracle.
const oldSeal = (file: string) => {
  const bytes = fs.readFileSync(file);
  const events = bytes.toString("utf8").split("\n").slice(0, -1).filter(Boolean).flatMap(line => {
    try { const e = JSON.parse(line); return typeof e.sequence === "number" && typeof e.id === "string" ? [e] : []; }
    catch { return []; }
  });
  return { lines: events.length, firstSequence: Math.min(...events.map(e => e.sequence)),
    lastSequence: Math.max(0, ...events.map(e => e.sequence)), sha256: createHash("sha256").update(bytes).digest("hex") };
};
const trackReads = (root: string) => {
  const opened = new Map<number, string>();
  const reads: Array<{ bytes: number; locked: boolean; file: string }> = [];
  const open = fs.openSync, read = fs.readSync, readFile = fs.readFileSync;
  vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    const fd = (open as (...args: unknown[]) => number)(file, ...args); opened.set(fd, String(file)); return fd;
  }) as typeof fs.openSync);
  vi.spyOn(fs, "readSync").mockImplementation(((fd: number, ...args: unknown[]) => {
    const bytes = (read as (...args: unknown[]) => number)(fd, ...args);
    const file = opened.get(fd) ?? "";
    if (file.endsWith(".jsonl") && file.includes(`${path.sep}archive${path.sep}`)) reads.push({ bytes, file, locked: fs.existsSync(path.join(root, ".lock")) });
    return bytes;
  }) as typeof fs.readSync);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    const value = (readFile as (...args: unknown[]) => string | Buffer)(file, ...args);
    if (String(file).endsWith(".jsonl") && String(file).includes(`${path.sep}archive${path.sep}`)) reads.push({ bytes: Buffer.byteLength(value), file: String(file), locked: fs.existsSync(path.join(root, ".lock")) });
    return value;
  }) as typeof fs.readFileSync);
  return reads;
};
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });

describe("durable off-lock archive sealing (#4383)", () => {
  it("resumes standard SHA-256 across padding boundaries and JSON round-trips", () => {
    for (const size of [0, 1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 4097, 1024 * 1024]) {
      const bytes = Buffer.alloc(size); for (let i = 0; i < size; i++) bytes[i] = i % 251;
      let hash = new ArchiveSha256();
      for (let offset = 0; offset < size; offset += 97) hash = new ArchiveSha256(JSON.parse(JSON.stringify(hash.update(bytes.subarray(offset, offset + 97)).state())));
      expect(hash.digest()).toBe(createHash("sha256").update(bytes).digest("hex"));
      expect(hash.state().bytes).toBe(size);
    }
    expect(() => new ArchiveSha256({ words: [], bytes: 0, tail: "" })).toThrow("checkpoint");
  });

  it("emits the historical seal digest and counters, without reading closed-day bytes", () => {
    const f = fixture(); f.archive.catchUp([entry(1), entry(3), entry(5, "x".repeat(8192))]);
    const expected = oldSeal(f.target);
    const reads = trackReads(f.root); close(f, 6);
    expect(reads.filter(r => r.file === f.target)).toEqual([]);
    expect(f.seal().files["ops.owner.jsonl"]).toEqual(expected);
  });

  it("restores the running digest after a same-boot crash before the live append", async () => {
    const f = fixture();
    vi.useFakeTimers({ now: at, toFake: ["Date"] });
    await f.store.publish({ topic: "ops.owner", from, text: "committed" });
    f.archive.begin(entry(2, "crashed")); // Leave PENDING and the synced line/checkpoint.
    const restarted = new MeshStore(f.root, 4096, 500);
    await restarted.publish({ topic: "ops.owner", from, text: "restart" });
    const expected = oldSeal(f.target);
    vi.setSystemTime(at + 86400_000);
    await restarted.publish({ topic: "ops.owner", from, text: "rollover" });
    expect(f.seal().files["ops.owner.jsonl"]).toEqual(expected);
    expect(fs.readFileSync(f.target, "utf8")).not.toContain("crashed");
  });

  it("recovers a rebooted durable append and its running digest", async () => {
    const f = fixture(); vi.useFakeTimers({ now: at, toFake: ["Date"] });
    await f.store.publish({ topic: "ops.owner", from, text: "live" });
    f.archive.begin(entry(2, "durable but live lost"));
    fs.writeFileSync(path.join(f.dir, "BOOT"), "previous boot");
    await new MeshStore(f.root, 4096, 500).publish({ topic: "ops.owner", from, text: "restart" });
    const expected = oldSeal(f.target);
    vi.setSystemTime(at + 86400_000);
    await f.store.publish({ topic: "ops.owner", from, text: "next day" });
    expect(f.seal().files["ops.owner.jsonl"]).toEqual(expected);
    expect(expected.lines).toBe(3);
  });

  it("coexists idempotently with an old-code seal and rebuilds after old-code appends", () => {
    const f = fixture(); f.archive.catchUp([entry(1), entry(2)]);
    fs.appendFileSync(f.target, `${entry(3).line}\n`); // Old writer ignores digest checkpoints.
    const expected = oldSeal(f.target);
    fs.writeFileSync(f.sealPath, JSON.stringify({ version: 1, day, files: { "ops.owner.jsonl": expected } }) + "\n");
    const oldBytes = fs.readFileSync(f.sealPath, "utf8");
    close(f, 4); expect(fs.readFileSync(f.sealPath, "utf8")).toBe(oldBytes);
    fs.rmSync(f.sealPath); close(f, 5);
    expect(f.seal().files["ops.owner.jsonl"]).toEqual(expected);
    const newBytes = fs.readFileSync(f.sealPath, "utf8"); close(f, 6);
    expect(fs.readFileSync(f.sealPath, "utf8")).toBe(newBytes);
    expect(f.seal().files["ops.owner.jsonl"]).toEqual(oldSeal(f.target));
  });

  it("advances a legacy day in one globally bounded slice per rollover/retry", () => {
    const f = fixture(); fs.mkdirSync(path.dirname(f.target), { recursive: true });
    const lines = Array.from({ length: 1000 }, (_, i) => entry(i + 1, "x".repeat(1024)).line + "\n").join("");
    fs.writeFileSync(f.target, lines);
    fs.writeFileSync(path.join(f.dir, "HEAD.json"), JSON.stringify({ sequence: 1000, id: "event-1000", file: `${day}/ops.owner.jsonl` }));
    const expected = oldSeal(f.target);
    const reads = trackReads(f.root);
    for (let i = 0; i < 40 && !fs.existsSync(f.sealPath); i++) {
      const before = reads.length; close(f, 1001 + i);
      expect(reads.slice(before).filter(r => r.file === f.target).reduce((n, r) => n + r.bytes, 0)).toBeLessThanOrEqual(ARCHIVE_DIGEST_SLICE_BYTES);
    }
    expect(f.seal().files["ops.owner.jsonl"]).toEqual(expected);
  });

  it("rejects a raced old writer's recovery snapshot before touching PENDING or live state", () => {
    const f = fixture(); f.archive.catchUp([entry(1)]); const pending = f.archive.begin(entry(2));
    fs.writeFileSync(path.join(f.dir, "BOOT"), "prior boot");
    const plan = f.archive.prepareRecovery(1);
    fs.appendFileSync(f.target, `${entry(3).line}\n`);
    expect(() => f.archive.recover(1, plan)).toThrow(MeshArchiveRecoveryChanged);
    expect(f.archive.pending()?.id).toBe(pending.id);
    expect(f.archive.prepareRecovery(1)?.promote.map(e => e.event.sequence)).toEqual([2, 3]);
  });

  it("reads BOOT recovery only back to the live sequence, outside the mesh lock", async () => {
    const f = fixture(); vi.useFakeTimers({ now: at, toFake: ["Date"] });
    const entries = Array.from({ length: 4096 }, (_, i) => entry(i + 1, "x".repeat(1024)));
    f.archive.catchUp(entries);
    fs.writeFileSync(path.join(f.root, "events.jsonl"), `${entries.at(-2)!.line}\n`);
    fs.writeFileSync(path.join(f.root, "sequence"), "4095");
    fs.writeFileSync(path.join(f.dir, "BOOT"), "prior boot");
    const reads = trackReads(f.root);
    const published = await f.store.publish({ topic: "ops.owner", from, text: "after reboot" });
    expect(published.sequence).toBe(4097);
    const outside = reads.filter(r => !r.locked);
    expect(outside.reduce((n, r) => n + r.bytes, 0)).toBeLessThanOrEqual(ARCHIVE_DIGEST_SLICE_BYTES);
    expect(reads.filter(r => r.locked).reduce((n, r) => n + r.bytes, 0)).toBeLessThanOrEqual(ARCHIVE_DIGEST_SLICE_BYTES);
    expect(fs.readFileSync(path.join(f.root, "events.jsonl"), "utf8")).toContain("event-4096");
  });

  it("repairs a large torn BOOT pending suffix without expanding locked archive reads", async () => {
    const f = fixture(); vi.useFakeTimers({ now: at, toFake: ["Date"] });
    await f.store.publish({ topic: "ops.owner", from });
    const size = fs.statSync(f.target).size;
    fs.appendFileSync(f.target, "x".repeat(2 * 1024 * 1024));
    fs.writeFileSync(path.join(f.dir, "PENDING.json"), JSON.stringify({ sequence: 2, id: "torn", file: `${day}/ops.owner.jsonl`, size }));
    fs.writeFileSync(path.join(f.root, "sequence"), "2");
    fs.writeFileSync(path.join(f.dir, "BOOT"), "prior boot");
    const reads = trackReads(f.root);
    const published = await f.store.publish({ topic: "ops.owner", from });
    expect(published.sequence).toBe(3);
    expect(reads.every(r => r.bytes <= ARCHIVE_DIGEST_SLICE_BYTES)).toBe(true);
    expect(reads.filter(r => r.locked).reduce((n,r) => n + r.bytes, 0)).toBeLessThanOrEqual(ARCHIVE_DIGEST_SLICE_BYTES);
    expect(fs.statSync(f.target).size).toBeLessThan(4096);
  });

  it("parses oversized legacy lines outside the mesh lock, then installs only a matching digest", async () => {
    const f = fixture(); vi.useFakeTimers({ now: at + 86400_000, toFake: ["Date"] });
    fs.mkdirSync(path.dirname(f.target), { recursive: true });
    fs.writeFileSync(f.target, `${entry(1, "é".repeat(200_000)).line}\n${entry(2).line}\n`);
    fs.writeFileSync(path.join(f.dir, "HEAD.json"), JSON.stringify({ sequence: 2, id: "event-2", file: `${day}/ops.owner.jsonl` }));
    fs.writeFileSync(path.join(f.root, "sequence"), "2");
    const expected = oldSeal(f.target);
    await f.store.publish({ topic: "ops.owner", from });
    expect(fs.existsSync(f.sealPath)).toBe(false);
    const reads = trackReads(f.root);
    await f.store.publish({ topic: "ops.owner", from });
    expect(reads.filter(r => r.locked && r.file === f.target).reduce((n,r) => n + r.bytes, 0)).toBeLessThanOrEqual(ARCHIVE_DIGEST_SLICE_BYTES);
    expect(reads.some(r => !r.locked && r.file === f.target)).toBe(true);
    expect(f.seal().files["ops.owner.jsonl"]).toEqual(expected);
  });

  it("publish retries a raced BOOT preflight before reserving a new sequence", async () => {
    const f = fixture(); vi.useFakeTimers({ now: at, toFake: ["Date"] });
    await f.store.publish({ topic: "ops.owner", from });
    fs.writeFileSync(path.join(f.dir, "BOOT"), "previous boot");
    const prepare = MeshArchive.prototype.prepareRecovery; let raced = false;
    vi.spyOn(MeshArchive.prototype, "prepareRecovery").mockImplementation(function (this: MeshArchive, lastLive) {
      const plan = prepare.call(this, lastLive);
      if (!raced) { raced = true; fs.appendFileSync(f.target, `${entry(2, "old writer").line}\n`); }
      return plan;
    });
    const published = await f.store.publish({ topic: "ops.owner", from });
    expect(published.sequence).toBe(3);
    expect(fs.readFileSync(path.join(f.root, "events.jsonl"), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line).sequence)).toEqual([1, 2, 3]);
  });

  it("rolls over a 200 MB tracked day with a mesh-lock hold below 50 ms", { timeout: 120_000 }, async () => {
    const f = fixture({ maxEventBytes: 128 * 1024 }); vi.useFakeTimers({ now: at, toFake: ["Date"] });
    const text = "x".repeat(64 * 1024 - 512);
    const entries = Array.from({ length: Math.ceil(200_000_000 / (text.length + 160)) + 2 }, (_, i) => entry(i + 1, text));
    f.archive.catchUp(entries);
    expect(fs.statSync(f.target).size).toBeGreaterThanOrEqual(200_000_000);
    // Catch-up creates the same durable metadata a day's incremental publishes would create.
    fs.writeFileSync(path.join(f.root, "events.jsonl"), `${entries.at(-1)!.line}\n`);
    fs.writeFileSync(path.join(f.root, "sequence"), String(entries.length + 1));
    f.archive.begin(entry(entries.length + 1, "durable append before checkpoint/live commit"));
    fs.writeFileSync(path.join(f.dir, "BOOT"), "previous boot");
    const recoveryReads = trackReads(f.root);
    const restarted = new MeshStore(f.root, 128 * 1024, 500);
    expect((await restarted.publish({ topic: "ops.owner", from, text: "resumed digest" })).sequence).toBe(entries.length + 2);
    const recovery = {
      outsideLockBytes: recoveryReads.filter(r => !r.locked).reduce((n,r) => n + r.bytes, 0),
      underLockBytes: recoveryReads.filter(r => r.locked).reduce((n,r) => n + r.bytes, 0),
    };
    expect(recovery.outsideLockBytes).toBeLessThanOrEqual(2 * ARCHIVE_DIGEST_SLICE_BYTES);
    expect(recovery.underLockBytes).toBeLessThanOrEqual(ARCHIVE_DIGEST_SLICE_BYTES + 4096);
    const digestFile = path.join(path.dirname(f.target), `.digest-${createHash("sha256").update(path.basename(f.target)).digest("hex")}.json`);
    const bytes = fs.statSync(f.target).size;
    expect(JSON.parse(fs.readFileSync(digestFile, "utf8")).hash.bytes).toBe(bytes);
    vi.restoreAllMocks();
    const reads = trackReads(f.root);
    const lock = path.join(f.root, ".lock"); let start = 0, held = 0;
    const mkdir = fs.mkdirSync, rename = fs.renameSync;
    vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
      const before = performance.now();
      const result = (mkdir as (...args: unknown[]) => unknown)(target, ...args);
      if (String(target) === lock) start = before; return result;
    }) as typeof fs.mkdirSync);
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      rename(source, target);
      if (String(source) === lock) held = performance.now() - start;
    });
    const barriers: Array<{ kind: string; ms: number }> = [];
    const fdatasync = fs.fdatasyncSync, fsync = fs.fsyncSync;
    vi.spyOn(fs, "fdatasyncSync").mockImplementation(fd => {
      const at = performance.now(); fdatasync(fd); barriers.push({ kind: "fdatasync", ms: performance.now() - at });
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const at = performance.now(); fsync(fd); barriers.push({ kind: "fsync", ms: performance.now() - at });
    });
    vi.setSystemTime(at + 86400_000);
    const published = await f.store.publish({ topic: "ops.owner", from, text: "rollover" });
    expect(published.sequence).toBe(entries.length + 3);
    const closedDayBytesUnderLock = reads.filter(r => r.locked && r.file === f.target).reduce((n, r) => n + r.bytes, 0);
    const tracked = { bytes, lockHoldMs: held, closedDayBytesUnderLock, barriers: [...barriers] };
    expect(closedDayBytesUnderLock).toBe(0);
    expect(held).toBeGreaterThan(0); expect(held).toBeLessThan(50);
    expect(f.seal().files["ops.owner.jsonl"].sha256).toBe(createHash("sha256").update(fs.readFileSync(f.target)).digest("hex"));
    // The same 200 MB file with no digest is legacy: only one bounded slice is allowed.
    fs.rmSync(f.sealPath);
    fs.rmSync(digestFile);
    const before = reads.length; barriers.length = 0;
    await f.store.publish({ topic: "ops.owner", from, text: "legacy retry" });
    const legacyBytes = reads.slice(before).filter(r => r.locked && r.file === f.target).reduce((n, r) => n + r.bytes, 0);
    const legacy = { bytes, lockHoldMs: held, closedDayBytesUnderLock: legacyBytes, barriers };
    console.log(`ARCHIVE_ROLLOVER_BENCH ${JSON.stringify({ tracked, legacy, recovery })}`);
    if (process.env.ARCHIVE_SEAL_BENCH_OUT) fs.writeFileSync(process.env.ARCHIVE_SEAL_BENCH_OUT, JSON.stringify({ tracked, legacy, recovery }, null, 2) + "\n");
    expect(legacyBytes).toBeLessThanOrEqual(ARCHIVE_DIGEST_SLICE_BYTES);
    expect(fs.existsSync(f.sealPath)).toBe(false);
    expect(held).toBeGreaterThan(0); expect(held).toBeLessThan(50);

  });
});
