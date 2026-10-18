import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { constants, zstdCompressSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import { canRemoveTerminalRun, compactTerminalRunEvents, pruneActorRunArchives, markRunRootActive, markRunRootClosed, sweepTempRunRoots, FABRIC_RUN_ROOT_PREFIX } from "../src/storage/retention.js";
import { openRunLogSource, readRunLogText } from "../src/run-log-source.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// Data-safety regression: only test-created private TMP fixtures are published/removed. The fixture
// installs real checksummed Node Zstd bytes, including on Windows where production compression is disabled.
const roots: string[] = [];
const HOUR = 3_600_000;
const now = 10 * HOUR;
const marker = '{"fabricTruncated":true,"reason":"terminal run event retention"}\n';
const frame = (bytes: Buffer) => zstdCompressSync(bytes, { params: { [constants.ZSTD_c_checksumFlag]: 1 } });
const events = (padding = 0) => Array.from({ length: 401 }, (_, sequence) => JSON.stringify({ sequence, text: "α😀".repeat(padding) }) + "\n");
const fixture = (record: Record<string, unknown> = {}, lines = events()) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "compressed-run-retention-"));
  roots.push(root); fs.chmodSync(root, 0o700);
  const directory = path.join(root, "runs", "old");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, "events.jsonl");
  const bytes = Buffer.from(lines.join("")), encoded = frame(bytes);
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id: "a".repeat(32), status: "completed", finishedAt: 4 * HOUR, transport: "process", sessionId: "2147483647", ...record }), { mode: 0o600 });
  fs.writeFileSync(`${file}.zst`, encoded, { mode: 0o600 });
  fs.writeFileSync(path.join(directory, "reply.json"), '{"text":"retained result"}', { mode: 0o600 });
  fs.utimesSync(directory, 4 * HOUR / 1000, 4 * HOUR / 1000);
  return { root, directory, file, bytes, encoded, lines };
};
const run = () => fixture().directory;
const prune = (directory: string, options: Partial<Parameters<typeof pruneActorRunArchives>[0]> = {}) => pruneActorRunArchives({
  runsDirectory: path.dirname(directory), retentionMs: 7 * 24 * HOUR, now, ...options,
});
const snapshot = (directory: string) => fs.readdirSync(directory).sort().map(name => {
  const file = path.join(directory, name), stat = fs.lstatSync(file);
  return { name, bytes: fs.readFileSync(file), ino: stat.ino, mode: stat.mode, mtime: stat.mtimeMs, ctime: stat.ctimeMs };
});
afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("compressed run retention names", () => {
  it("accepts compressed logs and exact codec temp evidence behind existing fences", () => {
    const root = run();
    for (const name of [
      ".events.jsonl.compress-123-0123456789abcdef0123456789abcdef.tmp",
      ".events.jsonl.restore-123-abcdef0123456789abcdef0123456789.tmp",
    ]) fs.writeFileSync(path.join(root, name), "evidence", { mode: 0o600 });
    expect(canRemoveTerminalRun(root)).toBe(true);
  });

  it("keeps malformed or unknown temporary names fail-closed", () => {
    const root = run();
    fs.writeFileSync(path.join(root, ".events.jsonl.compress-0-0123456789abcdef0123456789abcdef.tmp"), "bad", { mode: 0o600 });
    expect(canRemoveTerminalRun(root)).toBe(false);
  });
});

describe("compressed terminal run tail retention", () => {
  it.each([
    { name: "finishedAt", record: {} },
    { name: "directory-mtime fallback", record: { finishedAt: undefined } },
    { name: "final-answer receipt", record: { finishedAt: now, updatedAt: now, finalAnswerReceipt: { id: "receipt", recordedAt: 4 * HOUR } } },
  ])("bounds an aged compressed terminal run through public archive pruning to the same final 200 events ($name)", ({ record }) => {
    const { directory, file, bytes, encoded, lines } = fixture(record);
    expect(encoded.readUInt32LE(0)).toBe(0xfd2fb528);
    expect(encoded[4]! & 4).toBe(4);
    expect(bytes.length).toBeLessThan(256 * 1024);
    expect(readRunLogText(file)).toBe(bytes.toString());
    const status = fs.readFileSync(path.join(directory, "status.json"));
    const mtime = fs.statSync(directory).mtimeMs;
    const changes: unknown[] = [];
    expect(prune(directory, { onCompact: change => changes.push(change) })).toEqual([]);
    const expected = marker + lines.slice(-200).join("");
    expect(fs.readFileSync(file, "utf8")).toBe(expected);
    expect(readRunLogText(file)).toBe(expected);
    expect(expected.trimEnd().split("\n")).toHaveLength(201); // marker does not consume an event slot
    expect(fs.existsSync(`${file}.zst`)).toBe(false);
    expect(changes).toEqual([{ path: file, beforeBytes: encoded.length, afterBytes: Buffer.byteLength(expected) }]);
    expect(fs.statSync(directory).mtimeMs).toBe(mtime);
    expect(fs.readFileSync(path.join(directory, "status.json"))).toEqual(status);
    expect(fs.readFileSync(path.join(directory, "reply.json"), "utf8")).toBe('{"text":"retained result"}');
    const bounded = snapshot(directory);
    expect(prune(directory)).toEqual([]);
    expect(snapshot(directory)).toEqual(bounded);
    expect(prune(directory, { now: 4 * HOUR + 7 * 24 * HOUR })).toEqual([directory]);
  });

  it("enforces the 256-KiB cap on decoded bytes with the marker and only complete trailing events", () => {
    const { directory, file, lines, bytes } = fixture({}, events(500));
    const cap = 256 * 1024;
    expect(bytes.length).toBeGreaterThan(cap);
    const expectedLines: string[] = [];
    let length = Buffer.byteLength(marker);
    for (const line of lines.slice(-200).reverse()) {
      if (length + Buffer.byteLength(line) > cap) break;
      expectedLines.unshift(line); length += Buffer.byteLength(line);
    }
    expect(expectedLines.length).toBeGreaterThan(0);
    expect(expectedLines.length).toBeLessThan(200);
    expect(prune(directory)).toEqual([]);
    expect(readRunLogText(file)).toBe(marker + expectedLines.join(""));
    expect(fs.statSync(file).size).toBe(length);
    expect(length).toBeLessThanOrEqual(cap);
    expect(JSON.parse(expectedLines.at(-1)!)).toMatchObject({ sequence: 400 });
    expect(fs.existsSync(`${file}.zst`)).toBe(false);
  });

  it("keeps just the marker when a decoded final event exceeds the entire tail budget", () => {
    const { directory, file } = fixture({}, [JSON.stringify({ text: "x".repeat(300_000) }) + "\n"]);
    expect(prune(directory)).toEqual([]);
    expect(readRunLogText(file)).toBe(marker);
    expect(fs.existsSync(`${file}.zst`)).toBe(false);
  });

  it.each([1000, 1024])("preserves a bounded near-limit compressed log byte-for-byte (%i decoded bytes)", length => {
    const line = JSON.stringify({ text: "x".repeat(length - Buffer.byteLength('{"text":""}\n')) }) + "\n";
    const { directory, file } = fixture({}, [line]);
    const before = snapshot(directory), mtime = fs.statSync(directory).mtimeMs;
    expect(prune(directory, { terminalRunEventsMaxBytes: 1024 })).toEqual([]);
    expect(snapshot(directory)).toEqual(before);
    expect(fs.statSync(directory).mtimeMs).toBe(mtime);
    expect(fs.existsSync(file)).toBe(false);
  });

  it.each([
    { name: "young", record: { finishedAt: 4 * HOUR + 1 } },
    { name: "live", record: { sessionId: String(process.pid) } },
    { name: "running", record: { status: "running" } },
    { name: "cleanup debt", record: { cleanupPending: true } },
    { name: "unknown transport", record: { transport: "herdr" } },
    { name: "latest", record: {}, options: { latestRunId: "old" } },
    { name: "retained", record: {}, options: { isRetained: () => true } },
    { name: "dry-run", record: {}, options: { dryRun: true } },
  ])("leaves $name compressed runs untouched through public pruning", ({ record, options }) => {
    const { directory, file } = fixture(record);
    const before = snapshot(directory), mtime = fs.statSync(directory).mtimeMs;
    expect(prune(directory, options)).toEqual([]);
    expect(snapshot(directory)).toEqual(before);
    expect(fs.statSync(directory).mtimeMs).toBe(mtime);
    expect(fs.existsSync(file)).toBe(false);
  });

  it.each(["unresolved-worker.json", "archive-pending.json", "actor-run-archive-pending.json"])("keeps compressed history while %s debt remains", name => {
    const { directory, file } = fixture();
    fs.writeFileSync(path.join(directory, name), "{}", { mode: 0o600 });
    const before = snapshot(directory);
    expect(prune(directory)).toEqual([]);
    expect(snapshot(directory)).toEqual(before);
    expect(fs.existsSync(file)).toBe(false);
  });

  it("checks deadlines and a newly retained reference again before publishing", () => {
    for (const fence of ["deadline", "reference"] as const) {
      const { directory, file } = fixture();
      const before = snapshot(directory);
      let checks = 0;
      const options = fence === "deadline" ? { expired: () => ++checks > 1 } : { isRetained: () => ++checks > 1 };
      expect(compactTerminalRunEvents(directory, { now, ...options })).toBe(false);
      expect(snapshot(directory)).toEqual(before);
      expect(fs.existsSync(file)).toBe(false);
    }
  });

  it("compacts compressed events through the closed managed-root sweep before unchanged whole-run expiry", () => {
    const { root, directory, lines } = fixture();
    const managed = path.join(root, FABRIC_RUN_ROOT_PREFIX + "compressed");
    fs.mkdirSync(managed, { mode: 0o700 });
    const moved = path.join(managed, "old"); fs.renameSync(directory, moved);
    markRunRootActive(managed, 4 * HOUR); markRunRootClosed(managed, 4 * HOUR, true);
    expect(sweepTempRunRoots({ tempRoot: root, now, orphanedTempRunRetentionMs: 24 * HOUR, oneShotRunRetentionMs: 24 * HOUR })).toEqual({ removedRoots: [], removedRuns: [] });
    expect(readRunLogText(path.join(moved, "events.jsonl"))).toBe(marker + lines.slice(-200).join(""));
    expect(fs.existsSync(path.join(moved, "events.jsonl.zst"))).toBe(false);
  });

  it("publishes plain first, keeps both generations discoverable in the window, then unlinks the judged compressed source", () => {
    const { directory, file, bytes, encoded, lines } = fixture();
    const expected = marker + lines.slice(-200).join("");
    const oldSource = openRunLogSource(file);
    const oldDescriptor = fs.openSync(`${file}.zst`, "r");
    const link = fs.linkSync, unlink = fs.unlinkSync, sync = fs.fsyncSync;
    const order: string[] = [];
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      link(from, to);
      if (String(to) === file) {
        order.push("publish-plain");
        expect(fs.readFileSync(`${file}.zst`)).toEqual(encoded);
        expect(readRunLogText(file)).toBe(expected); // new readers choose the complete plain tail
      }
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      sync(fd);
      if (fs.fstatSync(fd).isDirectory() && fs.existsSync(file)) order.push("plain-directory-sync");
    });
    vi.spyOn(fs, "unlinkSync").mockImplementation(target => {
      if (String(target) === `${file}.zst` || path.basename(String(target)).startsWith("events.jsonl.zst.")) {
        order.push("unlink-compressed");
        expect(readRunLogText(file)).toBe(expected);
      }
      unlink(target);
    });
    try {
      expect(prune(directory)).toEqual([]);
      expect(order[0]).toBe("publish-plain");
      expect(order.at(-1)).toBe("unlink-compressed");
      if (process.platform !== "win32") expect(order).toContain("plain-directory-sync");
      expect(fs.existsSync(`${file}.zst`)).toBe(false);
      const heldBytes = Buffer.alloc(bytes.length);
      expect(oldSource.read(heldBytes, 0, heldBytes.length, 0)).toBe(bytes.length);
      expect(heldBytes).toEqual(bytes); // old logical generation is still complete after retirement
      const heldEncoded = Buffer.alloc(encoded.length);
      expect(fs.readSync(oldDescriptor, heldEncoded, 0, heldEncoded.length, 0)).toBe(encoded.length);
      expect(heldEncoded).toEqual(encoded); // so is a reader holding the old physical descriptor
    } finally { oldSource.close(); fs.closeSync(oldDescriptor); }
  });

  it("compacts a restored full plain copy and retires its matching compressed history", () => {
    const { directory, file, bytes, lines } = fixture();
    fs.writeFileSync(file, bytes, { mode: 0o600 });
    expect(prune(directory)).toEqual([]);
    expect(readRunLogText(file)).toBe(marker + lines.slice(-200).join(""));
    expect(fs.existsSync(`${file}.zst`)).toBe(false);
  });

  it.each(["directory-sync", "verified-removal"])("retries %s failure without rewriting the already published tail", fault => {
    const { directory, file, encoded, lines } = fixture();
    const expected = marker + lines.slice(-200).join("");
    if (fault === "directory-sync") {
      const sync = fs.fsyncSync;
      vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        if (fs.fstatSync(fd).isDirectory()) throw new Error("fixture directory sync failed");
        sync(fd);
      });
    } else verifiedRemoveTestHooks.beforeOpen = target => {
      if (target === `${file}.zst`) throw new Error("fixture removal refused");
    };
    // Off Linux the unchanged fallback has no beforeOpen hook; inject its unlink failure instead.
    if (fault === "verified-removal" && process.platform !== "linux") {
      const unlink = fs.unlinkSync;
      vi.spyOn(fs, "unlinkSync").mockImplementation(target => {
        if (String(target) === `${file}.zst`) throw new Error("fixture removal refused");
        unlink(target);
      });
    }
    // Windows has no directory barrier to fail; preserve the compressed source at its unlink instead.
    if (fault === "directory-sync" && process.platform === "win32") {
      const unlink = fs.unlinkSync;
      vi.spyOn(fs, "unlinkSync").mockImplementation(target => {
        if (String(target) === `${file}.zst`) throw new Error("fixture removal refused");
        unlink(target);
      });
    }
    expect(compactTerminalRunEvents(directory, { now })).toBe(true);
    expect(fs.readFileSync(file, "utf8")).toBe(expected);
    expect(fs.readFileSync(`${file}.zst`)).toEqual(encoded);
    const plain = fs.statSync(file);
    vi.restoreAllMocks(); delete verifiedRemoveTestHooks.beforeOpen;
    expect(compactTerminalRunEvents(directory, { now, dryRun: true })).toBe(true);
    expect(fs.readFileSync(`${file}.zst`)).toEqual(encoded);
    expect(compactTerminalRunEvents(directory, { now })).toBe(true);
    expect(fs.existsSync(`${file}.zst`)).toBe(false);
    const after = fs.statSync(file);
    expect([after.ino, after.mtimeMs, after.ctimeMs]).toEqual([plain.ino, plain.mtimeMs, plain.ctimeMs]);
    expect(readRunLogText(file)).toBe(expected);
  });

  it("does not retire compressed history merely because an unrelated bounded plain log is present", () => {
    const { directory, file, encoded } = fixture();
    fs.writeFileSync(file, '{"unrelated":true}\n', { mode: 0o600 });
    const before = snapshot(directory);
    expect(compactTerminalRunEvents(directory, { now })).toBe(false);
    expect(snapshot(directory)).toEqual(before);
    expect(fs.readFileSync(`${file}.zst`)).toEqual(encoded);
  });

  it("never deletes the compressed source on an EEXIST plain-publication collision", () => {
    const { directory, file, encoded } = fixture();
    const link = fs.linkSync;
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      if (String(to) === file) fs.writeFileSync(file, '{"collision":true}\n', { flag: "wx", mode: 0o600 });
      link(from, to);
    });
    expect(compactTerminalRunEvents(directory, { now })).toBe(false);
    expect(fs.readFileSync(file, "utf8")).toBe('{"collision":true}\n');
    expect(fs.readFileSync(`${file}.zst`)).toEqual(encoded);
    expect(fs.readdirSync(directory).sort()).toEqual(["events.jsonl", "events.jsonl.zst", "reply.json", "status.json"]);
  });

  it.each(["write", "zero-write", "file-sync"])("retains the complete compressed generation on a pre-publication %s failure", fault => {
    const { directory, file } = fixture();
    const before = snapshot(directory);
    if (fault === "write") vi.spyOn(fs, "writeSync").mockImplementation(() => { throw new Error("fixture ENOSPC"); });
    else if (fault === "zero-write") vi.spyOn(fs, "writeSync").mockReturnValue(0);
    else vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("fixture fsync EIO"); });
    expect(compactTerminalRunEvents(directory, { now })).toBe(false);
    expect(snapshot(directory)).toEqual(before);
    expect(fs.existsSync(file)).toBe(false);
  });

  it("completes partial tail writes before publishing or retiring the compressed source", () => {
    const { directory, file, lines } = fixture();
    const write = fs.writeSync;
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, bytes: Buffer, offset: number, length: number, position: number) =>
      write(fd, bytes, offset, Math.min(length, 7), position)) as typeof fs.writeSync);
    expect(compactTerminalRunEvents(directory, { now })).toBe(true);
    expect(readRunLogText(file)).toBe(marker + lines.slice(-200).join(""));
    expect(fs.existsSync(`${file}.zst`)).toBe(false);
  });

  it("refuses corrupt checksummed frames without publishing a plain replacement", () => {
    const { directory, file, encoded } = fixture();
    const bad = Buffer.from(encoded); bad[bad.length - 1] = bad[bad.length - 1]! ^ 1;
    fs.writeFileSync(`${file}.zst`, bad);
    const before = snapshot(directory);
    expect(compactTerminalRunEvents(directory, { now })).toBe(false);
    expect(snapshot(directory)).toEqual(before);
    expect(fs.existsSync(file)).toBe(false);
  });

  it("refuses an encoded source beyond the existing 128-MiB cap before allocating it", () => {
    const { directory, file } = fixture();
    const cap = 128 * 1024 * 1024;
    fs.truncateSync(`${file}.zst`, cap + 1); // Private sparse fixture, not a decoded 128-MiB allocation.
    const before = fs.statSync(`${file}.zst`);
    const allocate = vi.spyOn(Buffer, "alloc");
    expect(compactTerminalRunEvents(directory, { now })).toBe(false);
    expect(allocate.mock.calls.some(([size]) => size > cap)).toBe(false);
    const after = fs.statSync(`${file}.zst`);
    expect([after.ino, after.size, after.mtimeMs, after.ctimeMs]).toEqual([before.ino, before.size, before.mtimeMs, before.ctimeMs]);
    expect(fs.existsSync(file)).toBe(false);
  });

  it.skipIf(process.platform === "win32").each(["plain-link", "compressed-link", "hardlink", "writable-source", "public-directory"])("preserves unsafe %s custody without publication", kind => {
    const { root, directory, file, encoded } = fixture();
    if (kind === "plain-link") {
      const victim = path.join(root, "victim.jsonl");
      fs.writeFileSync(victim, '{"keep":true}\n', { mode: 0o600 });
      fs.symlinkSync(victim, file);
    } else if (kind === "compressed-link") {
      const source = path.join(root, "source.zst");
      fs.renameSync(`${file}.zst`, source); fs.symlinkSync(source, `${file}.zst`);
    } else if (kind === "hardlink") fs.linkSync(`${file}.zst`, path.join(root, "linked.zst"));
    else if (kind === "writable-source") fs.chmodSync(`${file}.zst`, 0o660);
    else fs.chmodSync(directory, 0o755);
    expect(compactTerminalRunEvents(directory, { now })).toBe(false);
    expect(fs.readFileSync(`${file}.zst`)).toEqual(encoded);
    if (kind === "plain-link") expect(fs.readFileSync(file, "utf8")).toBe('{"keep":true}\n');
    else expect(fs.existsSync(file)).toBe(false);
  });

  it.skipIf(process.platform !== "linux")("rechecks the judged compressed inode at the verified deletion boundary", () => {
    const { root, directory, file, encoded } = fixture();
    const replacement = frame(Buffer.from('{"replacement":true}\n'));
    let swapped = false;
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (target !== `${file}.zst`) return;
      swapped = true;
      fs.renameSync(target, path.join(root, "judged.zst"));
      fs.writeFileSync(target, replacement, { mode: 0o600 });
    };
    expect(compactTerminalRunEvents(directory, { now })).toBe(true);
    expect(swapped).toBe(true);
    expect(fs.readFileSync(`${file}.zst`)).toEqual(replacement);
    expect(fs.readFileSync(path.join(root, "judged.zst"))).toEqual(encoded);
    expect(readRunLogText(file).startsWith(marker)).toBe(true);
  });

  it.skipIf(process.platform !== "linux")("keeps a compressed source that gains a hardlink after judgment", () => {
    const { root, directory, file, encoded } = fixture();
    verifiedRemoveTestHooks.beforeOpen = target => {
      if (target === `${file}.zst`) fs.linkSync(target, path.join(root, "held-link.zst"));
    };
    expect(compactTerminalRunEvents(directory, { now })).toBe(true);
    expect(fs.readFileSync(`${file}.zst`)).toEqual(encoded);
    expect(fs.readFileSync(path.join(root, "held-link.zst"))).toEqual(encoded);
    expect(fs.statSync(`${file}.zst`).nlink).toBe(2);
  });
});
