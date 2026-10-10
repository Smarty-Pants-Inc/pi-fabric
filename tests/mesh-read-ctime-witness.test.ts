import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { stateReadIdentity } from "../src/mesh/read-journal.js";

// smarty-dev#4250 (pi-fabric#560): an authoritative read binds a followed terminal endpoint to
// state.json through the writer's kernel ctime witness instead of re-hashing the payload. Any
// tuple mismatch (a replacement, an in-place rewrite with the mtime restored) falls back to the
// full hash and serves the canonical bytes.
const roots: string[] = [];
const identity = { id: "session:writer", name: "writer", kind: "main" as const };
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const fixture = async () => {
  if (process.platform === "win32") return undefined;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "read-ctime-witness-")); roots.push(root);
  const writer = new MeshStore(root, 256 * 1024, 1000);
  // ~1 MB canonical payload, so a whole-file read or hash is unmistakable.
  await writer.writeBatch({ identity, ops: Array.from({ length: 100 }, (_, i) => ({ kind: "put" as const,
    key: `bulk/${String(i).padStart(3, "0")}`, value: "x".repeat(10_000) })) });
  await writer.put({ key: "field/owner", value: "old-host", identity });
  const file = path.join(root, "state.json"), journal = path.join(root, "state.read-journal.jsonl");
  const signal = path.join(root, "state.read-signal.json");
  const reader = new MeshStore(root, 256 * 1024, 1000);
  expect(reader.get("field/owner")?.value).toBe("old-host"); // a verified anchor
  // Every byte read from a descriptor opened on state.json, plus whole-file reads of it.
  const open = fs.openSync.bind(fs);
  const stateFds = new Set<number>();
  vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    const fd = (open as (...args: unknown[]) => number)(target, ...rest);
    if (path.resolve(String(target)) === file) stateFds.add(fd); else stateFds.delete(fd);
    return fd;
  }) as typeof fs.openSync);
  const close = fs.closeSync.bind(fs);
  vi.spyOn(fs, "closeSync").mockImplementation((fd: number) => { stateFds.delete(fd); close(fd); });
  // Attributed at call time: a descriptor counts as state.json only while it is open on it.
  const read = fs.readSync.bind(fs);
  let descriptorReads = 0, descriptorBytes = 0;
  vi.spyOn(fs, "readSync").mockImplementation(((fd: number, ...rest: unknown[]) => {
    const count = (read as (...args: unknown[]) => number)(fd, ...rest);
    if (stateFds.has(fd)) { descriptorReads++; descriptorBytes += count; }
    return count;
  }) as typeof fs.readSync);
  const readFile = vi.spyOn(fs, "readFileSync");
  const stateReads = () => descriptorReads + readFile.mock.calls.filter(([target]) => String(target) === file).length;
  const stateBytes = () => descriptorBytes;
  const parses = () => readFile.mock.calls.filter(([target]) => String(target) === file).length;
  const rows = () => fs.readFileSync(journal, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
  const witness = () => JSON.parse(fs.readFileSync(signal, "utf8")).witness as Record<string, string> | undefined;
  const tuple = (stat: fs.BigIntStats) => ({ dev: String(stat.dev), ino: String(stat.ino), size: String(stat.size),
    ctimeNs: String(stat.ctimeNs), mtimeNs: String(stat.mtimeNs) });
  return { root, writer, reader, file, journal, signal, stateReads, stateBytes, parses, rows, witness, tuple };
};
type Fixture = NonNullable<Awaited<ReturnType<typeof fixture>>>;

// The copied-marker forger of the P2 review: it also rewrites the journal's terminal identity and
// stamp to the replaced file (outer checksum resealed). Only the kernel witness stands between.
const forgeTerminal = (f: Fixture) => {
  const rows = f.rows(), terminal = rows.at(-1)!, stat = fs.statSync(f.file);
  terminal.delta.identity = stateReadIdentity(f.file);
  terminal.delta.stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
  terminal.checksum = createHash("sha256").update(JSON.stringify(terminal.delta)).digest("hex");
  fs.writeFileSync(f.journal, rows.map(row => JSON.stringify(row)).join("\n") + "\n");
};
// Same length, so size cannot give the rewrite away.
const alter = (bytes: Buffer) => Buffer.from(bytes.toString("utf8").replace('"value":"old-host"', '"value":"new-host"'), "utf8");

describe.skipIf(process.platform === "win32")("kernel ctime witness for authoritative reads (smarty-dev#4250)", () => {
  it("accepts an unchanged file with zero state.json content reads", async () => {
    const f = (await fixture())!;
    for (let n = 1; n <= 5; n++) {
      await f.writer.put({ key: "field/heartbeat", value: n, identity });
      // The writer recorded exactly the renamed file's kernel tuple.
      expect(f.witness()).toMatchObject(f.tuple(fs.statSync(f.file, { bigint: true })));
      const reads = f.stateReads();
      expect(f.reader.get("field/heartbeat")?.value).toBe(n);       // ordinary = authoritative
      const listed = f.reader.listAll("field/");
      expect(f.stateReads()).toBe(reads); // zero: no header, no payload, no parse
      expect(listed).toEqual(new MeshStore(f.root, 256 * 1024, 1000).listAll("field/")); // = a canonical parse
      await f.writer.put({ key: "field/heartbeat", value: n + 0.5, identity });
      const authoritative = f.stateReads(); // after the writer, which reads for its own commit
      expect(f.reader.get("field/heartbeat")?.value).toBe(n + 0.5);
      expect(f.reader.get("field/heartbeat", { fresh: true })?.value).toBe(n + 0.5);
      expect(f.stateReads()).toBe(authoritative); // zero: no header, no payload, no parse
    }
  });

  it.each(["copied bytes", "altered bytes"])("detects a forged replacement (%s) with the old mtime restored and serves the canonical state", async variant => {
    const f = (await fixture())!;
    await f.writer.put({ key: "field/heartbeat", value: 1, identity });
    const old = fs.statSync(f.file, { bigint: true }), oldMs = fs.statSync(f.file);
    const bytes = fs.readFileSync(f.file);
    const forged = f.file + ".forged";
    fs.writeFileSync(forged, variant === "copied bytes" ? bytes : alter(bytes));
    fs.utimesSync(forged, oldMs.atime, oldMs.mtime);
    fs.renameSync(forged, f.file);
    forgeTerminal(f);
    const now = fs.statSync(f.file, { bigint: true });
    expect(String(now.size)).toBe(String(old.size));
    expect(now.ino).not.toBe(old.ino);                       // a replacement is a new inode
    expect(f.witness()).not.toMatchObject(f.tuple(now));     // the tuple differs
    const bytesBefore = f.stateBytes(), parses = f.parses();
    const expected = variant === "copied bytes" ? "old-host" : "new-host";
    expect(f.reader.get("field/owner")?.value).toBe(expected);
    expect(f.reader.get("field/heartbeat")?.value).toBe(1);
    // Fell back to the full payload hash (copied bytes verify; altered bytes fail it and parse).
    expect(f.stateBytes() - bytesBefore).toBeGreaterThan(bytes.length - 1024);
    expect(f.parses() - parses).toBe(variant === "copied bytes" ? 0 : 1);
    expect(new MeshStore(f.root, 256 * 1024, 1000).get("field/owner")?.value).toBe(expected);
  });

  it("detects an in-place rewrite with the mtime restored (ctime changed) and serves the canonical state", async () => {
    const f = (await fixture())!;
    await f.writer.put({ key: "field/heartbeat", value: 1, identity });
    const old = fs.statSync(f.file, { bigint: true }), oldMs = fs.statSync(f.file);
    const altered = alter(fs.readFileSync(f.file));
    await sleep(20); // past one coarse timer tick (see the granularity limit in read-journal.ts)
    const fd = fs.openSync(f.file, "r+");
    try { fs.writeSync(fd, altered, 0, altered.length, 0); } finally { fs.closeSync(fd); }
    fs.utimesSync(f.file, oldMs.atime, oldMs.mtime);
    forgeTerminal(f);
    const now = fs.statSync(f.file, { bigint: true });
    expect([now.ino, now.size]).toEqual([old.ino, old.size]);  // same inode, same size
    expect(now.ctimeNs).not.toBe(old.ctimeNs);                  // but the kernel moved ctime
    expect(f.witness()).not.toMatchObject(f.tuple(now));
    const parses = f.parses();
    expect(f.reader.get("field/owner")?.value).toBe("new-host");
    expect(f.parses()).toBe(parses + 1);
    expect(f.reader.listAll("field/").find(entry => entry.key === "field/owner")?.value).toBe("new-host");
  });

  it.each(["missing", "old-format", "other payload hash"])("falls back to the payload hash on a %s witness", async kind => {
    const f = (await fixture())!;
    await f.writer.put({ key: "field/heartbeat", value: 1, identity });
    const signal = JSON.parse(fs.readFileSync(f.signal, "utf8"));
    if (kind === "missing") fs.rmSync(f.signal);
    else {
      if (kind === "old-format") delete signal.witness;
      else signal.witness.payloadHash = "0".repeat(64);
      fs.writeFileSync(f.signal, JSON.stringify(signal));
    }
    const before = f.stateBytes(), parses = f.parses();
    expect(f.reader.get("field/heartbeat")?.value).toBe(1);
    expect(f.stateBytes() - before).toBeGreaterThan(fs.statSync(f.file).size - 1024);
    expect(f.parses()).toBe(parses); // the hash verified: still a journal follow
  });
});
