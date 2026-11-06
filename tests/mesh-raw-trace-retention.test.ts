import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshArchive, currentBoot, type MeshArchiveEntry } from "../src/mesh/archive.js";
import { EventLog, type MeshEvent } from "../src/mesh/event-log.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

// Data-safety regressions on the actual locked repair/rollback/compaction paths.
// Parsed event history cannot stand in for the original generation's malformed bytes.
const fixtures: string[] = [];
const from = { id: "raw-trace-test", name: "raw trace test", kind: "main" as const };
const entry = (sequence: number): MeshArchiveEntry => {
  const event: MeshEvent = { id: `event-${sequence}`, sequence, topic: "raw.trace", kind: "message", from,
    text: "x".repeat(180), createdAt: Date.UTC(2026, 9, 27) };
  return { event, line: JSON.stringify(event) };
};
const bytesOf = (entries: MeshArchiveEntry[]) => Buffer.from(entries.map(item => `${item.line}\n`).join(""));
const malformed = Buffer.from([0x7b, 0x22, 0x74, 0x6f, 0x72, 0x6e, 0x22, 0x3a, 0xff, 0x00, 0xc3]);
const sha256 = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");

const setup = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-raw-trace-"));
  fixtures.push(base);
  const root = path.join(base, "mesh");
  const dir = path.join(base, "events-archive");
  fs.mkdirSync(root);
  fs.mkdirSync(dir);
  const lock = new MeshLock(root, {}, () => undefined);
  const log = new EventLog({ root, lock, maxEventBytes: 1_024, maxReadEvents: 100 }, {
    maxEventLogBytes: 2_048, retainedEventLogBytes: 1_025,
  });
  const archive = new MeshArchive(dir, root);
  return { root, dir, lock, log, archive, live: path.join(root, "events.jsonl"),
    liveCopies: path.join(root, "trace-archive"), archiveCopies: path.join(dir, "trace-archive") };
};

const copiedFiles = (directory: string): string[] => {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(item => {
    const file = path.join(directory, item.name);
    return item.isDirectory() ? copiedFiles(file) : item.isFile() ? [file] : [];
  });
};
const expectRawCopy = (directory: string, original: Buffer) => {
  const copies = copiedFiles(directory).filter(file => /\.(?:jsonl|ndjson)$/.test(file));
  const exact = copies.filter(file => fs.readFileSync(file).equals(original));
  expect(exact, "a byte-exact raw generation must survive separately from parsed events").not.toHaveLength(0);
  const receipts = fs.readFileSync(path.join(directory, "MANIFEST.jsonl"), "utf8").trim().split("\n")
    .map(line => JSON.parse(line) as { to: string; sha256: string; bytes: number });
  for (const file of exact) {
    expect(sha256(fs.readFileSync(file))).toBe(sha256(original));
    expect(receipts.find(receipt => receipt.to === file)).toMatchObject({
      sha256: sha256(original), bytes: original.length,
    });
  }
};
const blockCopies = (directory: string) => fs.writeFileSync(directory, "not a directory");
const missingCopyDirectory = (directory: string) => {
  const mkdir = fs.mkdirSync.bind(fs);
  vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, ...rest: unknown[]) => {
    if (String(target).startsWith(directory)) throw Object.assign(new Error("raw archive unavailable"), { code: "ENOENT" });
    return (mkdir as (...args: unknown[]) => unknown)(target, ...rest);
  }) as typeof fs.mkdirSync);
};
const archiveSeed = async (fixture: ReturnType<typeof setup>) => {
  await fixture.lock.withLock(() => {
    fixture.archive.catchUp([entry(1)]);
    fixture.archive.recovered(undefined);
  });
  return path.join(fixture.dir, "2026", "10", "27", "raw.trace.jsonl");
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const fixture of fixtures.splice(0)) fs.rmSync(fixture, { recursive: true, force: true });
});

describe("mesh raw trace retention at destructive byte boundaries", () => {
  it.each([
    ["unterminated JSON", Buffer.from(entry(2).line), false],
    ["malformed non-UTF8 tail", malformed, false],
    ["entirely unterminated file", malformed, true],
  ] as const)("preserves the live original before repairing %s", async (_name, tail, noCompleteLine) => {
    const fixture = setup();
    const prefix = noCompleteLine ? Buffer.alloc(0) : bytesOf([entry(1)]);
    const original = Buffer.concat([prefix, tail]);
    fs.writeFileSync(fixture.live, original);
    const published = await fixture.log.publish({ topic: "raw.trace", from, text: "after repair" });
    expect(fs.readFileSync(fixture.live)).toEqual(Buffer.concat([prefix, Buffer.from(`${JSON.stringify(published)}\n`)]));
    expect(fixture.log.read({ after: 0 }).map(event => event.id)).not.toContain("event-2");
    expectRawCopy(fixture.liveCopies, original);
  });

  it.each(["blocked-directory", "ENOENT"])("refuses live repair without losing its input when archiving fails (%s)", async failure => {
    const fixture = setup();
    const original = Buffer.concat([bytesOf([entry(1)]), malformed]);
    fs.writeFileSync(fixture.live, original);
    if (failure === "blocked-directory") blockCopies(fixture.liveCopies);
    else missingCopyDirectory(fixture.liveCopies);
    await expect(fixture.log.publish({ topic: "raw.trace", from, text: "must not append" })).rejects.toThrow();
    expect(fs.readFileSync(fixture.live)).toEqual(original);
    expect(fs.existsSync(path.join(fixture.root, "sequence"))).toBe(false);
  });

  it("does not archive a healthy newline-terminated live log below the compaction threshold", async () => {
    const fixture = setup();
    fs.writeFileSync(fixture.live, bytesOf([entry(1)]));
    const published = await fixture.log.publish({ topic: "raw.trace", from, text: "healthy" });
    expect(fixture.log.read({ after: 0 }).map(event => event.sequence)).toEqual([1, published.sequence]);
    expect(copiedFiles(fixture.liveCopies)).toEqual([]);
  });

  it.each([false, true])("archives the complete live generation before compaction (event archive enabled: %s)", async enabled => {
    const fixture = setup();
    if (enabled) fs.writeFileSync(path.join(fixture.root, "event-archive.json"), JSON.stringify({ version: 1, dir: fixture.dir }));
    const original = Buffer.concat([bytesOf([entry(1), entry(2), entry(3), entry(4), entry(5), entry(6)]), malformed, Buffer.from("\n")]);
    fs.writeFileSync(fixture.live, original);
    const published = await fixture.log.publish({ topic: "raw.trace", from, text: "trigger compaction" });
    const generation = Buffer.concat([original, Buffer.from(`${JSON.stringify(published)}\n`)]);
    expect(fs.readFileSync(fixture.live).length).toBeLessThan(generation.length);
    expect(JSON.parse(fs.readFileSync(path.join(fixture.root, "generation"), "utf8"))).toBe(1);
    expect(fixture.log.read({ after: 0 }).at(-1)).toEqual(published);
    expectRawCopy(fixture.liveCopies, generation);
  });

  it("keeps the live generation and inode when its pre-compaction archive fails", async () => {
    const fixture = setup();
    const original = bytesOf([entry(1), entry(2), entry(3), entry(4), entry(5), entry(6)]);
    fs.writeFileSync(fixture.live, original);
    const before = fs.statSync(fixture.live);
    blockCopies(fixture.liveCopies);
    await expect(fixture.log.publish({ topic: "raw.trace", from, text: "append survives failed compaction" })).rejects.toThrow();
    const after = fs.readFileSync(fixture.live);
    expect(after.subarray(0, original.length)).toEqual(original);
    expect(after.length).toBeGreaterThan(original.length); // No change to append-before-compaction semantics.
    expect(fs.statSync(fixture.live).ino).toBe(before.ino);
    expect(fs.existsSync(path.join(fixture.root, "generation"))).toBe(false);
  });

  it.each(["begin", "catchUp"] as const)("preserves a malformed archive tail before %s repairs it", async operation => {
    const fixture = setup();
    const file = await archiveSeed(fixture);
    fs.appendFileSync(file, malformed);
    const original = fs.readFileSync(file);
    await fixture.lock.withLock(() => {
      if (operation === "begin") fixture.archive.commit(fixture.archive.begin(entry(2)));
      else fixture.archive.catchUp([entry(2)]);
    });
    expect(fs.readFileSync(file)).toEqual(bytesOf([entry(1), entry(2)]));
    expect(fixture.archive.readAfter(0, 2, () => true, 10).map(event => event.sequence)).toEqual([1, 2]);
    expectRawCopy(fixture.archiveCopies, original);
  });

  it.each(["rollback", "same-boot recovery"] as const)("keeps original bytes while %s removes an unpublished event from delivery", async operation => {
    const fixture = setup();
    const file = await archiveSeed(fixture);
    const pending = await fixture.lock.withLock(() => fixture.archive.begin(entry(2)));
    const original = fs.readFileSync(file);
    await fixture.lock.withLock(() => {
      if (operation === "rollback") fixture.archive.rollback(pending);
      else expect(fixture.archive.recover(1)).toEqual({ rebooted: false, promote: [] });
    });
    expect(fs.readFileSync(file)).toEqual(bytesOf([entry(1)]));
    expect(fixture.archive.readAfter(0, 2, () => true, 10).map(event => event.sequence)).toEqual([1]);
    expect(fixture.archive.lookup(2)).toBeUndefined();
    expect(fs.existsSync(path.join(fixture.dir, "PENDING.json"))).toBe(false);
    expectRawCopy(fixture.archiveCopies, original);
  });

  it("preserves reboot recovery's malformed suffix without changing promotion of complete rows", async () => {
    const fixture = setup();
    const file = await archiveSeed(fixture);
    await fixture.lock.withLock(() => fixture.archive.begin(entry(2)));
    fs.appendFileSync(file, malformed);
    fs.writeFileSync(path.join(fixture.dir, "BOOT"), "earlier-boot");
    const original = fs.readFileSync(file);
    const plan = fixture.archive.prepareRecovery(1);
    const recovered = await fixture.lock.withLock(() => fixture.archive.recover(1, plan));
    expect(recovered.rebooted).toBe(true);
    expect(recovered.promote.map(item => item.event)).toEqual([entry(2).event]);
    expect(fs.readFileSync(file)).toEqual(bytesOf([entry(1), entry(2)]));
    expectRawCopy(fixture.archiveCopies, original);
  });

  it.each((["rollback", "same-boot recovery", "reboot recovery", "begin", "catchUp"] as const)
    .flatMap(operation => (["blocked-directory", "ENOENT"] as const).map(failure => [operation, failure] as const)))
  ("keeps the archive input intact when copying fails during %s (%s)", async (operation, failure) => {
    const fixture = setup();
    const file = await archiveSeed(fixture);
    const pending = await fixture.lock.withLock(() => fixture.archive.begin(entry(2)));
    if (operation === "begin" || operation === "catchUp" || operation === "reboot recovery") fs.appendFileSync(file, malformed);
    if (operation === "reboot recovery") fs.writeFileSync(path.join(fixture.dir, "BOOT"), "earlier-boot");
    const original = fs.readFileSync(file);
    const marker = fs.readFileSync(path.join(fixture.dir, "PENDING.json"));
    const index = fs.readFileSync(path.join(fixture.dir, "sequence-index", "0", "2.json"));
    const plan = operation === "reboot recovery" ? fixture.archive.prepareRecovery(1) : undefined;
    if (failure === "blocked-directory") blockCopies(fixture.archiveCopies);
    else missingCopyDirectory(fixture.archiveCopies);
    await expect(fixture.lock.withLock(() => {
      if (operation === "rollback") fixture.archive.rollback(pending);
      else if (operation === "same-boot recovery" || operation === "reboot recovery") fixture.archive.recover(1, plan);
      else if (operation === "begin") fixture.archive.begin(entry(3));
      else fixture.archive.catchUp([entry(3)]);
    })).rejects.toThrow();
    expect(fs.readFileSync(file)).toEqual(original);
    expect(fs.readFileSync(path.join(fixture.dir, "PENDING.json"))).toEqual(marker);
    expect(fs.readFileSync(path.join(fixture.dir, "sequence-index", "0", "2.json"))).toEqual(index);
  });

  it.each(["live repair", "live compaction", "archive rollback"] as const)
  ("refuses %s when the independent copy readback hashes different bytes", async operation => {
    const fixture = setup();
    let source: string;
    let mutate: () => Promise<unknown>;
    if (operation === "archive rollback") {
      source = await archiveSeed(fixture);
      const pending = await fixture.lock.withLock(() => fixture.archive.begin(entry(2)));
      mutate = () => fixture.lock.withLock(() => fixture.archive.rollback(pending));
    } else {
      source = fixture.live;
      fs.writeFileSync(source, operation === "live repair"
        ? Buffer.concat([bytesOf([entry(1)]), malformed])
        : bytesOf([entry(1), entry(2), entry(3), entry(4), entry(5), entry(6)]));
      mutate = () => fixture.log.publish({ topic: "raw.trace", from, text: "readback failure" });
    }
    const original = fs.readFileSync(source);
    const copies = operation === "archive rollback" ? fixture.archiveCopies : fixture.liveCopies;
    const open = fs.openSync.bind(fs);
    const read = fs.readSync.bind(fs);
    const readbackDescriptors = new Set<number>();
    let corrupted = false;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: string | number, ...rest: unknown[]) => {
      const fd = (open as (...args: unknown[]) => number)(file, flags, ...rest);
      if (String(file).startsWith(copies + path.sep) && String(file).endsWith(".jsonl") &&
          typeof flags === "number" && !(flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR))) readbackDescriptors.add(fd);
      else readbackDescriptors.delete(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "readSync").mockImplementation(((...args: unknown[]) => {
      const count = (read as (...args: unknown[]) => number)(...args);
      const [fd, buffer, offset] = args as [number, Buffer, number];
      if (readbackDescriptors.has(fd) && Buffer.isBuffer(buffer) && count > 0) {
        buffer[offset] = buffer[offset]! ^ 1;
        corrupted = true;
      }
      return count;
    }) as typeof fs.readSync);
    await expect(mutate()).rejects.toThrow("readback verification failed");
    expect(corrupted).toBe(true);
    const after = fs.readFileSync(source);
    expect(after.subarray(0, original.length)).toEqual(original);
    if (operation !== "live compaction") expect(after).toEqual(original);
    expect(fs.existsSync(path.join(fixture.root, "generation"))).toBe(false);
  });

  it("does not copy or truncate a healthy no-op reboot recovery", async () => {
    const fixture = setup();
    const file = await archiveSeed(fixture);
    await fixture.lock.withLock(() => fixture.archive.begin(entry(2)));
    const original = fs.readFileSync(file);
    fs.writeFileSync(path.join(fixture.dir, "BOOT"), "earlier-boot");
    const plan = fixture.archive.prepareRecovery(1);
    await fixture.lock.withLock(() => fixture.archive.recover(1, plan));
    expect(fs.readFileSync(file)).toEqual(original);
    expect(copiedFiles(fixture.archiveCopies)).toEqual([]);
    fs.writeFileSync(path.join(fixture.dir, "BOOT"), currentBoot());
  });
});
