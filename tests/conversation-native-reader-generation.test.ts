import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";

const directories: string[] = [];
const workspace = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "reader-generation-"));
  directories.push(directory);
  return directory;
};
const jsonl = (records: unknown[]) => records.map((record) => JSON.stringify(record) + "\n").join("");
// Node's overloaded readSync type exposes the options overload to vi.spyOn;
// runtime source reads use the explicit five-argument positional overload.
const readPositions = (reads: { mock: { calls: unknown[][] } }) => reads.mock.calls.map((call) => call[4]);
const message = (i: number, generation: string, padding: number) => ({ role: "user", content: `${generation}-${i}:` + "x".repeat(padding), timestamp: i });
const records = (kind: "session" | "events", generation: string, padding: number, count = 150) => [
  ...(kind === "session" ? [{ type: "session", id: "session" }] : []),
  ...Array.from({ length: count }, (_, i) => kind === "events"
    ? { type: "message_end", message: message(i, generation, padding) }
    : { type: "message", id: `m${i}`, parentId: i ? `m${i - 1}` : null, timestamp: "2026-01-01T00:00:00.000Z", message: message(i, generation, padding) }),
];

// Model genuine reuse independently of the host allocator: the real newly
// opened descriptor supplies bytes/size/isFile, but its exact dev/ino are the
// same across generations. Number-only callers also see collapsed Windows IDs.
const metadata = (current: () => { ino: bigint; birth: bigint; mtime: bigint }, classification = false) => {
  const project = <T extends fs.Stats | fs.BigIntStats>(stat: T, exact: boolean | undefined): T => {
    const value = current();
    return new Proxy(stat, { get(target, key) {
      if (key === "ino") return exact ? value.ino : Number(value.ino);
      if (key === "dev") return exact ? 7n : 7;
      if (key === "birthtimeNs") return value.birth;
      if (key === "mtimeNs") return value.mtime;
      if (key === "birthtimeMs") return exact ? value.birth / 1000000n : Number(value.birth) / 1000000;
      if (key === "mtimeMs") return exact ? value.mtime / 1000000n : Number(value.mtime) / 1000000;
      return Reflect.get(target, key);
    } });
  };
  const real = fs.fstatSync.bind(fs);
  const descriptorStats = vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) => {
    const stat = options?.bigint ? real(fd, { bigint: true }) : real(fd);
    return (new Error().stack ?? "").includes("openDescriptor") ? project(stat, options?.bigint) : stat;
  }) as typeof fs.fstatSync);
  if (classification) {
    const realPath = fs.lstatSync.bind(fs);
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: { bigint?: boolean }) => {
      const stat = options?.bigint ? realPath(file, { bigint: true }) : realPath(file);
      return (new Error().stack ?? "").includes("classifyLog") ? project(stat, options?.bigint) : stat;
    }) as typeof fs.lstatSync);
  }
  return descriptorStats;
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

const cases = (["session", "events"] as const).flatMap((kind) =>
  (["same", "shrink", "grow"] as const).flatMap((size) =>
    (["readPinned", "readFollow", "loadNewer", "loadOlder"] as const).flatMap((api) =>
      [false, true].map((suspended) => ({ kind, size, api, suspended })))));

describe("native reader descriptor generations", () => {
  it.each(cases)("relocates same dev/ino $kind $size replacement via $api (suspended=$suspended)", ({ kind, size, api, suspended }) => {
    const file = path.join(workspace(), `${kind}.jsonl`);
    const input = { id: "reader", status: "running", ...(kind === "session" ? { sessionFile: file } : { eventsFile: file }) };
    let generation = 1n;
    metadata(() => ({ ino: 15199648742414488n, birth: generation * 1000000n, mtime: generation * 1000000n }));
    fs.writeFileSync(file, jsonl(records(kind, "old", 5000)));
    const reader = new NativeConversationReader();
    reader.read(input, false);
    const before = reader.loadOlder()!;
    expect(before.hasMore).toBe(true);
    fs.appendFileSync(file, jsonl(records(kind, "old", 5000, 152).slice(-2)));
    expect(reader.read(input, false).hasNewer).toBe(true);
    if (suspended) expect(reader.suspend()).toBe(true);
    const padding = size === "shrink" ? 4900 : size === "grow" ? 5100 : 5000;
    // Two genuinely unread arrivals belong after the relocated pinned fence.
    const replacement = records(kind, "new", padding, 152);
    fs.writeFileSync(`${file}.new`, jsonl(replacement));
    fs.renameSync(`${file}.new`, file);
    generation = 2n;
    const after = api === "loadOlder" ? reader.loadOlder()! : api === "loadNewer" ? reader.loadNewer()! : reader.read(input, api === "readFollow");
    const following = api === "readFollow" || api === "loadNewer";
    // Whole backward pages may include extra older records after relocation;
    // every previously loaded logical record must still be present and fresh.
    const covered = after.messages.filter((item) => item.timestamp >= before.messages[0]!.timestamp && item.timestamp < 150);
    expect(covered.map((item) => item.timestamp)).toEqual(before.messages.map((item) => item.timestamp));
    expect(covered).toEqual(before.messages.map((item) => message(item.timestamp, "new", padding)));
    expect(after.messages).toEqual(after.messages.map((item) => message(item.timestamp, "new", padding)));
    expect(JSON.stringify(after.messages)).not.toContain("old-");
    expect(after.messages.at(-1)?.timestamp).toBe(following ? 151 : 149);
    expect(after.hasNewer).toBe(!following);
    if (kind === "session") expect(after.leafId).toBe(following ? "m151" : "m149");
    const newer = reader.loadNewer()!;
    expect(newer.messages.filter((item) => item.timestamp >= 150)).toEqual([message(150, "new", padding), message(151, "new", padding)]);
    expect(reader.loadNewer()!.messages).toEqual(newer.messages);
    expect(reader.loadOlder(3)!.messages).toEqual(Array.from({ length: 152 }, (_, i) => message(i, "new", padding)));
    reader.clear();
  });

  it.each(["same", "shrink"] as const)("checks content for same-birth same-ID %s rewrites", (size) => {
    const file = path.join(workspace(), "events.jsonl");
    let changed = false;
    metadata(() => ({ ino: 15199648742414488n, birth: 1000000n, mtime: changed ? 2000000n : 1000000n }));
    fs.writeFileSync(file, jsonl(records("events", "old", 16, 1)));
    const input = { id: "reader", status: "running", eventsFile: file };
    const reader = new NativeConversationReader();
    reader.read(input, false);
    fs.writeFileSync(file, jsonl(records("events", "new", size === "shrink" ? 8 : 16, 1)));
    changed = true;
    expect(reader.read(input, false).messages).toEqual([message(0, "new", size === "shrink" ? 8 : 16)]);
    reader.clear();
  });

  it("distinguishes adjacent Windows IDs even when Number and timestamps collapse", () => {
    const file = path.join(workspace(), "events.jsonl");
    let ino = 15199648742414488n;
    expect(Number(ino)).toBe(Number(ino + 1n));
    metadata(() => ({ ino, birth: 1000000n, mtime: 1000000n }));
    fs.writeFileSync(file, jsonl(records("events", "old", 16, 1)));
    const input = { id: "reader", status: "running", eventsFile: file };
    const reader = new NativeConversationReader();
    reader.read(input, false);
    fs.writeFileSync(`${file}.new`, jsonl(records("events", "new", 16, 1)));
    fs.renameSync(`${file}.new`, file);
    ino++;
    expect(reader.read(input, false).messages).toEqual([message(0, "new", 16)]);
    reader.clear();
  });

  it.each(["numeric alias", "reused generation"] as const)("reclassifies public logFile after %s", (variant) => {
    const file = path.join(workspace(), "log.jsonl");
    let changed = false;
    metadata(() => ({ ino: 15199648742414488n + (changed && variant === "numeric alias" ? 1n : 0n),
      birth: changed && variant === "reused generation" ? 2000000n : 1000000n, mtime: 1000000n }), true);
    fs.writeFileSync(file, jsonl(records("events", "old", 32, 1)));
    const input = { id: "reader", status: "running", logFile: file };
    const reader = new NativeConversationReader();
    const before = reader.read(input, false);
    expect(before.eventsFile).toBe(file);
    expect(before.sessionFile).toBeUndefined();
    fs.writeFileSync(`${file}.new`, jsonl(records("session", "new", 16, 1)));
    expect(fs.statSync(`${file}.new`).size).toBeGreaterThan(fs.statSync(file).size);
    fs.renameSync(`${file}.new`, file);
    changed = true;
    const after = reader.read(input, false);
    expect(after.sessionFile).toBe(file);
    expect(after.eventsFile).toBeUndefined();
    expect(after.messages).toEqual([message(0, "new", 16)]);
    expect(after.leafId).toBe("m0");
    expect(after.error).toBeUndefined();
    reader.clear();
  });

  it("uses size evidence even when IDs and all timestamps are unchanged", () => {
    const file = path.join(workspace(), "events.jsonl");
    metadata(() => ({ ino: 15199648742414488n, birth: 1000000n, mtime: 1000000n }));
    fs.writeFileSync(file, jsonl(records("events", "old", 32, 1)));
    const input = { id: "reader", status: "running", eventsFile: file };
    const reader = new NativeConversationReader();
    reader.read(input, false);
    fs.writeFileSync(file, jsonl(records("events", "new", 16, 1)));
    expect(reader.read(input, false).messages).toEqual([message(0, "new", 16)]);
    reader.clear();
  });

  it("verifies growing same-ID content when creation time is unavailable", () => {
    const file = path.join(workspace(), "events.jsonl");
    let mtime = 1000000n;
    metadata(() => ({ ino: 15199648742414488n, birth: 0n, mtime }));
    fs.writeFileSync(file, jsonl(records("events", "old", 16, 1)));
    const input = { id: "reader", status: "running", eventsFile: file };
    const reader = new NativeConversationReader();
    reader.read(input, false);
    fs.writeFileSync(file, jsonl(records("events", "new", 32, 2)));
    mtime++;
    const pinned = reader.read(input, false);
    expect(pinned.messages).toEqual([message(0, "new", 32)]);
    expect(pinned.hasNewer).toBe(true);
    expect(reader.loadNewer()!.messages).toEqual([message(0, "new", 32), message(1, "new", 32)]);
    reader.clear();
  });

  it("verifies identical-prefix zero-birth growth then appends safely", () => {
    const file = path.join(workspace(), "events.jsonl");
    let mtime = 1000000n;
    metadata(() => ({ ino: 15199648742414488n, birth: 0n, mtime }));
    const initial = jsonl(records("events", "new", 16, 1));
    fs.writeFileSync(file, initial);
    const input = { id: "reader", status: "running", eventsFile: file };
    const reader = new NativeConversationReader();
    const before = reader.read(input, false);
    const reads = vi.spyOn(fs, "readSync");
    fs.appendFileSync(file, jsonl(records("events", "new", 16, 2).slice(-1)));
    mtime++;
    const pinned = reader.read(input, false);
    expect(pinned.messages).toBe(before.messages);
    expect(pinned.hasNewer).toBe(true);
    expect(reads).toHaveBeenCalledTimes(1); // Loaded-page proof, no suffix IO.
    expect(readPositions(reads)[0]).toBe(0);
    reads.mockClear();
    expect(reader.loadNewer()!.messages).toEqual([message(0, "new", 16), message(1, "new", 16)]);
    expect(reads).toHaveBeenCalledTimes(1);
    expect(readPositions(reads)[0]).toBe(Buffer.byteLength(initial));
    reads.mockClear();
    reader.read(input);
    reader.loadNewer();
    expect(reads).not.toHaveBeenCalled();
    reader.clear();
  });

  it("reads only the append page and no settled or pinned source bytes", () => {
    const file = path.join(workspace(), "events.jsonl");
    let mtime = 1000000n;
    metadata(() => ({ ino: 15199648742414488n, birth: 1000000n, mtime }));
    const initial = jsonl(records("events", "new", 16, 1));
    fs.writeFileSync(file, initial);
    const input = { id: "reader", status: "running", eventsFile: file };
    const reader = new NativeConversationReader();
    const before = reader.read(input);
    const reads = vi.spyOn(fs, "readSync");
    fs.appendFileSync(file, jsonl(records("events", "new", 16, 2).slice(-1)));
    mtime++;
    expect(reader.read(input, false).messages).toBe(before.messages);
    expect(reads).not.toHaveBeenCalled();
    expect(reader.loadNewer()!.messages).toEqual([message(0, "new", 16), message(1, "new", 16)]);
    expect(reads).toHaveBeenCalledTimes(1);
    expect(readPositions(reads)[0]).toBe(Buffer.byteLength(initial));
    const fd = reads.mock.calls[0]![0];
    expect(() => fs.fstatSync(fd)).toThrow();
    reads.mockClear();
    reader.read(input);
    reader.loadNewer();
    expect(reads).not.toHaveBeenCalled();
    reader.clear();
  });

  it("closes the admitted descriptor when exact metadata acquisition fails", () => {
    const file = path.join(workspace(), "events.jsonl");
    fs.writeFileSync(file, jsonl(records("events", "new", 16, 1)));
    const real = fs.fstatSync.bind(fs);
    const admitted: number[] = [];
    vi.spyOn(fs, "fstatSync").mockImplementation(((...args: Parameters<typeof fs.fstatSync>) => {
      if ((new Error().stack ?? "").includes("openDescriptor")) {
        admitted.push(args[0]);
        throw Object.assign(new Error("metadata EIO"), { code: "EIO" });
      }
      return real(...args);
    }) as typeof fs.fstatSync);
    const reader = new NativeConversationReader();
    expect(reader.read({ id: "reader", status: "running", eventsFile: file }).unavailable?.eventsFile).toBe(true);
    expect(admitted.length).toBeGreaterThan(0);
    for (const fd of admitted) expect(() => real(fd)).toThrow();
    reader.clear();
  });

  it.each([false, true])("rejects ambiguous reused-inode boundaries (suspended=%s)", (suspended) => {
    const file = path.join(workspace(), "events.jsonl");
    let generation = 1n;
    metadata(() => ({ ino: 15199648742414488n, birth: generation * 1000000n, mtime: generation * 1000000n }));
    fs.writeFileSync(file, jsonl(records("events", "old", 16, 1)));
    const input = { id: "reader", status: "running", eventsFile: file };
    const reader = new NativeConversationReader();
    reader.read(input, false);
    if (suspended) reader.suspend();
    const replacement = records("events", "new", 16, 1)[0];
    fs.writeFileSync(file, jsonl([replacement, replacement]));
    generation++;
    const rejected = reader.read(input, false);
    expect(rejected.messages).toEqual([]);
    expect(rejected.unavailable?.eventsFile).toBe(true);
    expect(rejected.error).toContain("ambiguous");
    expect(reader.loadNewer()!.messages).toEqual([]);
    fs.writeFileSync(file, jsonl(records("events", "new", 16, 1)));
    expect(reader.read(input, false).messages).toEqual([message(0, "new", 16)]);
    reader.clear();
  });
});
