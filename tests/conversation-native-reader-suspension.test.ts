import "./fixtures/conversation-host.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { NativeConversationReader, type NativeConversationTranscript } from "../src/ui/conversation-native-reader.js";
import { NativeReaderCheckpoint } from "../src/ui/conversation-native-reader-checkpoint.js";

const directories: string[] = [];
const workspace = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "reader-suspension-"));
  directories.push(directory);
  return directory;
};
const jsonl = (records: unknown[]) => records.map((record) => JSON.stringify(record) + "\n").join("");
const entry = (i: number, length = 5000) => ({
  type: "message", id: `m${i}`, parentId: i ? `m${i - 1}` : null,
  timestamp: "2026-01-01T00:00:00.000Z", message: { role: "user", content: `${i}:` + "x".repeat(length), timestamp: i },
});
const header = { type: "session", id: "session" };
const source = (file: string) => ({ id: "reader", status: "running", logFile: file });
const content = (snapshot: NativeConversationTranscript) => ({ messages: snapshot.messages, entries: snapshot.entries, streaming: snapshot.streaming, pendingMessages: snapshot.pendingMessages, leafId: snapshot.leafId, hasMore: snapshot.hasMore, hasNewer: snapshot.hasNewer, historyComplete: snapshot.historyComplete });
const trackCheckpoints = () => {
  const original = fs.mkdtempSync.bind(fs);
  return vi.spyOn(fs, "mkdtempSync").mockImplementation(((prefix: string) => {
    const directory = original(prefix);
    directories.push(directory);
    return directory;
  }) as typeof fs.mkdtempSync);
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("native reader disk suspension", () => {
  it.each(["relocation", "unread"] as const)("preserves poisoned evidence across a failed %s replacement transaction", (phase) => {
    const file = path.join(workspace(), "session.jsonl");
    const input = { id: "reader", status: "running", sessionFile: file };
    fs.writeFileSync(file, jsonl([header, entry(0, 16)]));
    const reader = new NativeConversationReader();
    reader.read(input, false);
    fs.writeFileSync(file, jsonl([header, { ...entry(0, 16), message: { ...entry(0, 16).message, content: "mutated" } }]));
    reader.loadLatest(); // Contradictory interval permanently poisons the proof.
    const newRecord = { ...entry(0, 16), message: { ...entry(0, 16).message, content: "new-generation" } };
    const oldInode = fs.statSync(file).ino;
    fs.writeFileSync(`${file}.new`, jsonl([header, newRecord, entry(1, 16)]));
    fs.renameSync(`${file}.new`, file);
    const inode = fs.statSync(file).ino;
    expect(inode).not.toBe(oldInode);
    const realRead = fs.readSync.bind(fs);
    let injected = 0;
    const reads = vi.spyOn(fs, "readSync").mockImplementation(((...args: Parameters<typeof fs.readSync>) => {
      const stack = new Error().stack ?? "";
      const actualPhase = stack.includes("relocateBounds") ? "relocation"
        : stack.includes("replaceWindowIfNeeded") && !stack.includes("readBackwardPage") ? "unread" : "other";
      if (fs.fstatSync(args[0]).ino === inode && actualPhase === phase) {
        injected++;
        throw Object.assign(new Error("private payload EIO"), { code: "EIO" });
      }
      return realRead(...args);
    }) as typeof fs.readSync);
    const failed = reader.read({ ...input, status: "completed" }, true);
    expect(injected).toBe(1);
    expect(failed.messages).toEqual([]);
    expect(failed.entries).toEqual([]);
    expect(failed.unavailable?.sessionFile).toBe(true);
    expect(failed.error).not.toContain("private");
    reads.mockRestore();
    const recovered = reader.read(input, false);
    expect(fs.statSync(file).ino).toBe(inode);
    expect(recovered.messages).toEqual([newRecord.message]);
    expect(recovered.leafId).toBe("m0");
    expect(recovered.hasNewer).toBe(true);
    expect(reader.loadNewer()!.messages).toEqual([newRecord.message, entry(1, 16).message]);
    const temporary = trackCheckpoints();
    expect(reader.suspend()).toBe(true);
    const checkpoint = path.join(temporary.mock.results[0]!.value as string, "checkpoint");
    for (const damage of ["missing", "corrupt"]) {
      if (damage === "missing") fs.unlinkSync(checkpoint);
      else fs.writeFileSync(checkpoint, "corrupt");
      const rereads = vi.spyOn(fs, "readSync");
      const unavailable = reader.read(input, false);
      expect(unavailable.messages).toEqual([]);
      expect(unavailable.error).toContain("Unable to restore reader history");
      expect(rereads).not.toHaveBeenCalled();
      rereads.mockRestore();
    }
    reader.clear();
  });

  it.each(["session", "events"] as const)("bounds duplicate %s page verification across loadLatest and suspension", (kind) => {
    const file = path.join(workspace(), `${kind}.jsonl`);
    const record = (i: number) => kind === "session" ? entry(i, 16)
      : { type: "message_end", message: entry(i, 16).message };
    const bytes = jsonl([...(kind === "session" ? [header] : []), record(0)]);
    fs.writeFileSync(file, bytes);
    const input = { id: "reader", status: "running", ...(kind === "session" ? { sessionFile: file } : { eventsFile: file }) };
    const reader = new NativeConversationReader();
    const before = reader.read(input, false);
    const replaceIdentically = (replacementBytes = bytes) => {
      const inode = fs.statSync(file).ino;
      fs.writeFileSync(`${file}.new`, replacementBytes);
      fs.renameSync(`${file}.new`, file);
      expect(fs.statSync(file).ino).not.toBe(inode);
    };
    const reads = vi.spyOn(fs, "readSync");
    replaceIdentically();
    reads.mockClear();
    const baseline = reader.read(input, false);
    expect(content(baseline)).toEqual(content(before));
    expect(baseline.revision).toBe(before.revision);
    const baselineReads = reads.mock.calls.length;
    expect(baselineReads).toBe(1);
    trackCheckpoints();
    let resumedVerificationReads = 0;
    for (let round = 0; round < 2; round++) {
      for (let i = 0; i < 128; i++) {
        const latest = reader.loadLatest()!;
        expect(content(latest)).toEqual(content(before));
        expect(latest.status).toBe(before.status);
        expect(latest.unavailable).toBeUndefined();
        expect(latest.error).toBeUndefined();
      }
      if (round === 0) {
        const latest = reader.last!;
        expect(reader.suspend()).toBe(true);
        const resumed = reader.last!;
        expect(content(resumed)).toEqual(content(latest));
        expect(resumed.revision).toBe(latest.revision + 1);
        // Validate before any post-resume page reload could recreate evidence.
        replaceIdentically();
        reads.mockClear();
        const verifiedResume = reader.read(input, false);
        resumedVerificationReads = reads.mock.calls.length;
        expect(content(verifiedResume)).toEqual(content(resumed));
        expect(verifiedResume.revision).toBe(resumed.revision);
      }
    }
    const latest = reader.last!;
    replaceIdentically();
    reads.mockClear();
    const pinned = reader.read(input, false);
    const verificationReads = reads.mock.calls.length;
    expect(content(pinned)).toEqual(content(before));
    expect(pinned.revision).toBe(latest.revision);
    expect(pinned.status).toBe(before.status);
    expect(pinned.unavailable).toBeUndefined();
    expect(pinned.error).toBeUndefined();
    expect(verificationReads).toBe(baselineReads);
    expect(resumedVerificationReads).toBe(baselineReads);
    fs.appendFileSync(file, jsonl([record(1)]));
    const appended = reader.read(input, false);
    expect(appended.messages).toEqual(before.messages);
    expect(appended.hasNewer).toBe(true);
    expect(appended.revision).toBe(pinned.revision + 1);
    const newer = reader.loadNewer()!;
    expect(newer.messages).toEqual([...before.messages, entry(1, 16).message]);
    expect(newer.hasNewer).toBe(false);
    expect(newer.revision).toBe(appended.revision + 1);
    const appendedBytes = bytes + jsonl([record(1)]);
    replaceIdentically(appendedBytes);
    reads.mockClear();
    expect(content(reader.read(input, false))).toEqual(content(newer));
    expect(reads.mock.calls.length).toBe(2); // Original page plus appended page.
    for (let i = 0; i < 128; i++) expect(content(reader.loadLatest()!)).toEqual(content(newer));
    replaceIdentically(appendedBytes);
    reads.mockClear();
    expect(content(reader.read(input, false))).toEqual(content(newer));
    // The overlapping full page is distinct evidence, not a replacement for
    // either earlier fingerprint; duplicate invocations still add nothing.
    expect(reads.mock.calls.length).toBe(3);
    reader.clear();
  });

  it.each(["missing", "corrupt"])("retains prior digests after reloading mutated offsets with a %s checkpoint", (failure) => {
    const file = path.join(workspace(), "session.jsonl");
    const original = jsonl([header, entry(0, 16)]);
    fs.writeFileSync(file, original);
    const reader = new NativeConversationReader();
    reader.read(source(file), false);
    for (let i = 0; i < 128; i++) reader.loadLatest();
    fs.writeFileSync(file, original.replace(/x/g, "y"));
    reader.loadLatest();
    const temporary = trackCheckpoints();
    expect(reader.suspend()).toBe(true);
    const checkpoint = path.join(temporary.mock.results[0]!.value as string, "checkpoint");
    if (failure === "missing") fs.unlinkSync(checkpoint);
    else fs.writeFileSync(checkpoint, "corrupt");
    const failed = reader.read({ ...source(file), status: "completed" }, false);
    expect(failed.messages).toEqual([]);
    expect(failed.error).toContain("Unable to restore reader history");
    expect(failed.unavailable?.sessionFile).toBe(true);
    expect(failed.status).toBe("completed");
    expect(reader.suspended).toBe(true);
    reader.clear();
  });
  it.each(["missing", "corrupt"])("bounds distinct fixed-interval rewrite hash work and fails closed with a %s checkpoint", (failure) => {
    const file = path.join(workspace(), "session.jsonl");
    const input = { id: "reader", status: "running", sessionFile: file };
    const bytes = (i: number) => jsonl([header, { ...entry(0, 16), message: { role: "user", content: String(i).padStart(16, "0"), timestamp: 0 } }]);
    fs.writeFileSync(file, bytes(0));
    const size = fs.statSync(file).size;
    const inode = fs.statSync(file).ino;
    const reader = new NativeConversationReader();
    reader.read(input, false);
    const stringify = vi.spyOn(JSON, "stringify");
    for (let i = 1; i <= 256; i++) {
      fs.writeFileSync(file, bytes(i));
      const latest = reader.loadLatest()!;
      expect(latest.messages).toEqual([{ role: "user", content: String(i).padStart(16, "0"), timestamp: 0 }]);
      expect(latest.entries).toHaveLength(1);
      expect(latest.leafId).toBe("m0");
    }
    // Hash input is the raw record array, not a decoded history/payload. Once
    // contradictory, this fixed interval needs no further hashes or versions.
    const hashes = stringify.mock.calls.filter(([value]) => Array.isArray(value) && value.length === 2 && typeof value[0] === "string" && value[0].startsWith('{"type":"session"')).length;
    stringify.mockRestore();
    expect(fs.statSync(file).size).toBe(size);
    expect(fs.statSync(file).ino).toBe(inode);
    expect(hashes).toBeLessThanOrEqual(2);
    const temporary = trackCheckpoints();
    expect(reader.suspend()).toBe(true);
    const checkpoint = path.join(temporary.mock.results[0]!.value as string, "checkpoint");
    if (failure === "missing") fs.unlinkSync(checkpoint);
    else fs.writeFileSync(checkpoint, "corrupt");
    const reads = vi.spyOn(fs, "readSync");
    const failed = reader.read({ ...input, status: "completed" }, false);
    expect(failed.messages).toEqual([]);
    expect(failed.status).toBe("completed");
    expect(failed.unavailable?.sessionFile).toBe(true);
    expect(failed.error).toContain("Unable to restore reader history");
    expect(reader.suspended).toBe(true);
    expect(reads.mock.calls.length).toBe(0);
    reader.clear();
  });
  it.each(["grow", "shrink", "timestamp"] as const)("bounds changing logical-history evidence and relocates current fences (%s)", (variant) => {
    const file = path.join(workspace(), "session.jsonl");
    const input = { id: "reader", status: "running", sessionFile: file };
    const record = (i: number) => ({
      ...entry(0, 16),
      timestamp: new Date(Date.UTC(2026, 0, 1, 0, 0, variant === "timestamp" ? i : 0)).toISOString(),
      message: { role: "user", content: String(i).padStart(variant === "grow" ? 16 + i : variant === "shrink" ? 272 - i : 16, "x"), timestamp: variant === "timestamp" ? 1000 + i : 0 },
    });
    fs.writeFileSync(file, jsonl([header, record(0)]));
    const inode = fs.statSync(file).ino;
    const initialSize = fs.statSync(file).size;
    const reader = new NativeConversationReader();
    const stringify = vi.spyOn(JSON, "stringify");
    const reads = vi.spyOn(fs, "readSync");
    const evidenceSizes: number[] = [];
    const originalSet = Map.prototype.set;
    // Observe ordinary map allocations, without accessing reader private state.
    const evidenceSet = vi.spyOn(Map.prototype, "set").mockImplementation(function (this: Map<unknown, unknown>, key: unknown, value: unknown) {
      const result = originalSet.call(this, key, value);
      if (value && typeof value === "object" && "digest" in value && "start" in value && "first" in value) evidenceSizes.push(this.size);
      return result;
    });
    reader.read(input, false);
    for (let i = 1; i <= 256; i++) {
      fs.writeFileSync(file, jsonl([header, record(i)]));
      const latest = reader.loadLatest()!;
      expect(latest.messages).toEqual([record(i).message]);
      expect(latest.entries).toHaveLength(1);
      expect(latest.entries[0]!.timestamp).toBe(record(i).timestamp);
      expect(latest.leafId).toBe("m0");
    }
    const hashes = stringify.mock.calls.filter(([value]) => Array.isArray(value) && value.length === 2 && typeof value[0] === "string" && value[0].startsWith('{"type":"session"')).length;
    stringify.mockRestore();
    const rereadCount = reads.mock.calls.length;
    reads.mockRestore();
    evidenceSet.mockRestore();
    expect(fs.statSync(file).ino).toBe(inode);
    expect(Math.sign(fs.statSync(file).size - initialSize)).toBe(variant === "grow" ? 1 : variant === "shrink" ? -1 : 0);
    // The same two logical records must not generate 256 obsolete fingerprints.
    expect(evidenceSizes.length).toBeLessThanOrEqual(257);
    expect(Math.max(...evidenceSizes)).toBe(1);
    expect(hashes).toBeLessThanOrEqual(2);
    expect(rereadCount).toBe(257);
    const temporary = trackCheckpoints();
    expect(reader.suspend()).toBe(true);
    const resumed = reader.last!;
    expect(resumed.messages).toEqual([record(256).message]);
    expect(reader.suspend()).toBe(true);
    const checkpoint = path.join(temporary.mock.results[1]!.value as string, "checkpoint");
    const checkpointBytes = fs.readFileSync(checkpoint);
    for (const failure of ["missing", "corrupt"]) {
      if (failure === "missing") fs.unlinkSync(checkpoint);
      else fs.writeFileSync(checkpoint, "corrupt");
      const failedReads = vi.spyOn(fs, "readSync");
      const failed = reader.read(input, false);
      expect(failed.messages).toEqual([]);
      expect(failed.error).toContain("Unable to restore reader history");
      expect(failed.unavailable?.sessionFile).toBe(true);
      expect(reader.suspended).toBe(true);
      expect(failedReads.mock.calls).toHaveLength(0);
      failedReads.mockRestore();
    }
    fs.writeFileSync(checkpoint, checkpointBytes);
    expect(reader.last!.messages).toEqual([record(256).message]);
    fs.copyFileSync(file, `${file}.new`);
    fs.renameSync(`${file}.new`, file);
    expect(fs.statSync(file).ino).not.toBe(inode);
    const fresh = new NativeConversationReader();
    const expected = fresh.read(input, false);
    const relocated = reader.read(input, false);
    expect(content(relocated)).toEqual(content(expected));
    expect(relocated.entries[0]!.timestamp).toBe(record(256).timestamp);
    expect(relocated.error).toBeUndefined();
    expect(relocated.unavailable).toBeUndefined();
    // Re-parsing a new inode refreshes fences but cannot erase prior conflict.
    expect(reader.suspend()).toBe(true);
    const relocatedCheckpoint = path.join(temporary.mock.results[2]!.value as string, "checkpoint");
    for (const failure of ["missing", "corrupt"]) {
      if (failure === "missing") fs.unlinkSync(relocatedCheckpoint);
      else fs.writeFileSync(relocatedCheckpoint, "corrupt");
      const failedReads = vi.spyOn(fs, "readSync");
      expect(reader.read(input, false).error).toContain("Unable to restore reader history");
      expect(reader.last!.messages).toEqual([]);
      expect(failedReads.mock.calls).toHaveLength(0);
      failedReads.mockRestore();
    }
    fresh.clear();
    reader.clear();
  });

  it("checkpoints only final-sized live tool state and replays older pages exactly after resume", () => {
    const file = path.join(workspace(), "events.jsonl");
    const oldMessage = { role: "user", content: "old" + "o".repeat(300000), timestamp: 1 };
    fs.writeFileSync(file, jsonl([
      { type: "message_end", message: oldMessage },
      { type: "tool_execution_start", toolCallId: "tool", toolName: "bash", args: { command: "test" } },
    ]));
    const reader = new NativeConversationReader();
    expect(reader.read(source(file)).hasMore).toBe(true);
    for (let i = 1; i <= 1000; i++) {
      fs.appendFileSync(file, jsonl([{ type: "tool_execution_update", toolCallId: "tool", partialResult: { content: [{ type: "text", text: "x".repeat(i * 64) }] } }]));
      reader.read(source(file));
    }
    fs.appendFileSync(file, jsonl([{ type: "tool_execution_end", toolCallId: "tool", result: { content: [{ type: "text", text: "x".repeat(64000) }] } }]));
    const completed = reader.read(source(file));
    const temporary = trackCheckpoints();
    reader.suspend();
    const checkpoint = path.join(temporary.mock.results[0]!.value as string, "checkpoint");
    expect(fs.statSync(file).size).toBeGreaterThan(32000000);
    expect(fs.statSync(checkpoint).size).toBeLessThan(140000);
    const older = reader.loadOlder()!;
    expect(older.messages).toEqual([oldMessage]);
    expect(older.streaming).toEqual(completed.streaming);
    expect(older.hasMore).toBe(false);
  });

  it("offloads to one private payload plus ownership marker and restores loaded/pinned history without original files", () => {
    const file = path.join(workspace(), "session.jsonl");
    const records = [header, ...Array.from({ length: 150 }, (_, i) => entry(i))];
    fs.writeFileSync(file, jsonl(records));
    const reader = new NativeConversationReader();
    reader.read(source(file), false);
    reader.loadOlder();
    fs.appendFileSync(file, jsonl([entry(150)]));
    const pinned = reader.read(source(file), false);
    expect(pinned.hasMore).toBe(true);
    expect(pinned.hasNewer).toBe(true);
    const temporary = trackCheckpoints();
    const restore = vi.spyOn(NativeReaderCheckpoint.prototype, "restore");
    expect(reader.suspend()).toBe(true);
    expect(reader.suspended).toBe(true);
    expect(reader.suspend()).toBe(true);
    expect(restore).not.toHaveBeenCalled();
    const directory = temporary.mock.results[0]!.value as string;
    expect(fs.readdirSync(directory).sort()).toEqual([".fabric-scratch.json", "checkpoint"]);
    const checkpoint = path.join(directory, "checkpoint");
    // Windows accepts chmod/mode options but does not expose POSIX permission bits.
    if (process.platform !== "win32") {
      expect(fs.statSync(directory).mode & 0o777).toBe(0o700);
      expect(fs.statSync(checkpoint).mode & 0o777).toBe(0o600);
    }
    expect(fs.statSync(checkpoint).size).toBeGreaterThan(256000);
    fs.unlinkSync(file);
    const resumed = reader.last!;
    expect(content(resumed)).toEqual(content(pinned));
    expect(resumed.revision).toBeGreaterThan(pinned.revision);
    expect(reader.suspended).toBe(false);
    expect(fs.existsSync(directory)).toBe(false);
    const unavailable = reader.read(source(file), false);
    expect(unavailable.messages).toBe(resumed.messages);
    expect(unavailable.unavailable?.sessionFile).toBe(true);
    expect(reader.suspend()).toBe(true);
    expect(content(reader.last!)).toEqual(content(unavailable));
    fs.writeFileSync(file, jsonl([...records, entry(150)]));
    const recreated = reader.read(source(file), false);
    expect(recreated.messages).toHaveLength(pinned.messages.length);
    expect(content(recreated)).toEqual(content(pinned));
    expect(recreated.unavailable).toBeUndefined();
    expect(recreated.status).toBe("running");
    expect(recreated.revision).toBeGreaterThan(unavailable.revision);
    const newer = reader.loadNewer()!;
    expect(newer.messages).toHaveLength(pinned.messages.length + 1);
    expect(newer.messages).toEqual([...pinned.messages, entry(150).message]);
    expect(newer.hasNewer).toBe(false);
    expect(reader.read(source(file), false).messages).toEqual(newer.messages);
    fs.appendFileSync(file, jsonl([entry(151)]));
    expect(reader.read(source(file), false).hasNewer).toBe(true);
    expect(reader.loadNewer()!.messages).toEqual([...newer.messages, entry(151).message]);
    expect(reader.suspend()).toBe(true);
    expect(reader.loadOlder()!.historyComplete).toBe(true);
    reader.clear();
  });

  it.each(["missing", "corrupt"])("fails closed on a %s checkpoint with rewritten loaded ranges", (failure) => {
    const file = path.join(workspace(), "session.jsonl");
    fs.writeFileSync(file, jsonl([header, ...Array.from({ length: 150 }, (_, i) => entry(i))]));
    const reader = new NativeConversationReader();
    reader.read(source(file), false);
    reader.loadOlder();
    const temporary = trackCheckpoints();
    expect(reader.suspend()).toBe(true);
    const checkpoint = path.join(temporary.mock.results[0]!.value as string, "checkpoint");
    if (failure === "missing") fs.unlinkSync(checkpoint);
    else fs.writeFileSync(checkpoint, "corrupt");
    // Same inode, same record sizes, but different payloads: never replay them
    // as though they were the checkpoint's original loaded history.
    fs.writeFileSync(file, jsonl([header, ...Array.from({ length: 150 }, (_, i) => ({
      ...entry(i), message: { ...entry(i).message, content: entry(i).message.content.replace(/x/g, "y") },
    }))]));
    const failed = reader.read({ ...source(file), status: "completed" }, false);
    expect(failed.messages).toEqual([]);
    expect(failed.error).toContain("Unable to restore reader history");
    expect(failed.error!.length).toBeLessThanOrEqual(201);
    expect(failed.unavailable?.sessionFile).toBe(true);
    expect(failed.status).toBe("completed");
    expect(reader.suspended).toBe(true);
    reader.clear();
  });

  it.each(["missing", "corrupt"])("recovers a %s checkpoint from exact loaded byte ranges, not the latest tail", (failure) => {
    const file = path.join(workspace(), "session.jsonl");
    fs.writeFileSync(file, jsonl([header, ...Array.from({ length: 200 }, (_, i) => entry(i))]));
    const reader = new NativeConversationReader();
    reader.read(source(file), false);
    reader.loadOlder();
    fs.appendFileSync(file, jsonl([entry(200)]));
    const before = reader.read(source(file), false);
    const temporary = trackCheckpoints();
    expect(reader.suspend()).toBe(true);
    const checkpoint = path.join(temporary.mock.results[0]!.value as string, "checkpoint");
    if (failure === "missing") fs.unlinkSync(checkpoint);
    else fs.writeFileSync(checkpoint, "corrupt");
    const restored = reader.last!;
    expect(content(restored)).toEqual(content(before));
    expect(restored.error).toBeUndefined();
    expect(reader.suspended).toBe(false);
    expect(reader.loadNewer()!.messages).toHaveLength(before.messages.length + 1);
  });

  it("reports lost backing data without dropping bookmarks and retries the same pinned range", () => {
    const file = path.join(workspace(), "session.jsonl");
    const records = [header, ...Array.from({ length: 100 }, (_, i) => entry(i))];
    fs.writeFileSync(file, jsonl(records));
    const reader = new NativeConversationReader();
    const before = reader.read(source(file), false);
    const temporary = trackCheckpoints();
    expect(reader.suspend()).toBe(true);
    const checkpoint = path.join(temporary.mock.results[0]!.value as string, "checkpoint");
    fs.unlinkSync(checkpoint);
    fs.unlinkSync(file);
    const failed = reader.read(source(file), false);
    expect(failed.error).toContain("Unable to restore reader history");
    expect(failed.error!.length).toBeLessThanOrEqual(201);
    expect(failed.unavailable?.sessionFile).toBe(true);
    expect(reader.suspended).toBe(true);
    expect(reader.last).toBe(failed);
    const settled = reader.read({ ...source(file), status: "completed" }, false);
    expect(settled.status).toBe("completed");
    expect(settled.revision).toBeGreaterThan(failed.revision);
    expect(settled.error).toBe(failed.error);
    fs.writeFileSync(file, jsonl([...records, entry(100)]));
    const restored = reader.last!;
    expect(content(restored)).toEqual(content(before));
    expect(reader.suspended).toBe(false);
    expect(restored.revision).toBeGreaterThan(failed.revision);
    expect(reader.read(source(file), false).hasNewer).toBe(true);
  });

  it("retains usable live state on checkpoint write or publish failure", () => {
    const file = path.join(workspace(), "session.jsonl");
    fs.writeFileSync(file, jsonl([header, entry(0)]));
    const reader = new NativeConversationReader();
    const before = reader.read(source(file));
    const temporary = trackCheckpoints();
    const write = vi.spyOn(fs, "writeFileSync").mockImplementationOnce(() => { throw new Error("full disk"); });
    expect(reader.suspend()).toBe(false);
    expect(reader.suspended).toBe(false);
    expect(reader.last).toBe(before);
    expect(fs.existsSync(temporary.mock.results[0]!.value as string)).toBe(false);
    write.mockRestore();
    vi.spyOn(fs, "renameSync").mockImplementationOnce(() => { throw new Error("publish failed"); });
    expect(reader.suspend()).toBe(false);
    expect(reader.last).toBe(before);
    expect(fs.existsSync(temporary.mock.results[1]!.value as string)).toBe(false);
  });

  it("cleans up without resuming on clear/source replacement and tolerates cleanup errors", () => {
    const file = path.join(workspace(), "session.jsonl");
    fs.writeFileSync(file, jsonl([header, entry(0)]));
    const reader = new NativeConversationReader();
    reader.read(source(file));
    const temporary = trackCheckpoints();
    const restore = vi.spyOn(NativeReaderCheckpoint.prototype, "restore");
    expect(reader.suspend()).toBe(true);
    reader.clear();
    expect(reader.last).toBeUndefined();
    expect(restore).not.toHaveBeenCalled();
    expect(fs.existsSync(temporary.mock.results[0]!.value as string)).toBe(false);
    reader.read(source(file));
    reader.suspend();
    reader.read({ ...source(file), id: "other" });
    expect(restore).not.toHaveBeenCalled();
    expect(fs.existsSync(temporary.mock.results[1]!.value as string)).toBe(false);
    reader.suspend();
    vi.spyOn(fs, "rmSync").mockImplementationOnce(() => { throw new Error("cleanup denied"); });
    expect(reader.last!.messages).toHaveLength(1);
    expect(reader.suspended).toBe(false);
  });

  it("preserves no-session streaming state and incomplete arguments through repeated suspension", () => {
    const file = path.join(workspace(), "events.jsonl");
    fs.writeFileSync(file, jsonl([
      { type: "message_start", message: { role: "assistant", content: [], timestamp: 1 } },
      { type: "message_update", assistantMessageEvent: { type: "toolcall_start", contentIndex: 0, id: "tool", toolName: "read" } },
      { type: "message_update", assistantMessageEvent: { type: "toolcall_delta", contentIndex: 0, delta: '{"path":' } },
      { type: "tool_execution_start", toolCallId: "tool", toolName: "read", args: { path: "x" } },
      { type: "tool_execution_update", toolCallId: "tool", partialResult: { content: [{ type: "text", text: "partial" }], details: { full: true } } },
      { type: "queue_update", steering: ["next"], followUp: ["later"] },
    ]));
    const reader = new NativeConversationReader();
    const before = reader.read(source(file), false);
    trackCheckpoints();
    reader.suspend();
    expect(content(reader.last!)).toEqual(content(before));
    reader.suspend();
    fs.appendFileSync(file, jsonl([{ type: "message_update", assistantMessageEvent: { type: "toolcall_delta", contentIndex: 0, delta: '"x"}' } }]));
    const newer = reader.loadNewer()!;
    expect(newer.streaming.partialAssistant?.content[0]).toMatchObject({ arguments: { path: "x" } });
    expect(newer.streaming.tools[0]?.partial).toEqual(before.streaming.tools[0]?.partial);
    expect(newer.pendingMessages).toEqual(before.pendingMessages);
    reader.suspend();
    expect(reader.loadLatest()!.pendingMessages).toEqual(before.pendingMessages);
    reader.clear();
  });

  it("preserves loaded stable session pages across actor activation rollover while suspended", () => {
    const directory = workspace();
    const session = path.join(directory, "session.jsonl");
    const firstRun = path.join(directory, "run1.events.jsonl");
    const nextRun = path.join(directory, "run2.events.jsonl");
    fs.writeFileSync(session, jsonl([header, ...Array.from({ length: 100 }, (_, i) => entry(i))]));
    fs.writeFileSync(firstRun, jsonl([{ type: "message_end", message: { role: "user", content: "old run", timestamp: 500 } }]));
    fs.writeFileSync(nextRun, jsonl([{ type: "message_end", message: { role: "user", content: "new run", timestamp: 501 } }]));
    const reader = new NativeConversationReader();
    reader.read({ ...source(firstRun), sessionFile: session }, false);
    const loaded = reader.loadOlder()!;
    expect(loaded.messages).toHaveLength(101);
    trackCheckpoints();
    reader.suspend();
    fs.unlinkSync(session);
    fs.unlinkSync(firstRun);
    const next = reader.read({ ...source(nextRun), sessionFile: session }, false);
    expect(next.messages).toHaveLength(101);
    expect(next.messages.at(-1)).toMatchObject({ content: "new run" });
    expect(next.messages.slice(0, 100)).toEqual(loaded.messages.slice(0, 100));
    expect(next.unavailable?.sessionFile).toBe(true);
    expect(next.revision).toBeGreaterThan(loaded.revision);
  });
});
