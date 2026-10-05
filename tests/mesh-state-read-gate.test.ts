import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, assertMeshStateReadable } from "../src/mesh/store.js";
import { writeFileAtomic } from "../src/core/atomic-write.js";
import { replayStateJournal, stateReadIdentity } from "../src/mesh/read-journal.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
const roots: string[] = [];
const readFile = fs.readFileSync.bind(fs);
const identity = { id: "session:writer", name: "writer", kind: "main" as const };
const fixture = async (options: ConstructorParameters<typeof MeshStore>[3] = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "state-read-gate-")); roots.push(root);
  const writer = new MeshStore(root, 256 * 1024, 1000, options);
  await writer.put({ key: "field/heartbeat/writer", value: 0, identity });
  const reader = new MeshStore(root, 256 * 1024, 1000, { readCacheMs: 5000 });
  const file = path.join(root, "state.json"), journal = path.join(root, "state.read-journal.jsonl");
  const disk = () => JSON.parse(fs.readFileSync(file, "utf8"));
  reader.stateToken();
  const reads = vi.spyOn(fs, "readFileSync");
  const count = () => reads.mock.calls.filter(([target]) => String(target) === file).length;
  return { root, writer, reader, file, journal, disk, count };
};
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
describe("process-wide physical state read gate (#4383)", () => {
  it("fresh readers share one parsed snapshot and never reopen an unchanged canonical file", async () => {
    const f = await fixture(), token = f.reader.stateToken({ fresh: true });
    const others = [new MeshStore(f.root, 256 * 1024, 1000), new MeshStore(path.join(f.root, "."), 256 * 1024, 1000)];
    const opens = vi.spyOn(fs, "openSync");
    for (let poll = 0; poll < 100; poll++) for (const reader of [f.reader, ...others]) {
      expect(reader.stateToken({ fresh: true })).toBe(token);
      expect(reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(0);
      expect(reader.listAll("field/", { fresh: true })).toHaveLength(1);
    }
    expect(f.count()).toBe(0);
    expect(opens.mock.calls.filter(([target]) => String(target) === f.file).length).toBeLessThanOrEqual(2);
  });
  it("fresh lists override explicit snapshots; ordinary hits do not slide the idle window", async () => {
    vi.useFakeTimers({ now: 1000000 });
    const f = await fixture(), old = f.reader.stateToken();
    await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
    expect(f.reader.listAll("field/", { snapshot: old })[0]?.value).toBe(0);
    expect(f.reader.listAll("field/", { snapshot: old, fresh: true })[0]?.value).toBe(1);
    await f.writer.put({ key: "field/heartbeat/writer", value: 2, identity });
    vi.setSystemTime(1004999); expect(f.reader.get("field/heartbeat/writer")?.value).toBe(1);
    vi.setSystemTime(1005000); expect(f.reader.get("field/heartbeat/writer")?.value).toBe(2);
  });
  it.each(["absent", "first", "last"])("gates static legacy files (marker layout=%s) and sees changed in-place writes", async layout => {
    const f = await fixture(), state = f.disk(), generation = state.readGeneration;
    if (layout !== "first") delete state.readGeneration;
    if (layout === "last") state.readGeneration = generation;
    writeFileAtomic(f.file, JSON.stringify(state)); f.reader.stateToken({ fresh: true }); const before = f.count();
    for (let n = 0; n < 10; n++) f.reader.get("field/heartbeat/writer", { fresh: true });
    expect(f.count()).toBe(before);
    state.entries["field/heartbeat/writer"].value = 7; fs.writeFileSync(f.file, JSON.stringify(state));
    // Some filesystems timestamp two sub-tick in-place writes identically. This probe covers
    // changed metadata; same-metadata copied-marker ABA is covered by the conservative adapter tests.
    const changed = new Date(fs.statSync(f.file).mtimeMs + 10);
    fs.utimesSync(f.file, changed, changed);
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(7); expect(f.count()).toBe(before + 1);
  });
  it("a derived participant index cannot hide same-version legacy name/ownership edits", async () => {
    const f = await fixture(), now = Date.now();
    const hash = (id: string) => createHash("sha256").update(id).digest("hex");
    const peer = "session:peer", participantKey = "topology/participants/" + hash(peer);
    await f.writer.writeBatch({ identity, ops: [
      ...["old-host", "new-host"].map(id => ({ kind: "put" as const, key: "topology/hosts/" + hash(id),
        value: { format: 1, id, rootId: peer, identity, startedAt: 1, updatedAt: now, expiresAt: now + 15000 } })),
      { kind: "put", key: participantKey, value: { format: 1, id: peer, kind: "root", rootId: peer,
        ownerHostId: "old-host", ownerIdentityId: identity.id, name: "old-name", status: "idle",
        runner: "pi", transport: "host", capabilities: ["steer"], controlProtocol: "v1", startedAt: 1, updatedAt: now } },
    ] });
    const directory = new ParticipantDirectory(f.reader, { enabled: true, hostId: "observer", rootId: "observer", identity });
    expect(directory.list({ scope: "project", fresh: true }).find(p => p.id === peer)?.name).toBe("old-name");
    const state = f.disk(), value = state.entries[participantKey].value;
    value.name = "new-name"; value.ownerHostId = "new-host"; // Keep version, UUID and updatedAt unchanged.
    writeFileAtomic(f.file, JSON.stringify(state));
    expect(directory.list({ scope: "project", fresh: true }).find(p => p.id === peer))
      .toMatchObject({ name: "new-name", ownerHostId: "new-host" });
    await directory.close();
  });
  it("strict routing checks share readable snapshots but never certify a tolerant corrupt cache", async () => {
    const f = await fixture();
    assertMeshStateReadable(f.root); expect(f.count()).toBe(0);
    writeFileAtomic(f.file, "{}");
    expect(f.reader.listAll("", { fresh: true })).toEqual([]);
    expect(() => assertMeshStateReadable(f.root)).toThrow("invalid state format");
  });
  it("a copied-marker replacement during a full read cannot certify older bytes under the newer physical identity", async () => {
    const f = await fixture(), state = f.disk();
    state.entries["field/heartbeat/writer"].value = 1;
    writeFileAtomic(f.file, JSON.stringify(state));
    const stat = fs.statSync.bind(fs), read = readFile, frozen = stat(f.file);
    // Keep the lossy tuple fixed while real bigint inode/ctime identity still changes.
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) =>
      String(target) === f.file && !(args[0] as { bigint?: boolean } | undefined)?.bigint
        ? frozen : (stat as (...args: unknown[]) => fs.Stats)(target, ...args)) as typeof fs.statSync);
    let replaced = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      const bytes = (read as (...args: unknown[]) => unknown)(target, ...args);
      if (String(target) === f.file && !replaced) {
        replaced = true; state.entries["field/heartbeat/writer"].value = 2;
        writeFileAtomic(f.file, JSON.stringify(state));
      }
      return bytes;
    }) as typeof fs.readFileSync);
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(2);
    expect(replaced).toBe(true);
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(2);
  });
  it("an expired ordinary read detects changed physical identity even if a legacy marker and lossy stat repeat", async () => {
    vi.useFakeTimers({ now: 1000000 });
    const f = await fixture(), state = f.disk();
    const stat = fs.statSync.bind(fs), frozen = stat(f.file);
    state.entries["field/heartbeat/writer"].value = 9;
    writeFileAtomic(f.file, JSON.stringify(state));
    vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) =>
      String(target) === f.file && !(args[0] as { bigint?: boolean } | undefined)?.bigint
        ? frozen : (stat as (...args: unknown[]) => fs.Stats)(target, ...args)) as typeof fs.statSync);
    vi.setSystemTime(1005000);
    expect(f.reader.get("field/heartbeat/writer")?.value).toBe(9);
  });
  it("every mutating put, delete, batch and prepare path bumps generation and invalidates local and peer snapshots", async () => {
    const f = await fixture();
    const writes = [
      () => f.writer.put({ key: "field/heartbeat/writer", value: 1, identity }),
      () => f.writer.delete({ key: "field/heartbeat/writer" }),
      () => f.writer.writeBatch({ identity, ops: [{ kind: "put" as const, key: "field/batch/a", value: "batch" }] }),
      () => f.writer.writeBatch({ identity, ops: [], prepare: () => [{ kind: "put" as const, key: "field/prepared/a", value: "prepared" }] }),
      () => f.writer.writeBatch({ identity, ops: [{ kind: "delete" as const, key: "field/batch/a" }] }),
      () => f.writer.writeBatch({ identity, ops: [], prepare: () => [{ kind: "delete" as const, key: "field/prepared/a" }] }),
    ];
    for (const write of writes) {
      const old = f.reader.stateToken({ fresh: true }), before = f.disk();
      await write();
      const after = f.disk();
      expect(after.readGeneration).not.toBe(before.readGeneration);
      expect(f.writer.stateToken({ fresh: true })).toEqual(after);
      expect(f.reader.stateToken({ fresh: true })).toEqual(after);
      expect(f.reader.stateToken()).not.toBe(old);
      expect(old).toEqual(before); // Write transactions and replay never mutate captured tokens.
    }
    const generation = f.disk().readGeneration;
    await f.writer.delete({ key: "field/missing/a" });
    await f.writer.writeBatch({ identity, ops: [{ kind: "delete", key: "field/missing/a" }] });
    await f.writer.confirmWritable();
    expect(f.disk().readGeneration).toBe(generation); // No state writes occurred.
  });
  it("confirmWritable revalidates without discarding unchanged bytes", async () => {
    const f = await fixture(), token = f.reader.stateToken(); await f.reader.confirmWritable();
    expect(f.reader.stateToken()).toBe(token); expect(f.count()).toBe(0);
  });
});
describe("incremental state read journal", () => {
  it("replays puts/deletes/tombstone eviction without a full read and preserves old tokens", async () => {
    const f = await fixture({ maxStateTombstones: 1 }), old = f.reader.stateToken();
    await f.writer.put({ key: "field/heartbeat/other", value: "other", identity });
    await f.writer.delete({ key: "field/heartbeat/writer" }); await f.writer.delete({ key: "field/heartbeat/other" });
    await f.writer.put({ key: "field/heartbeat/writer", value: "latest", identity }); const before = f.count();
    const token = f.reader.stateToken({ fresh: true }); expect(token).not.toBe(old);
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe("latest");
    expect(f.reader.get("field/heartbeat/other", { fresh: true })).toBeUndefined();
    expect(f.reader.get("field/heartbeat/writer", { snapshot: old })?.value).toBe(0);
    expect(f.count()).toBe(before); expect(token).toEqual(f.disk());
  });
  it("a new store replays from an older process snapshot without another full parse", async () => {
    const f = await fixture();
    await f.writer.put({ key: "field/heartbeat/writer", value: 4, identity });
    const before = f.count(), next = new MeshStore(f.root, 256 * 1024, 1000);
    expect(next.get("field/heartbeat/writer", { fresh: true })?.value).toBe(4);
    expect(f.count()).toBe(before);
  });
  it("a replay cursor advances only through the consumed UTF-8 prefix, not a later canonical generation", async () => {
    const f = await fixture(), base = f.disk(), baseIdentity = stateReadIdentity(f.file);
    const stamp = () => { const stat = fs.statSync(f.file);
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`; };
    await f.writer.put({ key: "field/heartbeat/writer", value: "雪😀", identity });
    const first = f.disk(), firstIdentity = stateReadIdentity(f.file)!;
    const firstStamp = stamp();
    const firstOffset = fs.statSync(f.journal).size;
    await f.writer.put({ key: "field/heartbeat/writer", value: "latest雪😀", identity });
    const last = f.disk(), lastIdentity = stateReadIdentity(f.file)!;
    // A writer may append a later record after the reader captured its canonical endpoint.
    // The helper must not skip that unseen record when returning its incremental cursor.
    const replay = replayStateJournal(f.root, base, first.readGeneration, firstIdentity, firstStamp, baseIdentity, first.readJournalHash)!;
    expect(replay.state).toEqual(first);
    expect(replay.cursor.offset).toBe(firstOffset);
    const next = replayStateJournal(f.root, replay.state, last.readGeneration, lastIdentity, stamp(),
      firstIdentity, last.readJournalHash, replay.cursor);
    expect(next?.state).toEqual(last);
    expect(next?.cursor.offset).toBe(fs.statSync(f.journal).size);
  });
  it("reads only appended bytes after its first replay", async () => {
    const f = await fixture(); await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
    f.reader.stateToken({ fresh: true }); const offset = fs.statSync(f.journal).size;
    await f.writer.put({ key: "field/heartbeat/writer", value: 2, identity }); const reads = vi.spyOn(fs, "readSync");
    f.reader.stateToken({ fresh: true });
    expect(reads.mock.calls.some(call => {
      const args: unknown[] = Array.from(call);
      return args[4] === offset && args[3] === fs.statSync(f.journal).size - offset;
    })).toBe(true);
  });
  it.each(["absent", "truncated", "checksum", "gap", "oversize", "recomputed-head", "recomputed-middle"])("falls back on a %s journal", async failure => {
    const f = await fixture(); await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
    await f.writer.put({ key: "field/heartbeat/writer", value: 2, identity }); const text = fs.readFileSync(f.journal, "utf8");
    if (failure === "absent") fs.rmSync(f.journal);
    if (failure === "truncated") fs.writeFileSync(f.journal, text.slice(0, -3));
    if (failure === "checksum") fs.writeFileSync(f.journal, text.replace('"checksum":"', '"checksum":"damaged'));
    if (failure === "gap") fs.writeFileSync(f.journal, text.split("\n").slice(1).join("\n"));
    if (failure === "oversize") fs.writeFileSync(f.journal, "x".repeat(2 * 1024 * 1024 + 1));
    if (failure.startsWith("recomputed-")) {
      const rows = text.trimEnd().split("\n").map(line => JSON.parse(line));
      const row = rows[failure === "recomputed-head" ? rows.length - 1 : 0];
      row.delta.entries["field/heartbeat/writer"].value = "forged";
      row.checksum = createHash("sha256").update(JSON.stringify(row.delta)).digest("hex");
      fs.writeFileSync(f.journal, rows.map(row => JSON.stringify(row)).join("\n") + "\n");
    }
    const before = f.count(); expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(2);
    expect(f.count()).toBe(before + 1);
  });
  it("rejects copied-marker legacy edits followed by unrelated current commits", async () => {
    const f = await fixture(), state = f.disk(); state.entries["field/heartbeat/writer"].value = "legacy modification";
    writeFileAtomic(f.file, JSON.stringify(state)); await f.writer.put({ key: "field/unrelated/writer", value: 1, identity });
    const before = f.count(); expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe("legacy modification");
    expect(f.count()).toBe(before + 1);
  });
  it("rejects stale journal identity after a same-generation legacy replacement", async () => {
    const f = await fixture(); await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
    f.reader.stateToken({ fresh: true }); const state = f.disk(); state.entries["field/heartbeat/writer"].value = 9;
    writeFileAtomic(f.file, JSON.stringify(state)); expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(9);
  });
  it("publication failure does not reject a canonical commit", async () => {
    const f = await fixture(); fs.mkdirSync(f.journal);
    await expect(f.writer.put({ key: "field/heartbeat/writer", value: 1, identity })).resolves.toMatchObject({ value: 1 });
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(1);
  });
  it("bounds rotation, falls back across lost history, then resumes incremental reads", async () => {
    const f = await fixture();
    for (let n = 0; n < 40; n++) await f.writer.put({ key: "field/bulk/value", value: `${n}:` + "x".repeat(64000), identity });
    expect(fs.statSync(f.journal).size).toBeLessThanOrEqual(2 * 1024 * 1024); const before = f.count();
    expect(f.reader.get("field/bulk/value", { fresh: true })?.value).toBe("39:" + "x".repeat(64000)); expect(f.count()).toBe(before + 1);
    await f.writer.put({ key: "field/heartbeat/writer", value: 3, identity }); const after = f.count();
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(3); expect(f.count()).toBe(after);
  });
  it("does not import a shared/journal snapshot past a smaller reader's size budget", async () => {
    const f = await fixture(); await f.writer.put({ key: "field/bulk/value", value: "x".repeat(150000), identity });
    f.reader.stateToken({ fresh: true }); const small = new MeshStore(f.root, 64000, 1000, { maxStateBytes: 128000 });
    expect(() => small.stateToken({ fresh: true })).toThrow("state exceeds");
  });
});
