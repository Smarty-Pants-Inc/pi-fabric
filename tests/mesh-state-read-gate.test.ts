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
  // Full parses are counted separately from conservative no-file-id recovery reads.
  const count = () => reads.mock.calls.filter(([target, encoding]) => String(target) === file && encoding === "utf8").length;
  const byteReads = () => reads.mock.calls.filter(([target]) => String(target) === file).length;
  return { root, writer, reader, file, journal, disk, count, byteReads };
};
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
describe("process-wide physical state read gate (#4383)", () => {
  it.each(["native", "win32"])("fresh readers share one parsed snapshot and never reopen an unchanged canonical file (%s)", async mode => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    if (mode === "win32") Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const f = await fixture(), token = f.reader.stateToken({ fresh: true });
      const others = [new MeshStore(f.root, 256 * 1024, 1000), new MeshStore(path.join(f.root, "."), 256 * 1024, 1000)];
      const opens = vi.spyOn(fs, "openSync");
      for (let poll = 0; poll < 100; poll++) for (const reader of [f.reader, ...others]) {
        expect(reader.stateToken({ fresh: true })).toBe(token);
        expect(reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(0);
        expect(reader.listAll("field/", { fresh: true })).toHaveLength(1);
      }
      expect(f.count()).toBe(0); expect(f.byteReads()).toBe(0);
      expect(opens.mock.calls.filter(([target]) => String(target) === f.file).length).toBeLessThanOrEqual(2);
    } finally { Object.defineProperty(process, "platform", platform); }
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
  it.each(["native", "win32"])("normal commits and changed parses add no identity reads or repeated headers (%s)", async mode => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    if (mode === "win32") Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const f = await fixture({ writeReadJournal: false });
      const beforeIdentity = stateReadIdentity(f.file);
      expect(beforeIdentity).toBeDefined();
      expect(stateReadIdentity(f.file)).toBe(beforeIdentity);
      expect(f.byteReads()).toBe(0);
      await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
      expect(f.byteReads()).toBe(1); // Exactly the writer's fresh canonical read.
      expect(stateReadIdentity(f.file)).not.toBe(beforeIdentity);
      expect(f.byteReads()).toBe(1); // Changed physical metadata needs no byte hash.
      expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(1);
      expect(f.byteReads()).toBe(2); // Exactly one ordinary changed-payload parse.
      const headers = vi.spyOn(fs, "readSync");
      for (let n = 0; n < 10; n++) {
        expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(1);
        expect(f.reader.cachedStateStamp(true, true)).toBeDefined();
      }
      expect(f.byteReads()).toBe(2);
      expect(headers).not.toHaveBeenCalled(); // Full parsing must not discard a valid header.
    } finally { Object.defineProperty(process, "platform", platform); }
  });
  it("win32 identity honors the read budget before opening payload bytes", async () => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const f = await fixture();
      expect(stateReadIdentity(f.file, 1)).toBeUndefined();
      expect(() => assertMeshStateReadable(f.root, 1)).toThrow("state exceeds 1 bytes");
      expect(f.byteReads()).toBe(0);
    } finally { Object.defineProperty(process, "platform", platform); }
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
    const baseStamp = stamp();
    await f.writer.put({ key: "field/heartbeat/writer", value: "雪😀", identity });
    const first = f.disk(), firstIdentity = stateReadIdentity(f.file)!;
    const firstStamp = stamp();
    const firstOffset = fs.statSync(f.journal).size;
    // Sidecar bytes beyond the requested endpoint are not authority, even with a valid
    // outer checksum. Keep the canonical endpoint present so its payload binding can be
    // verified; a canonical file already advanced to G2 now correctly requires fallback.
    fs.appendFileSync(f.journal, fs.readFileSync(f.journal, "utf8"));
    const replay = replayStateJournal(f.root, base, first.readGeneration, firstIdentity, firstStamp, baseIdentity, first.readJournalHash,
      undefined, true, baseStamp)!;
    expect(replay.state).toEqual(first);
    expect(replay.cursor.offset).toBe(firstOffset);
    fs.truncateSync(f.journal, firstOffset); // Drop the simulated uncommitted tail.
    await f.writer.put({ key: "field/heartbeat/writer", value: "latest雪😀", identity });
    const last = f.disk(), lastIdentity = stateReadIdentity(f.file)!;
    const next = replayStateJournal(f.root, replay.state, last.readGeneration, lastIdentity, stamp(),
      firstIdentity, last.readJournalHash, replay.cursor, true, firstStamp);
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
  it.each(["copied-sidecar", "forged-terminal", "forged-payload-hash"])("rejects %s replay after a copied-marker ownership replacement", async attack => {
    const f = await fixture(), old = f.reader.stateToken();
    await f.writer.put({ key: "field/heartbeat/writer", value: "journal-old", identity });
    const state = f.disk(), generation = state.readGeneration, chainHead = state.readJournalHash;
    const rows = fs.readFileSync(f.journal, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
    const terminal = rows[rows.length - 1];
    state.entries["field/heartbeat/writer"].value = "canonical-new";
    state.entries["field/heartbeat/writer"].updatedBy = { ...identity, id: "session:new-owner", name: "new-owner" };
    // Legacy/stale processes can preserve both G1 markers while replacing ownership.
    writeFileAtomic(f.file, JSON.stringify(state));
    expect(state.readGeneration).toBe(generation); expect(state.readJournalHash).toBe(chainHead);
    if (attack !== "copied-sidecar") {
      const stat = fs.statSync(f.file);
      terminal.delta.identity = stateReadIdentity(f.file);
      terminal.delta.stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
      if (attack === "forged-payload-hash") {
        const { readJournalHash: _hash, ...payload } = state;
        terminal.delta.canonicalPayloadHash = createHash("sha256").update(JSON.stringify(payload)).digest("hex");
      }
      terminal.checksum = createHash("sha256").update(JSON.stringify(terminal.delta)).digest("hex");
    }
    fs.writeFileSync(f.journal, rows.map(row => JSON.stringify(row)).join("\n") + "\n");
    const before = f.count();
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })).toMatchObject({
      value: "canonical-new", updatedBy: { id: "session:new-owner" },
    });
    expect(f.count()).toBe(before + 1); // A forged terminal can never certify journal-old.
    assertMeshStateReadable(f.root); expect(f.count()).toBe(before + 1);
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.updatedBy.id).toBe("session:new-owner");
    expect(f.reader.get("field/heartbeat/writer", { snapshot: old })?.value).toBe(0);
  });
  it("fresh participant ownership rejects an endpoint-only forged journal after a copied-marker replacement", async () => {
    const f = await fixture(), now = Date.now(), peer = "session:peer";
    const hash = (id: string) => createHash("sha256").update(id).digest("hex");
    const participantKey = "topology/participants/" + hash(peer);
    await f.writer.writeBatch({ identity, ops: [
      ...["old-host", "new-host"].map(id => ({ kind: "put" as const, key: "topology/hosts/" + hash(id),
        value: { format: 1, id, rootId: peer, identity, startedAt: 1, updatedAt: now, expiresAt: now + 15000 } })),
      { kind: "put", key: participantKey, value: { format: 1, id: peer, kind: "root", rootId: peer,
        ownerHostId: "old-host", ownerIdentityId: identity.id, name: "old-name", status: "idle",
        runner: "pi", transport: "host", capabilities: ["steer"], controlProtocol: "v1", startedAt: 1, updatedAt: now } },
    ] });
    const directory = new ParticipantDirectory(f.reader, { enabled: true, hostId: "observer", rootId: "observer", identity });
    try {
      expect(directory.list({ scope: "project", fresh: true }).find(p => p.id === peer)?.ownerHostId).toBe("old-host");
      await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
      const state = f.disk(), rows = fs.readFileSync(f.journal, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
      // Neither the entry revision nor either canonical marker advances for this legacy edit.
      state.entries[participantKey].value.ownerHostId = "new-host";
      state.entries[participantKey].value.name = "new-name";
      writeFileAtomic(f.file, JSON.stringify(state));
      const terminal = rows[rows.length - 1], stat = fs.statSync(f.file);
      terminal.delta.identity = stateReadIdentity(f.file);
      terminal.delta.stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
      terminal.checksum = createHash("sha256").update(JSON.stringify(terminal.delta)).digest("hex");
      fs.writeFileSync(f.journal, rows.map(row => JSON.stringify(row)).join("\n") + "\n");
      const before = f.count();
      expect(directory.list({ scope: "project", fresh: true }).find(p => p.id === peer))
        .toMatchObject({ ownerHostId: "new-host", name: "new-name" });
      expect(f.count()).toBe(before + 1);
      assertMeshStateReadable(f.root);
      expect(directory.list({ scope: "project", fresh: true }).find(p => p.id === peer)?.ownerHostId).toBe("new-host");
    } finally { await directory.close(); }
  });
  it.each(["fresh-first", "strict-first"])("strict readability rejects forged-terminal stale authority (%s)", async order => {
    const f = await fixture();
    await f.writer.put({ key: "field/heartbeat/writer", value: "journal-old", identity });
    const state = f.disk(), rows = fs.readFileSync(f.journal, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
    // Keep both canonical markers, but the actual canonical envelope is no longer readable.
    state.format = 99;
    state.entries["field/heartbeat/writer"].updatedBy.id = "session:new-owner";
    writeFileAtomic(f.file, JSON.stringify(state));
    const stat = fs.statSync(f.file), terminal = rows[rows.length - 1];
    terminal.delta.identity = stateReadIdentity(f.file);
    terminal.delta.stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    terminal.checksum = createHash("sha256").update(JSON.stringify(terminal.delta)).digest("hex");
    fs.writeFileSync(f.journal, rows.map(row => JSON.stringify(row)).join("\n") + "\n");
    const before = f.count();
    if (order === "strict-first") expect(() => assertMeshStateReadable(f.root)).toThrow("invalid state format");
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })).toBeUndefined();
    expect(f.count()).toBeGreaterThan(before);
    expect(() => assertMeshStateReadable(f.root)).toThrow("invalid state format");
    expect(() => assertMeshStateReadable(f.root)).toThrow("invalid state format");
  });
  it("falls back on a legacy terminal without a canonical payload binding even when its head and endpoint match", async () => {
    const f = await fixture(); await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
    const state = f.disk(), row = JSON.parse(fs.readFileSync(f.journal, "utf8").trimEnd());
    delete row.delta.canonicalPayloadHash;
    const { identity: _identity, stamp: _stamp, ...body } = row.delta;
    state.readJournalHash = createHash("sha256").update(JSON.stringify(body)).digest("hex");
    writeFileAtomic(f.file, JSON.stringify(state));
    const stat = fs.statSync(f.file);
    row.delta.identity = stateReadIdentity(f.file);
    row.delta.stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    row.checksum = createHash("sha256").update(JSON.stringify(row.delta)).digest("hex");
    fs.writeFileSync(f.journal, JSON.stringify(row) + "\n");
    const before = f.count();
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(1);
    expect(f.count()).toBe(before + 1);
  });
  it.each(["native", "win32"].flatMap(platform => ["short-read", "eio", "replacement"].map(failure => ({ platform, failure }))))(
    "falls back when canonical payload verification encounters $failure ($platform)", async ({ platform: mode, failure }) => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    if (mode === "win32") Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const f = await fixture(); await f.writer.put({ key: "field/bulk/utf8", value: "雪😀".repeat(15000), identity });
      const state = f.disk(), read = fs.readSync.bind(fs), before = f.count();
      let injected = false;
      const replace = () => {
        state.entries["field/heartbeat/writer"].value = "replacement";
        state.entries["field/heartbeat/writer"].updatedBy.id = "session:new-owner";
        writeFileAtomic(f.file, JSON.stringify(state));
        expect(JSON.parse(readFile(f.file, "utf8")).entries["field/heartbeat/writer"].value).toBe("replacement");
      };
      let replacementFd: number | undefined;
      const close = fs.closeSync.bind(fs);
      vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number) => {
        if (!injected && length === 64 * 1024 && fs.fstatSync(fd).ino === fs.statSync(f.file).ino) {
          injected = true;
          if (failure === "short-read") return 0;
          if (failure === "eio") throw Object.assign(new Error("verification EIO"), { code: "EIO" });
          replacementFd = fd;
        }
        return read(fd, buffer, offset, length, position);
      }) as typeof fs.readSync);
      vi.spyOn(fs, "closeSync").mockImplementation(fd => {
        close(fd);
        if (replacementFd === fd) {
          replacementFd = undefined;
          replace(); // Closed canonical handle: replacement must succeed on Windows too.
        }
      });
      expect(f.reader.get("field/heartbeat/writer", { fresh: true })).toMatchObject(failure === "replacement"
        ? { value: "replacement", updatedBy: { id: "session:new-owner" } } : { value: 0, updatedBy: { id: identity.id } });
      expect(injected).toBe(true); expect(f.count()).toBeGreaterThan(before);
      assertMeshStateReadable(f.root);
    } finally { Object.defineProperty(process, "platform", platform); }
  });
  it.each(["rename", "delete-create"])("win32 fallback reopens a successful %s replacement with repeated timestamps and size", async replacement => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const f = await fixture();
      await f.writer.put({ key: "field/bulk/utf8", value: "雪😀".repeat(15000), identity });
      const state = f.disk(), before = f.count(), frozen = fs.statSync(f.file, { bigint: true });
      const frozenStat = fs.statSync(f.file), stat = fs.statSync.bind(fs);
      // An adapter with no usable file id and coarse, repeated timestamps must
      // distinguish equal-sized content by hash, not certify stale ownership.
      vi.spyOn(fs, "statSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) =>
        String(target) === f.file
          ? (args[0] as { bigint?: boolean } | undefined)?.bigint ? { ...frozen, ino: 0n } : frozenStat
          : (stat as (...args: unknown[]) => unknown)(target, ...args)) as typeof fs.statSync);
      const oldIdentity = stateReadIdentity(f.file);
      let injected = false;
      const replace = () => {
        injected = true;
        state.entries["field/heartbeat/writer"].value = 9;
        state.entries["field/heartbeat/writer"].updatedBy.id = "session:newone";
        const text = JSON.stringify(state);
        expect(BigInt(Buffer.byteLength(text))).toBe(frozen.size);
        if (replacement === "rename") writeFileAtomic(f.file, text);
        else { fs.unlinkSync(f.file); fs.writeFileSync(f.file, text); }
        expect(JSON.parse(readFile(f.file, "utf8")).entries["field/heartbeat/writer"].value).toBe(9);
      };
      const descriptors = new Set<number>();
      const open = fs.openSync.bind(fs), close = fs.closeSync.bind(fs), fstat = fs.fstatSync.bind(fs), read = fs.readSync.bind(fs);
      const opens = vi.spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
        const fd = (open as (...args: unknown[]) => number)(target, ...args);
        if (String(target) === f.file) descriptors.add(fd);
        return fd;
      }) as typeof fs.openSync);
      vi.spyOn(fs, "closeSync").mockImplementation(fd => { descriptors.delete(fd); close(fd); });
      vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...args: unknown[]) =>
        descriptors.has(fd) && (args[0] as { bigint?: boolean } | undefined)?.bigint
          ? { ...frozen, ino: 0n } : (fstat as (...args: unknown[]) => unknown)(fd, ...args)) as typeof fs.fstatSync);
      // Also intercept the old streaming verifier: the red baseline must observe
      // an actual successful replacement, not merely miss the new path-read hook.
      vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number) => {
        if (!injected && descriptors.has(fd) && length === 64 * 1024) replace();
        return read(fd, buffer, offset, length, position);
      }) as typeof fs.readSync);
      vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
        const bytes = (readFile as (...args: unknown[]) => unknown)(target, ...args);
        if (!injected && String(target) === f.file && args.length === 0) replace();
        return bytes;
      }) as typeof fs.readFileSync);
      // Rebind the terminal physical endpoint to the adapter's identity. Its
      // chain-bound payload hash is still the committed, older canonical hash.
      const rows = fs.readFileSync(f.journal, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
      const terminal = rows[rows.length - 1];
      terminal.delta.identity = oldIdentity;
      terminal.checksum = createHash("sha256").update(JSON.stringify(terminal.delta)).digest("hex");
      fs.writeFileSync(f.journal, rows.map(row => JSON.stringify(row)).join("\n") + "\n");
      const current = f.reader.get("field/heartbeat/writer", { fresh: true });
      expect(injected).toBe(true);
      expect(current).toMatchObject({ value: 9, updatedBy: { id: "session:newone" } });
      expect(stateReadIdentity(f.file)).not.toBe(oldIdentity);
      expect(f.count()).toBeGreaterThan(before);
      expect(opens.mock.calls.some(([target]) => String(target) === f.file)).toBe(true); // Header only, never retained.
      assertMeshStateReadable(f.root);
      expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(9);
    } finally { Object.defineProperty(process, "platform", platform); }
  });
  it("an already advanced canonical endpoint cannot verify historical journal bytes", async () => {
    const f = await fixture(), base = f.disk(), baseIdentity = stateReadIdentity(f.file);
    await f.writer.put({ key: "field/heartbeat/writer", value: 1, identity });
    const first = f.disk(), firstIdentity = stateReadIdentity(f.file)!, stat = fs.statSync(f.file);
    const stamp = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
    await f.writer.put({ key: "field/heartbeat/writer", value: 2, identity });
    expect(replayStateJournal(f.root, base, first.readGeneration, firstIdentity, stamp, baseIdentity, first.readJournalHash)).toBeUndefined();
    expect(f.reader.get("field/heartbeat/writer", { fresh: true })?.value).toBe(2);
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
