import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#4250: idle readers follow the read journal by offset, never re-hash the whole
// canonical payload per change, and still fall back on rotation, legacy writers and corruption.
const roots: string[] = [];
const identity = { id: "session:writer", name: "writer", kind: "main" as const };
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = async (options: ConstructorParameters<typeof MeshStore>[3] = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "read-journal-follow-")); roots.push(root);
  const writer = new MeshStore(root, 256 * 1024, 1000, options);
  // ~1 MB canonical payload, so a whole-file read or hash is unmistakable.
  await writer.writeBatch({ identity, ops: Array.from({ length: 100 }, (_, i) => ({ kind: "put" as const,
    key: `bulk/${String(i).padStart(3, "0")}`, value: "x".repeat(10_000) })) });
  const file = path.join(root, "state.json"), journal = path.join(root, "state.read-journal.jsonl");
  const reader = new MeshStore(root, 256 * 1024, 1000);
  reader.stateToken();
  // Bytes read from any descriptor (readSync) plus whole-file reads of state.json.
  const readSync = vi.spyOn(fs, "readSync"), readFile = vi.spyOn(fs, "readFileSync");
  const bytes = () => readSync.mock.results.reduce((sum, r) => sum + (r.type === "return" ? Number(r.value) : 0), 0);
  const parses = () => readFile.mock.calls.filter(([target]) => String(target) === file).length;
  const rows = () => fs.readFileSync(journal, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
  return { root, writer, reader, file, journal, bytes, parses, rows };
};
const disk = (file: string) => JSON.parse(fs.readFileSync(file, "utf8"));

describe("read journal offset follow (smarty-dev#4250)", () => {
  it("sees every change exactly once, reading only appended bytes, without re-hashing the payload", async () => {
    const f = await fixture();
    let tokens = new Set<object>([f.reader.stateToken()]);
    for (let n = 1; n <= 20; n++) {
      await f.writer.put({ key: "field/heartbeat", value: n, identity });
      const before = f.bytes(), parses = f.parses();
      expect(f.reader.get("field/heartbeat")?.value).toBe(n);
      tokens.add(f.reader.stateToken());
      // Polling again without a change reads nothing and yields the same snapshot.
      for (let poll = 0; poll < 5; poll++) expect(f.reader.get("field/heartbeat")?.value).toBe(n);
      tokens.add(f.reader.stateToken());
      expect(f.parses()).toBe(parses);
      expect(f.bytes() - before).toBeLessThan(8 * 1024); // the appended record + a bounded header, not ~1 MB
    }
    expect(tokens.size).toBe(21); // one new snapshot per change, none for unchanged polls
    tokens = new Set();
    expect(f.reader.listAll("bulk/")).toEqual(new MeshStore(f.root, 256 * 1024, 1000).listAll("bulk/"));
  });

  it("an authoritative (fresh) read binds the followed endpoint to the canonical payload once", async () => {
    const f = await fixture();
    await f.writer.put({ key: "field/heartbeat", value: 1, identity });
    expect(f.reader.get("field/heartbeat")?.value).toBe(1);
    const before = f.bytes();
    expect(f.reader.get("field/heartbeat", { fresh: true })?.value).toBe(1);
    expect(f.bytes() - before).toBeGreaterThan(fs.statSync(f.file).size - 1024); // one payload hash
    const again = f.bytes();
    expect(f.reader.get("field/heartbeat", { fresh: true })?.value).toBe(1);
    expect(f.bytes() - again).toBeLessThan(1024); // bound once, not per read
  });

  it("publishes tombstone changes as a small patch and replays deletes, recreates and compaction", async () => {
    const f = await fixture({ maxStateTombstones: 5 });
    const reference = () => disk(f.file).tombstoneOrder;
    for (let n = 0; n < 12; n++) {
      const key = `leases/transient-${String(n % 8).padStart(40, "0")}`;
      await f.writer.put({ key, value: n, identity });
      await f.writer.delete({ key });
      const parses = f.parses();
      expect(f.reader.get(key)).toBeUndefined();
      const followed = (f.reader.stateToken() as { tombstoneOrder?: string[] }).tombstoneOrder;
      expect(f.parses()).toBe(parses);
      expect(followed).toEqual(reference());
    }
    const last = f.rows().at(-1)!.delta;
    expect(last.format).toBe(2);
    expect(last.tombstonePatch).toBeDefined();
    expect(last.tombstoneOrder).toBeUndefined();
    expect(JSON.stringify(last.tombstonePatch).length).toBeLessThan(JSON.stringify(reference()).length);
  });

  it("survives journal rotation (a new inode) by replaying it from the start and verifying once", async () => {
    const f = await fixture();
    await f.writer.put({ key: "field/heartbeat", value: 1, identity });
    expect(f.reader.get("field/heartbeat")?.value).toBe(1);
    await f.writer.put({ key: "field/heartbeat", value: 2, identity });
    const rotated = f.journal + ".next";
    fs.writeFileSync(rotated, fs.readFileSync(f.journal)); // same records, new inode
    fs.renameSync(rotated, f.journal);
    const parses = f.parses(), before = f.bytes();
    expect(f.reader.get("field/heartbeat")?.value).toBe(2);
    expect(f.parses()).toBe(parses);
    expect(f.bytes() - before).toBeGreaterThan(fs.statSync(f.file).size - 1024); // full verify on rotation
    await f.writer.put({ key: "field/heartbeat", value: 3, identity });
    const next = f.bytes();
    expect(f.reader.get("field/heartbeat")?.value).toBe(3);
    expect(f.bytes() - next).toBeLessThan(8 * 1024); // back to offset follow
  });

  it("follows across a generation written without a journal record by a canonical read", async () => {
    const f = await fixture();
    const legacy = new MeshStore(f.root, 256 * 1024, 1000, { writeReadJournal: false });
    await legacy.put({ key: "field/heartbeat", value: "legacy", identity });
    expect(f.reader.get("field/heartbeat")?.value).toBe("legacy");
    await f.writer.put({ key: "field/heartbeat", value: "journal", identity });
    const parses = f.parses();
    expect(f.reader.get("field/heartbeat")?.value).toBe("journal");
    expect(f.reader.get("bulk/000")?.value).toBe("x".repeat(10_000));
    expect(f.parses()).toBeLessThanOrEqual(parses + 1);
  });

  it.each(["entry", "link", "checksum"])("rejects a corrupted chain (%s) and serves the canonical payload", async corruption => {
    const f = await fixture();
    expect(f.reader.get("field/heartbeat")).toBeUndefined();
    await f.writer.put({ key: "field/heartbeat", value: "true", identity });
    const rows = f.rows(), terminal = rows.at(-1)!;
    if (corruption === "entry") terminal.delta.entries["field/heartbeat"].value = "forged";
    if (corruption === "link") terminal.delta.previousHash = "0".repeat(64);
    if (corruption !== "checksum") terminal.checksum = createHash("sha256").update(JSON.stringify(terminal.delta)).digest("hex");
    else terminal.delta.entries["field/heartbeat"].value = "forged"; // stale self-checksum
    fs.writeFileSync(f.journal, rows.map(row => JSON.stringify(row)).join("\n") + "\n");
    const parses = f.parses();
    expect(f.reader.get("field/heartbeat")?.value).toBe("true");
    expect(f.parses()).toBe(parses + 1);
  });
});
