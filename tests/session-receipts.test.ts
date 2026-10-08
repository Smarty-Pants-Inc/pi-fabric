import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { confirmedSessionEntries, confirmedSessionReceiptSnapshot, type SessionReceiptManager } from "../src/core/session-receipts.js";
import { confirmedMainInboxIds, confirmedRootInboxSession, projectRootInboxReceipt, rootInboxSession } from "../src/topology/root-inbox.js";

const matches = (line: string) => line.includes("pi-fabric-inbox") || line.includes("pi-fabric-agent-message");
const receipt = (id: string) => JSON.stringify({ type: "custom_message", customType: "pi-fabric-inbox", timestamp: "2026-01-01T00:00:00.000Z", details: { ids: [id] } }) + "\n";
const other = JSON.stringify({ type: "message", message: { role: "user", content: "unrelated" } }) + "\n";
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = (body: string | Buffer = other + receipt("first")) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "session-receipts-")); roots.push(root);
  const file = path.join(root, "session.jsonl"); fs.writeFileSync(file, body);
  const manager: SessionReceiptManager = { getEntries: () => [], getSessionFile: () => file, isPersisted: () => true };
  return { root, file, manager };
};
const full = (file: string): unknown[] => {
  const lines = fs.readFileSync(file, "utf8").split("\n"); lines.pop();
  const entries: unknown[] = [];
  for (const [index, line] of lines.entries()) if (matches(line)) { try { entries[index] = JSON.parse(line); } catch { /* no receipt */ } }
  entries.length = lines.length; return entries;
};
const bytesRead = (spy: ReturnType<typeof vi.spyOn>) => spy.mock.results.reduce((sum: number, result: { type: string; value?: unknown }) => sum + (result.type === "return" && typeof result.value === "number" ? result.value : 0), 0);

describe("incremental confirmed session receipts", () => {
  it("retains receipt identity, not a second copy of delivered text or metadata (#2177)", () => {
    const body = "large delivered body ".repeat(100_000);
    const entry = { type: "custom_message", id: "native", timestamp: "2026-01-01T00:00:00.000Z",
      customType: "pi-fabric-agent-message", content: body, display: true,
      details: { text: body, id: "delivery", chain: "chain", from: { id: "sender", name: body },
        data: { key: " work ", ref: "ref", deliveryId: "data-delivery", messageId: "message", body },
        items: [{ id: "item", chain: "item-chain", deliveryId: "item-delivery", text: body,
          from: { id: "other", name: body }, data: { ref: " item-ref ", body } }] } };
    const { manager, file } = fixture(JSON.stringify({ type: "session", id: "session" }) + "\n" + JSON.stringify(entry) + "\n");
    const project = projectRootInboxReceipt;
    const snapshot = confirmedSessionReceiptSnapshot(manager, matches, project);
    const encoded = JSON.stringify([...snapshot.entries.values()]);
    expect(encoded.length).toBeLessThan(1_000);
    expect(encoded).not.toContain("large delivered body");
    const expected = rootInboxSession([entry]);
    const actual = confirmedRootInboxSession(manager);
    expect([...actual.delivered!]).toEqual([...expected.delivered!]);
    expect([...actual.deliveredAt!]).toEqual([...expected.deliveredAt!]);
    expect(actual.holdsSteer("other", "item-ref")).toBe(false);
    expect(confirmedMainInboxIds(manager, "session")).toEqual(new Set(["delivery", "chain", "item", "item-chain"]));
    expect(() => confirmedMainInboxIds(manager, "wrong")).toThrow("identity mismatch");
    // Changing the projection cannot inherit a compact cache as canonical data.
    expect(confirmedSessionReceiptSnapshot(manager, matches).entries.get(1)).toEqual(entry);
    expect(fs.readFileSync(file, "utf8")).toContain("large delivered body");
  });

  it("isolates projected caches, keeps original indices, and reprojects only appended lines", () => {
    const { manager, file } = fixture(other + receipt("first"));
    const project = vi.fn((value: unknown) => ({ identity: (value as { details: unknown }).details }));
    const before = confirmedSessionReceiptSnapshot(manager, matches, project);
    expect(project).toHaveBeenCalledTimes(1);
    expect(confirmedSessionReceiptSnapshot(manager, matches, project).entries).toBe(before.entries);
    expect(project).toHaveBeenCalledTimes(1);
    fs.appendFileSync(file, "{pi-fabric-inbox malformed}\n" + other + receipt("second"));
    const after = confirmedSessionReceiptSnapshot(manager, matches, project);
    expect([...after.entries.keys()]).toEqual([1, 4]);
    expect(after.count).toBe(5);
    expect(project).toHaveBeenCalledTimes(2);
    const otherProject = (entry: unknown) => entry;
    expect(confirmedSessionReceiptSnapshot(manager, matches, otherProject).entries.get(1)).toEqual(JSON.parse(receipt("first")));
  });

  it("fails closed when projection throws and does not poison later receipts", () => {
    const { manager } = fixture();
    expect(() => confirmedSessionReceiptSnapshot(manager, matches, () => { throw new Error("projection failed"); })).toThrow("projection failed");
    expect(confirmedSessionReceiptSnapshot(manager, matches, projectRootInboxReceipt).entries.size).toBe(1);
    const memory = { getEntries: () => [JSON.parse(receipt("memory"))], isPersisted: () => false };
    expect(rootInboxSession([...confirmedSessionReceiptSnapshot(memory, matches, projectRootInboxReceipt).entries.values()]).holdsBatch(["memory"])).toBe(true);
  });

  it("reads zero bytes on an unchanged 150 MiB file, retaining only matching entries", () => {
    const { file, manager } = fixture();
    const line = JSON.stringify({ type: "custom", data: "x".repeat(1024 * 1024 - 28) }) + "\n";
    const fd = fs.openSync(file, "w");
    try { for (let index = 0; index < 150; index++) fs.writeSync(fd, line); fs.writeSync(fd, receipt("large")); } finally { fs.closeSync(fd); }
    expect(fs.statSync(file).size).toBeGreaterThanOrEqual(150 * 1024 * 1024);
    const first = confirmedSessionEntries(manager, matches);
    expect(first.length).toBe(151); expect(Object.keys(first)).toEqual(["150"]);
    const read = vi.spyOn(fs, "readSync"); const sync = vi.spyOn(fs, "fsyncSync");
    expect(confirmedSessionEntries(manager, matches)).toEqual(first);
    expect(read).not.toHaveBeenCalled(); expect(sync).toHaveBeenCalled();
    const snapshot = confirmedSessionReceiptSnapshot(manager, matches);
    expect(snapshot.count).toBe(151); expect(snapshot.entries.size).toBe(1);
  }, 30_000);

  it("reads only an append plus the 64-byte rewrite guard and preserves full-scan indices", () => {
    const { file, manager } = fixture(other + receipt("first") + other.repeat(600));
    const before = confirmedSessionEntries(manager, matches);
    const append = other + "{pi-fabric-inbox malformed}\n\n" + receipt("second");
    fs.appendFileSync(file, append);
    const read = vi.spyOn(fs, "readSync"); const next = confirmedSessionEntries(manager, matches);
    expect(bytesRead(read)).toBe(Buffer.byteLength(append) + 64);
    expect(next).toEqual(full(file)); expect(Object.keys(next)).toEqual(["1", "605"]);
    expect(before.length).toBe(602); expect(before[605]).toBeUndefined();
    const inbox = confirmedRootInboxSession(manager);
    expect(inbox.holdsBatch(["first"])).toBe(false); expect(inbox.holdsBatch(["second"])).toBe(true);
    expect([...inbox.delivered!]).toEqual([...rootInboxSession(full(file)).delivered!]);
  });

  it("carries a partial final UTF-8 line until completion, counting it only once", () => {
    const complete = Buffer.from(receipt("completed-😀")); const cut = complete.indexOf(Buffer.from("😀")) + 2;
    const { file, manager } = fixture(Buffer.concat([Buffer.from(other), complete.subarray(0, cut)]));
    const first = confirmedSessionEntries(manager, matches); expect(first.length).toBe(1); expect(Object.keys(first)).toEqual([]);
    const unchanged = vi.spyOn(fs, "readSync"); confirmedSessionEntries(manager, matches); expect(unchanged).not.toHaveBeenCalled();
    fs.appendFileSync(file, complete.subarray(cut)); const next = confirmedSessionEntries(manager, matches);
    expect(next).toEqual(full(file)); expect(next.length).toBe(2); expect(Object.keys(next)).toEqual(["1"]);
    expect(first.length).toBe(1);
  });

  it("handles lines spanning multiple read buffers and appends after the carry", () => {
    const long = receipt("x".repeat((1 << 20) * 2 + 7));
    const { file, manager } = fixture(long.slice(0, -2));
    expect(confirmedSessionEntries(manager, matches).length).toBe(0);
    fs.appendFileSync(file, long.slice(-2) + other + receipt("next"));
    expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
  });

  it("rescans inode replacement", () => {
    const { file, manager } = fixture(); confirmedSessionEntries(manager, matches);
    const replacement = file + ".next"; fs.writeFileSync(replacement, receipt("replace") + other.repeat(5)); fs.renameSync(replacement, file);
    const read = vi.spyOn(fs, "readSync"); expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
    expect(bytesRead(read)).toBe(fs.statSync(file).size);
  });

  it("rescans truncation, even if the shrunken file still reaches the complete-line offset", () => {
    const { file, manager } = fixture(receipt("first") + "x".repeat(200)); confirmedSessionEntries(manager, matches);
    fs.truncateSync(file, Buffer.byteLength(receipt("first")) + 100);
    const read = vi.spyOn(fs, "readSync"); expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
    expect(bytesRead(read)).toBe(fs.statSync(file).size);
  });

  it("detects a same-size rewrite by its trailing window, even with restored mtime", () => {
    const { file, manager } = fixture(other + receipt("first")); confirmedSessionEntries(manager, matches);
    const before = fs.statSync(file); fs.writeFileSync(file, other + receipt("other")); fs.utimesSync(file, before.atime, before.mtime);
    const read = vi.spyOn(fs, "readSync"); expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
    expect(bytesRead(read)).toBe(64 + before.size);
  });

  it("detects a rewritten prefix followed by an append", () => {
    const { file, manager } = fixture(); confirmedSessionEntries(manager, matches);
    fs.writeFileSync(file, other + receipt("other") + other + receipt("added"));
    expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
  });

  it("detects a rewritten partial line before its completion", () => {
    const partial = receipt("first").trimEnd(); const { file, manager } = fixture(other + partial);
    confirmedSessionEntries(manager, matches); fs.writeFileSync(file, other + receipt("other"));
    expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
  });

  it("invalidates missing files rather than returning stale receipts", () => {
    const { file, manager } = fixture(); confirmedSessionEntries(manager, matches); fs.renameSync(file, file + ".gone");
    expect(confirmedSessionEntries(manager, matches)).toEqual([]);
    fs.writeFileSync(file, receipt("new")); expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
  });

  it("requires confirmation even for a cache hit and recovers after a failed barrier", () => {
    const { file, manager } = fixture(); confirmedSessionEntries(manager, matches);
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("barrier failed"); });
    expect(() => confirmedSessionEntries(manager, matches)).toThrow("barrier failed");
    expect(confirmedRootInboxSession(manager).delivered!.size).toBe(0); sync.mockRestore();
    const read = vi.spyOn(fs, "readSync"); expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
    expect(bytesRead(read)).toBe(fs.statSync(file).size);
  });

  it("does not publish a partly read append after a read failure", () => {
    const { file, manager } = fixture(); confirmedSessionEntries(manager, matches); fs.appendFileSync(file, receipt("second"));
    const original = fs.readSync.bind(fs); let calls = 0;
    const read = vi.spyOn(fs, "readSync").mockImplementation((...args: Parameters<typeof fs.readSync>) => {
      if (++calls === 2) return 0; return original(...args);
    });
    expect(() => confirmedSessionEntries(manager, matches)).toThrow("changed while reading"); read.mockRestore();
    expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
  });

  it("does not share receipts across filters or session paths", () => {
    const { file, manager } = fixture(); confirmedSessionEntries(manager, matches);
    expect(Object.keys(confirmedSessionEntries(manager, () => false))).toEqual([]);
    expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
    const second = file + ".second"; fs.writeFileSync(second, receipt("second"));
    manager.getSessionFile = () => second; expect(confirmedSessionEntries(manager, matches)).toEqual(full(second));
  });

  it("preserves in-memory and deferred-first-write semantics", () => {
    const entries = [{ type: "custom_message" }];
    expect(confirmedSessionEntries({ getEntries: () => entries, isPersisted: () => false }, matches)).toBe(entries);
    expect(confirmedSessionEntries({ getEntries: () => entries }, matches)).toBe(entries);
    expect(confirmedSessionEntries({ getEntries: () => entries, isPersisted: () => true }, matches)).toEqual([]);
    const { file, manager } = fixture(); fs.renameSync(file, file + ".gone"); manager.getEntries = () => entries;
    expect(confirmedSessionEntries(manager, matches)).toEqual([]);
  });
});
