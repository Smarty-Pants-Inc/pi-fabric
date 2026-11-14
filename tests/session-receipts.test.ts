import { Hash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { confirmedSessionEntries, confirmedSessionReceiptSnapshot, type SessionReceiptManager } from "../src/core/session-receipts.js";
import * as atomicWrite from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { confirmedMainInboxIds, projectRootInboxReceipt, RootInbox } from "../src/topology/root-inbox.js";
import { confirmedRootInboxSession, registerIssuedInbox, rootInboxSession } from "./root-inbox-issued-fixture.js";

const matches = (line: string) => line.includes("pi-fabric-inbox") || line.includes("pi-fabric-agent-message");
// Cache the test-owned issuance so byte-rewrite and partial-line probes keep deterministic bytes.
const issuedReceipts = new Map<string, string>();
const receipt = (id: string): string => {
  const cached = issuedReceipts.get(id);
  if (cached) return cached;
  const entry = { type: "custom_message", customType: "pi-fabric-inbox", timestamp: "2026-01-01T00:00:00.000Z",
    content: "test-owned issued receipt", details: { ids: [id] } };
  registerIssuedInbox(entry.details, entry.content);
  const line = JSON.stringify(entry) + "\n";
  issuedReceipts.set(id, line);
  return line;
};
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
// Scoped descriptor replacement follows host-lease-archive-windows.test.ts.
// Change only the cache-policy platform AFTER real native open/fsync/namespace
// confirmation: fake Linux directory fsync is not supported on a Windows host.
// No barriers are omitted and no public product-only test API is needed.
const onPlatform = <T>(platform: NodeJS.Platform, confirm: () => T): T => {
  const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
  const namespace = atomicWrite.syncPathNamespace;
  const sync = vi.spyOn(atomicWrite, "syncPathNamespace").mockImplementation((...args) => {
    namespace(...args);
    Object.defineProperty(process, "platform", { ...descriptor, value: platform });
  });
  try { return confirm(); } finally {
    Object.defineProperty(process, "platform", descriptor);
    sync.mockRestore();
  }
};

describe("incremental confirmed session receipts", () => {
  it("retains receipt identity, not a second copy of delivered text or metadata (#2177)", () => {
    const body = "large delivered body ".repeat(100_000);
    const entry = { type: "custom_message", id: "native", timestamp: "2026-01-01T00:00:00.000Z",
      customType: "pi-fabric-agent-message", content: body, display: true,
      details: { text: body, id: "delivery", chain: "chain", from: { id: "sender", name: body },
        data: { key: " work ", ref: "ref", topic: "fleet.work.receipts", deliveryId: "data-delivery", messageId: "message", body },
        items: [{ id: "item", chain: "item-chain", deliveryId: "item-delivery", text: body,
          from: { id: "other", name: body }, data: { ref: " item-ref ", key: "item-key", topic: "fleet.work.receipts", body } }] } };
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
    expect(actual.holdsSteer("other", "item-ref", "fleet.work.receipts")).toBe(false);
    // The projection keeps a carrier's topic: its topic-scoped key claim survives the compact cache.
    expect(actual.holdsSteer("other", "item-key", "fleet.work.receipts")).toBe(true);
    expect(actual.holdsSteer("other", "item-key", "fleet.work.other")).toBe(false);
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

  it("reads zero bytes on an unchanged 150 MiB file on Linux, retaining only matching entries", () => {
    const { file, manager } = fixture();
    const line = JSON.stringify({ type: "custom", data: "x".repeat(1024 * 1024 - 28) }) + "\n";
    const fd = fs.openSync(file, "w");
    try { for (let index = 0; index < 150; index++) fs.writeSync(fd, line); fs.writeSync(fd, receipt("large")); } finally { fs.closeSync(fd); }
    expect(fs.statSync(file).size).toBeGreaterThanOrEqual(150 * 1024 * 1024);
    const first = confirmedSessionEntries(manager, matches);
    expect(first.length).toBe(151); expect(Object.keys(first)).toEqual(["150"]);
    const read = vi.spyOn(fs, "readSync"); const sync = vi.spyOn(fs, "fsyncSync");
    expect(onPlatform("linux", () => confirmedSessionEntries(manager, matches))).toEqual(first);
    expect(read).not.toHaveBeenCalled(); expect(sync).toHaveBeenCalled();
    const snapshot = confirmedSessionReceiptSnapshot(manager, matches);
    expect(snapshot.count).toBe(151); expect(snapshot.entries.size).toBe(1);
  }, 30_000);

  it("verifies the full old prefix, reads the append once, and preserves full-scan indices", () => {
    const { file, manager } = fixture(other + receipt("first") + other.repeat(600));
    const before = confirmedSessionEntries(manager, matches);
    const prefixSize = fs.statSync(file).size;
    const append = other + "{pi-fabric-inbox malformed}\n\n" + receipt("second");
    fs.appendFileSync(file, append);
    const read = vi.spyOn(fs, "readSync"); const next = confirmedSessionEntries(manager, matches);
    expect(bytesRead(read)).toBe(prefixSize + Buffer.byteLength(append));
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
    const unchanged = vi.spyOn(fs, "readSync");
    onPlatform("linux", () => confirmedSessionEntries(manager, matches)); expect(unchanged).not.toHaveBeenCalled();
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

  it("detects a same-size prefix rewrite, even with restored mtime", () => {
    const { file, manager } = fixture(other + receipt("first")); confirmedSessionEntries(manager, matches);
    const before = fs.statSync(file); fs.writeFileSync(file, other + receipt("other")); fs.utimesSync(file, before.atime, before.mtime);
    const read = vi.spyOn(fs, "readSync"); expect(confirmedSessionEntries(manager, matches)).toEqual(full(file));
    expect(bytesRead(read)).toBe(before.size * 2);
  });

  it.each(["mtimeNs", "ctimeNs"] as const)("invalidates canonical and derived receipts when only %s escapes Number precision (20527)", async (field) => {
    // Data safety: model NTFS's 100ns storage unit at a present-day epoch. These
    // distinct raw timestamps map to the SAME Number milliseconds, without a timer.
    const base = 1_794_000_000_000_000_000n;
    const millis = (ns: bigint): number => Number(ns / 1_000_000_000n) * 1_000 + Number(ns % 1_000_000_000n) / 1_000_000;
    expect(millis(base + 100n)).toBe(millis(base));
    const native = JSON.stringify({ type: "custom_message", customType: "pi-fabric-agent-message", timestamp: Date.now(),
      details: { id: "old", from: { id: "session:peer" }, data: { deliveryId: "old", topic: "fleet.work.receipts" } } });
    const body = native + "\n" + other.repeat(600);
    const { root, file, manager } = fixture(body);
    const derivedManager = { ...manager };
    const identity = fs.statSync(file, { bigint: true });
    const fstat = fs.fstatSync.bind(fs);
    let revision = 0n;
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) => {
      const raw = fstat(fd, { bigint: true });
      const actual = options?.bigint ? raw : fstat(fd);
      if (raw.dev !== identity.dev || raw.ino !== identity.ino) return actual;
      const mtimeNs = base + (field === "mtimeNs" ? revision : 0n);
      const ctimeNs = base + (field === "ctimeNs" ? revision : 0n);
      return Object.assign(actual, options?.bigint
        ? { mtimeNs, ctimeNs, mtimeMs: mtimeNs / 1_000_000n, ctimeMs: ctimeNs / 1_000_000n }
        : { mtimeMs: millis(mtimeNs), ctimeMs: millis(ctimeNs) });
    }) as typeof fs.fstatSync);
    // Isolate nanosecond invalidation from the Windows equal-stat hash fallback.
    const confirm = () => onPlatform("linux", () => confirmedSessionReceiptSnapshot(manager, matches));
    const derived = () => onPlatform("linux", () => confirmedRootInboxSession(derivedManager));
    const before = confirm();
    const derivedBefore = derived();
    expect(before.entries.size).toBe(1);
    expect(derivedBefore.delivered!.size).toBeGreaterThan(0);
    const replacement = JSON.stringify({ type: "custom", data: "x".repeat(native.length - 27) }).padEnd(native.length, " ");
    expect(Buffer.byteLength(replacement)).toBe(Buffer.byteLength(native));
    fs.writeFileSync(file, replacement + "\n" + other.repeat(600));
    revision = 100n;
    const read = vi.spyOn(fs, "readSync");
    const after = confirm();
    expect([...after.entries]).toEqual([]);
    expect(after.count).toBe(before.count);
    expect(after.update!.generation).not.toBe(before.update!.generation);
    expect(after.update!.from).toBe(0);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) * 2);
    read.mockClear();
    const derivedAfter = derived();
    expect(derivedAfter.delivered!.size).toBe(0);
    expect(derivedAfter).not.toBe(derivedBefore);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) * 2);
    // Exercise the consumer that can otherwise permanently suppress unseen work.
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
    const me = { id: "session:judge", name: "judge", kind: "main" as const };
    const inbox = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
    try {
      inbox.start();
      const event = await mesh.publish({ topic: "fleet.work.receipts", kind: "handoff", to: me.id,
        from: { id: "session:peer", name: "peer", kind: "main" }, text: "unseen old work", data: { deliveryId: "old" } });
      expect((await inbox.next(derivedAfter)).events.map(item => item.id)).toEqual([event.id]);
    } finally { await inbox.close(); await mesh.closeState(); }

    // A genuine append may keep the same timestamp too: size still drives prefix
    // verification, and matching old bytes preserve the generation and indices.
    const append = native.replaceAll('"old"', '"new"') + "\n";
    fs.appendFileSync(file, append);
    read.mockClear();
    const appended = confirm();
    expect(appended.update!.generation).toBe(after.update!.generation);
    expect(appended.update!.from).toBe(after.count);
    expect([...appended.entries.keys()]).toEqual([after.count]);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) + Buffer.byteLength(append));
    const derivedAppended = derived();
    expect(derivedAppended.delivered!.size).toBeGreaterThan(0);
    read.mockClear();
    const update = vi.spyOn(Hash.prototype, "update");
    const copy = vi.spyOn(Hash.prototype, "copy");
    const digest = vi.spyOn(Hash.prototype, "digest");
    const sync = vi.spyOn(fs, "fsyncSync");
    expect(onPlatform("linux", () => confirmedSessionReceiptSnapshot(manager, matches))).toBe(appended);
    expect(onPlatform("linux", () => confirmedRootInboxSession(derivedManager))).toBe(derivedAppended);
    expect(read).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(copy).not.toHaveBeenCalled();
    expect(digest).not.toHaveBeenCalled();
    expect(sync).toHaveBeenCalled();
  });

  it("hashes the entire unchanged Windows prefix including carry, reusing the exact snapshot without parsing or projection (20527)", () => {
    const body = receipt("windows-idle") + other.repeat(30_000) + "unfinished carry";
    const { file, manager } = fixture(body);
    const filter = vi.fn(matches);
    const project = vi.fn((entry: unknown) => entry);
    const confirm = () => onPlatform("win32", () => confirmedSessionReceiptSnapshot(manager, filter, project));
    const before = confirm();
    filter.mockClear(); project.mockClear();
    const read = vi.spyOn(fs, "readSync");
    const update = vi.spyOn(Hash.prototype, "update");
    const parse = vi.spyOn(JSON, "parse");
    const sync = vi.spyOn(fs, "fsyncSync");
    const after = confirm();
    expect(after).toBe(before);
    expect(after.update).toBe(before.update);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body));
    expect(update.mock.calls.reduce((sum, [bytes]) => sum + (bytes as Buffer).byteLength, 0)).toBe(Buffer.byteLength(body));
    expect(parse).not.toHaveBeenCalled();
    expect(filter).not.toHaveBeenCalled();
    expect(project).not.toHaveBeenCalled();
    expect(sync).toHaveBeenCalled();
    expect(fs.statSync(file).size).toBe(Buffer.byteLength(body));
  });

  it.each(["removed", "added"] as const)("invalidates a Windows receipt %s with an identical full raw stat tuple and unchanged tail (20527)", async (mode) => {
    const native = JSON.stringify({ type: "custom_message", customType: "pi-fabric-agent-message", timestamp: Date.now(),
      details: { id: "old", from: { id: "session:peer" }, data: { deliveryId: "old", topic: "fleet.work.receipts" } } });
    const unrelated = JSON.stringify({ type: "custom", data: "x".repeat(native.length - 27) }).padEnd(native.length, " ");
    expect(Buffer.byteLength(unrelated)).toBe(Buffer.byteLength(native));
    const tail = "\n" + other.repeat(600);
    const body = (mode === "removed" ? native : unrelated) + tail;
    const { root, file, manager } = fixture(body);
    const derivedManager = { ...manager };
    const identity = fs.statSync(file, { bigint: true });
    const numeric = fs.statSync(file);
    const fstat = fs.fstatSync.bind(fs);
    // Deterministically model an actual filesystem collision, not just Number
    // rounding. Identity, size and BOTH raw ns timestamps are equal on rewrite.
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: { bigint?: boolean }) => {
      const raw = fstat(fd, { bigint: true });
      const actual = options?.bigint ? raw : fstat(fd);
      if (raw.dev !== identity.dev || raw.ino !== identity.ino) return actual;
      return Object.assign(actual, options?.bigint
        ? { mtimeNs: identity.mtimeNs, ctimeNs: identity.ctimeNs, mtimeMs: identity.mtimeMs, ctimeMs: identity.ctimeMs }
        : { mtimeMs: numeric.mtimeMs, ctimeMs: numeric.ctimeMs });
    }) as typeof fs.fstatSync);
    const project = vi.fn((entry: unknown) => entry);
    const confirm = () => onPlatform("win32", () => confirmedSessionReceiptSnapshot(manager, matches, project));
    const derived = () => onPlatform("win32", () => confirmedRootInboxSession(derivedManager));
    const before = confirm();
    const derivedBefore = derived();
    expect(before.entries.size).toBe(mode === "removed" ? 1 : 0);
    expect(derivedBefore.delivered!.size > 0).toBe(mode === "removed");
    fs.writeFileSync(file, (mode === "removed" ? unrelated : native) + tail);
    expect(identity.size).toBe(BigInt(Buffer.byteLength(body)));
    expect(fs.statSync(file).size).toBe(Buffer.byteLength(body));
    const read = vi.spyOn(fs, "readSync");
    const after = confirm();
    expect(after.entries.size).toBe(mode === "removed" ? 0 : 1);
    expect(after.count).toBe(before.count);
    expect(after.update!.generation).not.toBe(before.update!.generation);
    expect(after.update!.from).toBe(0);
    expect([...after.update!.entries]).toEqual([...after.entries]);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) * 2);
    read.mockClear();
    const derivedAfter = derived();
    expect(derivedAfter).not.toBe(derivedBefore);
    expect(derivedAfter.delivered!.size > 0).toBe(mode === "added");
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) * 2);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
    const me = { id: "session:judge", name: "judge", kind: "main" as const };
    const inbox = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
    try {
      inbox.start();
      const event = await mesh.publish({ topic: "fleet.work.receipts", kind: "handoff", to: me.id,
        from: { id: "session:peer", name: "peer", kind: "main" }, text: "old work", data: { deliveryId: "old" } });
      expect((await inbox.next(derivedAfter)).events.map(item => item.id)).toEqual(mode === "removed" ? [event.id] : []);
    } finally { await inbox.close(); await mesh.closeState(); }

    // A true append with the same timestamps MUST preserve generation, old
    // projections and delta indices, and read exactly P+A rather than 2P+A.
    const append = native.replaceAll('"old"', '"new"') + "\n";
    fs.appendFileSync(file, append);
    read.mockClear(); project.mockClear();
    const appended = confirm();
    expect(appended.update!.generation).toBe(after.update!.generation);
    expect(appended.update!.from).toBe(after.count);
    expect([...appended.update!.entries.keys()]).toEqual([after.count]);
    expect(appended.entries.get(after.count)).toEqual(JSON.parse(append));
    expect(project).toHaveBeenCalledTimes(1);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) + Buffer.byteLength(append));
    const derivedAppended = derived();
    expect(derivedAppended.delivered!.size).toBeGreaterThan(derivedAfter.delivered!.size);
    read.mockClear(); project.mockClear();
    expect(confirm()).toBe(appended);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) + Buffer.byteLength(append));
    expect(project).not.toHaveBeenCalled();
    read.mockClear();
    expect(derived()).toBe(derivedAppended);
    expect(bytesRead(read)).toBe(Buffer.byteLength(body) + Buffer.byteLength(append));
  });

  it.each(["complete prefix", "partial carry"])("detects an early mutation in a >1 MiB %s with an unchanged tail", (mode) => {
    // Data-safety regression #20527: the changed receipt id is over two read buffers
    // away from both the old complete-line tail and the end of any partial carry.
    const old = JSON.stringify({ type: "custom_message", customType: "pi-fabric-inbox",
      details: { ids: ["old"] }, content: "x".repeat((1 << 20) * 2 + 7) });
    const body = mode === "complete prefix" ? old + "\n" + other.repeat(8) : other + old;
    const { file, manager } = fixture(body);
    const project = vi.fn((entry: unknown) => (entry as { details: unknown }).details);
    const before = confirmedSessionReceiptSnapshot(manager, matches, project);
    const prefixSize = fs.statSync(file).size;
    const changed = body.replace('"old"', '"new"') + (mode === "partial carry" ? "\n" : "");
    fs.writeFileSync(file, changed);
    const read = vi.spyOn(fs, "readSync");
    const after = confirmedSessionReceiptSnapshot(manager, matches, project);
    expect(bytesRead(read)).toBe(prefixSize + Buffer.byteLength(changed));
    expect([...after.entries.values()]).toEqual([{ ids: ["new"] }]);
    expect(after.count).toBe(full(file).length);
    expect(after.update!.generation).not.toBe(before.update!.generation);
    expect(after.update!.from).toBe(0);
    expect([...after.update!.entries]).toEqual([...after.entries]);
    expect([...before.entries.values()]).toEqual(mode === "complete prefix" ? [{ ids: ["old"] }] : []);
    read.mockClear();
    const update = vi.spyOn(Hash.prototype, "update");
    const copy = vi.spyOn(Hash.prototype, "copy");
    const digest = vi.spyOn(Hash.prototype, "digest");
    expect(onPlatform("linux", () => confirmedSessionReceiptSnapshot(manager, matches, project))).toBe(after);
    expect(read).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(copy).not.toHaveBeenCalled();
    expect(digest).not.toHaveBeenCalled();
  });

  it("retains generation and projections after metadata-only revalidation, then does no Linux idle reads or hashes", () => {
    const { file, manager } = fixture();
    const project = vi.fn((entry: unknown) => (entry as { details: unknown }).details);
    const before = confirmedSessionReceiptSnapshot(manager, matches, project);
    const stat = fs.statSync(file);
    fs.utimesSync(file, stat.atime, new Date(stat.mtimeMs + 10_000));
    const read = vi.spyOn(fs, "readSync");
    const after = confirmedSessionReceiptSnapshot(manager, matches, project);
    expect(bytesRead(read)).toBe(stat.size);
    expect(after.update!.generation).toBe(before.update!.generation);
    expect(after.update!.from).toBe(before.count);
    expect(after.update!.entries.size).toBe(0);
    expect([...after.entries]).toEqual([...before.entries]);
    expect(after.entries.get(1)).toBe(before.entries.get(1));
    expect(project).toHaveBeenCalledTimes(1);
    read.mockClear();
    const update = vi.spyOn(Hash.prototype, "update");
    const copy = vi.spyOn(Hash.prototype, "copy");
    const digest = vi.spyOn(Hash.prototype, "digest");
    const sync = vi.spyOn(fs, "fsyncSync");
    expect(onPlatform("linux", () => confirmedSessionReceiptSnapshot(manager, matches, project))).toBe(after);
    expect(read).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(copy).not.toHaveBeenCalled();
    expect(digest).not.toHaveBeenCalled();
    expect(sync).toHaveBeenCalled();
    expect(project).toHaveBeenCalledTimes(1);
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
