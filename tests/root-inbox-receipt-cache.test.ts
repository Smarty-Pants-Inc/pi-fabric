import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { SessionReceiptManager } from "../src/core/session-receipts.js";
import { MeshStore } from "../src/mesh/store.js";
import { RootInbox } from "../src/topology/root-inbox.js";
import * as shared from "../src/topology/root-inbox-shared.js";

vi.mock("node:crypto", async importOriginal => {
  const actual = await importOriginal<typeof import("node:crypto")>();
  return { ...actual, createHash: vi.fn(actual.createHash) };
});

// Keep reasons: the real idle receipt-rebuild incident and the private issuer/data-safety
// boundary. Compare with the uncached full-history reader, including iteration order.
const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
});
const TOPIC = "fleet.work.receipt-cache";
const native = (id: string, at: unknown = Date.now(), data: object = { key: id }) => ({
  type: "custom_message", customType: "pi-fabric-agent-message", id, timestamp: at,
  details: { id, from: { id: "peer" }, data: { topic: TOPIC, ...data }, receipts: [`native:${id}`] },
});
const unrelated = () => ({ type: "message", message: { role: "user", content: "not a receipt" } });
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "root-inbox-receipt-cache-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const file = path.join(root, "session.jsonl");
  let currentFile = file;
  let sessionId = "first-session";
  let issuer = shared.rootInboxReceiptIssuer();
  const verifier = shared.rootInboxReceiptVerifier(() => issuer);
  const manager: SessionReceiptManager = { getEntries: () => [], getSessionFile: () => currentFile,
    getSessionId: () => sessionId, isPersisted: () => true };
  const encode = (entries: unknown[]) => entries.map(entry => JSON.stringify(entry) + "\n").join("");
  const write = (entries: unknown[]) => fs.writeFileSync(currentFile, encode(entries));
  const append = (...entries: unknown[]) => fs.appendFileSync(currentFile, encode(entries));
  const issue = (id: string, at: unknown = Date.now()) => {
    const carrier = { type: "custom_message", customType: shared.ROOT_INBOX_CUSTOM_TYPE, timestamp: at,
      content: `shown ${id}`, details: { ids: [id], receipts: [`issued:${id}`], nonce: "" } };
    shared.registerRootInboxReceipt(issuer, carrier.details, carrier.content);
    return carrier;
  };
  const read = () => shared.confirmedRootInboxSession(manager, verifier);
  const full = () => shared.rootInboxSession(fs.readFileSync(currentFile, "utf8").split("\n").slice(0, -1).map(line => {
    try { return JSON.parse(line); } catch { return undefined; }
  }), verifier);
  const equal = () => {
    const actual = read(), expected = full();
    expect([...actual.delivered!]).toEqual([...expected.delivered!]);
    expect([...actual.deliveredAt!]).toEqual([...expected.deliveredAt!]);
    for (const ids of [[], ["shown"], ["second"], ["shown", "second"], ["missing"]])
      expect(actual.holdsBatch(ids), JSON.stringify(ids)).toBe(expected.holdsBatch(ids));
    for (const key of ["old", "new", "shown", "second", "missing"])
      expect(actual.holdsSteer("peer", key, TOPIC)).toBe(expected.holdsSteer("peer", key, TOPIC));
    return actual;
  };
  write([]);
  return { root, file, manager, verifier, write, append, issue, read, full, equal,
    issuer: () => issuer, replaceIssuer: () => { issuer = shared.rootInboxReceiptIssuer(); },
    generation: () => { sessionId = "next-session"; },
    switchFile: (next: string) => { currentFile = next; } };
};

describe("confirmed root inbox receipt-derived cache", () => {
  it("does no history derivation or canonical ledger remerge on N unchanged notifications", async () => {
    // Only existing public APIs here: this test must fail meaningfully on the base revision.
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "receipt-cache-idle-"));
    cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
    const file = path.join(root, "session.jsonl");
    fs.writeFileSync(file, Array.from({ length: 1_200 }, (_, n) => JSON.stringify(native(`old-${n}`)) + "\n").join(""));
    const manager = { getEntries: () => [], getSessionFile: () => file, isPersisted: () => true };
    const hash = vi.mocked(createHash);
    hash.mockClear();
    const first = shared.confirmedRootInboxSession(manager);
    expect(hash.mock.calls.length, "counter must observe actual receipt derivation").toBeGreaterThan(1_200);
    hash.mockClear();
    const barriers = vi.spyOn(fs, "fsyncSync");
    const N = 20;
    for (let n = 0; n < N; n++) {
      const view = shared.confirmedRootInboxSession(manager);
      expect(view.delivered!.size).toBe(first.delivered!.size);
    }
    expect(hash, "unchanged notifications must not hash historical native receipt identities").not.toHaveBeenCalled();
    expect(barriers.mock.calls.length, "cache hits still owe durability barriers").toBeGreaterThanOrEqual(N);
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
    cleanups.push(() => mesh.closeState());
    const me = { id: "session:cache-main", name: "cache-main", kind: "main" as const };
    const inbox = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
    cleanups.push(() => inbox.close());
    await inbox.next(first);
    const iterate = vi.spyOn(first.delivered!, Symbol.iterator);
    const times = vi.spyOn(first.deliveredAt!, "get");
    for (let n = 0; n < N; n++) {
      await mesh.publish({ topic: TOPIC, kind: "info", from: me, to: "session:unrelated", text: `notification ${n}` });
      expect(await inbox.wake(shared.confirmedRootInboxSession(manager), () => true)).toBeUndefined();
    }
    expect(iterate, "unchanged canonical indexes must not be re-merged").not.toHaveBeenCalled();
    expect(times).not.toHaveBeenCalled();
  });

  it("matches full rebuild across appends, compaction, rotation, generation change and restart", () => {
    const h = setup();
    const shown = h.issue("shown");
    h.write([native("old", 1_700_000_000_000), shown, native("new", 1_700_000_000_001)]);
    const before = h.equal();
    h.append(native("old", undefined), native("new", "invalid"), h.issue("second"));
    h.equal();
    expect(before.holdsBatch(["second"])).toBe(false);
    expect(before.delivered!.has(shared.eventReceipt("second"))).toBe(false);
    expect(before.holdsSteer("peer", "second", TOPIC)).toBe(false);
    h.append(...Array.from({ length: 501 }, unrelated), { type: "compaction", summary: "retained native history" });
    expect(h.equal().holdsBatch(["shown"])).toBe(false);
    const replacement = h.file + ".next";
    fs.writeFileSync(replacement, JSON.stringify(native("new")) + "\n" + JSON.stringify(shown) + "\n");
    fs.renameSync(replacement, h.file);
    h.equal();
    h.write([native("old")]); // truncation
    h.equal();
    const hash = vi.mocked(createHash); hash.mockClear();
    h.generation(); // same manager/file/stat, different native session identity
    h.read();
    expect(hash.mock.calls.length).toBeGreaterThan(0);
    h.equal();
    const restarted = { ...h.manager };
    const cold = shared.confirmedRootInboxSession(restarted, h.verifier);
    expect([...cold.delivered!]).toEqual([...h.full().delivered!]);
    h.replaceIssuer();
    h.write([shown, native("new")]);
    expect(h.equal().holdsBatch(["shown"])).toBe(false);
    h.switchFile(h.file + ".other");
    h.write([h.issue("second")]);
    h.equal();
  });

  it("does not traverse or re-verify unchanged authority-bearing inbox history", () => {
    const h = setup();
    h.write(Array.from({ length: 1_200 }, (_, n) => h.issue(`shown-${n}`)));
    const first = h.read();
    const gets = vi.spyOn(h.issuer().receipts, "get");
    const hash = vi.mocked(createHash); hash.mockClear();
    for (let n = 0; n < 20; n++) expect(h.read()).toBe(first);
    expect(gets).not.toHaveBeenCalled();
    expect(hash).not.toHaveBeenCalled();
  });

  it("derives only appended native receipt identities, never the unchanged prefix", () => {
    const h = setup();
    h.write(Array.from({ length: 1_000 }, (_, n) => native(`history-${n}`)));
    h.read();
    const hash = vi.mocked(createHash);
    hash.mockClear();
    h.append(native("new"));
    const next = h.read();
    expect(hash.mock.calls.length).toBe(2); // native-topic key + native carrier id
    expect(next.holdsSteer("peer", "new", TOPIC)).toBe(true);
    hash.mockClear();
    expect(h.read()).toBe(next);
    expect(hash).not.toHaveBeenCalled();
  });

  it("replays newest accepted nonces, preserves canonical timestamps and batch lookback", () => {
    const h = setup();
    const shown = h.issue("shown", 1_700_000_000_100);
    const tampered = { ...shown, content: "altered" };
    h.write([shown, ...Array.from({ length: 500 }, unrelated), tampered]);
    expect(h.equal().holdsBatch(["shown"])).toBe(false); // rejected newer copy cannot burn nonce
    h.append({ ...shown, timestamp: 1_700_000_000_200 });
    const fresh = h.equal();
    expect(fresh.holdsBatch(["shown"])).toBe(true);
    expect(fresh.deliveredAt!.get(shared.eventReceipt("shown"))).toBe(1_700_000_000_200);
    h.append({ ...shown, timestamp: "bad" });
    expect(h.equal().deliveredAt!.has(shared.eventReceipt("shown"))).toBe(false);
    // Eviction/revocation changes authority independently of file, stat and callback identity.
    const snapshot = h.issuer().receipts.get(shown.details.nonce)!;
    h.issuer().receipts.delete(shown.details.nonce);
    expect(h.equal().holdsBatch(["shown"])).toBe(false);
    h.issuer().receipts.set(shown.details.nonce, snapshot);
    expect(h.equal().holdsBatch(["shown"])).toBe(true);
    h.issuer().receipts.clear();
    expect(h.equal().delivered!.has(shared.eventReceipt("shown"))).toBe(false);
  });

  it("observes issuer eviction, same-closure replacement and unversioned verifier rejection/recovery", () => {
    const h = setup();
    const shown = h.issue("shown");
    h.write([shown]); h.equal();
    for (let n = 0; n < 2_048; n++) h.issue(`evict-${n}`);
    expect(h.equal().holdsBatch(["shown"])).toBe(false);
    h.replaceIssuer();
    expect(h.equal().holdsBatch(["shown"])).toBe(false);
    const second = h.issue("second"); h.append(second);
    expect(h.equal().holdsBatch(["second"])).toBe(true);
    let accept = true;
    const verifier: shared.RootInboxReceiptVerifier = (details, carrier, accepted) => accept ? h.verifier(details, carrier, accepted) : undefined;
    expect(shared.confirmedRootInboxSession(h.manager, verifier).holdsBatch(["second"])).toBe(true);
    accept = false;
    expect(shared.confirmedRootInboxSession(h.manager, verifier).holdsBatch(["second"])).toBe(false);
    accept = true;
    expect(shared.confirmedRootInboxSession(h.manager, verifier).holdsBatch(["second"])).toBe(true);
  });

  it("preserves timestamp authority when unchanged history crosses forward/backward clock boundaries", () => {
    const h = setup();
    let now = 1_700_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    h.write([native("old", now + 60_001), h.issue("shown", now + 60_002)]);
    expect(h.equal().deliveredAt!.size).toBe(0);
    now++;
    expect(h.equal().deliveredAt!.size).toBe(3);
    now++;
    expect(h.equal().deliveredAt!.has(shared.eventReceipt("shown"))).toBe(true);
    now -= 2;
    expect(h.equal().deliveredAt!.size).toBe(0);
    now += 2;
    h.equal();
  });

  it("still trims and persists expired ledger receipts when the canonical view is unchanged", async () => {
    const h = setup();
    let now = 1_700_000_000_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    h.write([native("old", now)]);
    const first = h.read();
    const mesh = new MeshStore(path.join(h.root, "mesh"), 64 * 1024, 500);
    cleanups.push(() => mesh.closeState());
    const me = { id: "session:expiry-main", name: "main", kind: "main" as const };
    const inbox = new RootInbox(mesh, me, () => [me.id], { horizonMs: 1_000 });
    cleanups.push(() => inbox.close());
    await inbox.next(first);
    const saved = () => mesh.get(inbox.key, { fresh: true })!.value as { delivered: string[]; deliveredAt: number[] };
    expect(saved().deliveredAt.every(at => at === now)).toBe(true);
    expect(saved().delivered.length).toBeGreaterThan(0);
    now += 1_001;
    expect(h.read()).toBe(first);
    await inbox.next(first);
    expect(saved().delivered).toEqual([]);
    expect(saved().deliveredAt).toEqual([]);
  });

  it("keeps the receipt view held by an awaited inbox read detached from later appends", async () => {
    const h = setup(); h.write([native("old")]);
    const before = h.read();
    const mesh = new MeshStore(path.join(h.root, "mesh"), 64 * 1024, 500);
    cleanups.push(() => mesh.closeState());
    const me = { id: "session:async-main", name: "main", kind: "main" as const };
    const inbox = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
    cleanups.push(() => inbox.close());
    inbox.start();
    const event = await mesh.publish({ topic: TOPIC, kind: "info", from: { ...me, id: "peer" }, to: me.id,
      text: "new", data: { deliveryId: "new" } });
    let release!: () => void, entered!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const started = new Promise<void>(resolve => { entered = resolve; });
    const put = mesh.put.bind(mesh);
    vi.spyOn(mesh, "put").mockImplementationOnce(async input => {
      const result = await put(input); entered(); await blocked; return result;
    });
    const reading = inbox.next(before);
    try {
      await started;
      h.append(native("new", Date.now(), { deliveryId: "new" }));
      expect(h.equal().holdsSteer("peer", "new", TOPIC)).toBe(false); // explicit carrier has no key
      expect([...before.delivered!]).not.toEqual([...h.read().delivered!]);
    } finally { release(); }
    expect((await reading).events.map(row => row.id)).toEqual([event.id]);
    expect((await inbox.next(h.read())).events).toEqual([]);
  });

  it("confirms barriers every call, rejects failures and recovers without stale authority", () => {
    const h = setup(); h.write([h.issue("shown"), native("old")]);
    h.equal();
    const sync = vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw new Error("failed barrier"); });
    expect(h.read().delivered!.size).toBe(0);
    sync.mockRestore();
    expect(h.equal().holdsBatch(["shown"])).toBe(true);
    fs.renameSync(h.file, h.file + ".gone");
    expect(h.read().delivered!.size).toBe(0);
    h.write([h.issue("second")]);
    h.equal();
    const throwing: shared.RootInboxReceiptVerifier = () => { throw new Error("verifier failed"); };
    expect(shared.confirmedRootInboxSession(h.manager, throwing).delivered!.size).toBe(0);
    h.equal();
  });

  it("keeps prior in-memory branch views detached across append, branch reset and session change", () => {
    const manager = SessionManager.inMemory();
    manager.appendCustomMessageEntry("pi-fabric-agent-message", "old", true, native("old").details);
    const before = shared.confirmedRootInboxSession(manager);
    const branch = manager.getLeafId()!;
    manager.appendCustomMessageEntry("pi-fabric-agent-message", "new", true, native("new").details);
    const appended = shared.confirmedRootInboxSession(manager);
    expect(appended.holdsSteer("peer", "new", TOPIC)).toBe(true);
    expect(before.holdsSteer("peer", "new", TOPIC)).toBe(false);
    expect([...before.delivered!]).not.toEqual([...appended.delivered!]);
    manager.branch(branch);
    const reset = shared.confirmedRootInboxSession(manager);
    expect(reset.holdsSteer("peer", "new", TOPIC)).toBe(false);
    expect(appended.holdsSteer("peer", "new", TOPIC)).toBe(true);
    manager.newSession();
    expect(shared.confirmedRootInboxSession(manager).delivered!.size).toBe(0);
  });
});
